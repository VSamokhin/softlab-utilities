/**
 * Copyright (C) 2025, Viktor Samokhin (wowyupiyo@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.softlab.datatransfer.adapters.postgres

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.chunked
import kotlinx.coroutines.launch
import org.softlab.datatransfer.core.BATCH_SIZE
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.REPORT_ON_INSERTS
import org.softlab.datatransfer.core.TransferDocument
import org.softlab.datatransfer.util.Postgres
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.plusAssign


class PostgresDestination(
    private val connection: Connection,
    private val dataTypeMappings: Map<String, String>,
    private val closeConnection: Boolean = true
) : DatabaseDestination {
    companion object {
        const val BACKEND = "postgres"
    }

    private val logger = KotlinLogging.logger {}

    constructor(
        jdbcUrl: String,
        username: String,
        password: String,
        dataTypeMappings: Map<String, String>
    ) : this(DriverManager.getConnection(jdbcUrl, username, password), dataTypeMappings, true)

    constructor(
        jdbcUrl: String,
        dataTypeMappings: Map<String, String>
    ): this(DriverManager.getConnection(jdbcUrl), dataTypeMappings, true)

    override fun getBackendName(): String = BACKEND

    override suspend fun createCollection(metadata: CollectionMetadata) {
        val schemaTable = Postgres.getSchemaTable(metadata.name)
        logger.debug { "Creating schema '${schemaTable.first}' if not exists and table '${schemaTable.second}'" }
        val columnsDef = metadata.fields.joinToString(", ") {
            "${it.name} ${mapType(it.type)}${if (!it.nullable) " NOT NULL" else ""}"
        }
        connection.createStatement().use {
            val createSchemaSql = "CREATE SCHEMA IF NOT EXISTS ${schemaTable.first};"
            logger.trace { "Executing SQL: $createSchemaSql" }
            it.execute(createSchemaSql)

            val createTableSql = "CREATE TABLE ${metadata.name} ($columnsDef);"
            logger.trace { "Executing SQL: $createTableSql" }
            it.execute(createTableSql)
        }
    }

    override suspend fun dropCollection(collectionName: String) {
        logger.debug { "Dropping table '$collectionName'" }
        val sql = "DROP TABLE IF EXISTS $collectionName;"
        connection.createStatement().use {
            logger.trace { "Executing SQL: $sql" }
            it.execute(sql)
        }
    }

    private fun mapType(type: String): String {
        return dataTypeMappings[type.lowercase()] ?: error("Unsupported type: $type")
    }

    @OptIn(ExperimentalCoroutinesApi::class, ExperimentalAtomicApi::class)
    override suspend fun insertDocuments(collectionName: String, documents: Flow<TransferDocument>) {
        logger.debug { "Inserting documents into '$collectionName'" }

        val schemaTable = Postgres.getSchemaTable(collectionName)
        val fields = Postgres.readColumns(schemaTable.first, schemaTable.second, connection)
        val fieldsLowerCase = fields.map { it.name.lowercase() }
        val columns = fields.map { it.name }.joinToString(", ") { it }
        val placeholders = fields.joinToString(", ") { "?" }

        val total = AtomicInt(0)
        val sql = "INSERT INTO $collectionName ($columns) VALUES ($placeholders);"
        logger.trace { "Insert SQL: $sql" }
        coroutineScope {
            documents.chunked(BATCH_SIZE).collect { docs ->
                launch {
                    val saved = connection.prepareStatement(sql).use { stmt ->
                        saveBatch(stmt, docs, collectionName, fieldsLowerCase)
                    }
                    total += saved
                    if (total.load() % REPORT_ON_INSERTS == 0) {
                        logger.info { "Inserted ${total.load()} rows into '$collectionName'" }
                    }
                }
            }
        }
        logger.info { "Total of ${total.load()} rows inserted into '$collectionName'" }
    }

    private fun saveBatch(
        stmt: PreparedStatement,
        docs: List<TransferDocument>,
        collectionName: String,
        fields: List<String>
    ): Int {
        for (doc in docs) {
            check(doc.keys.size <= fields.size) {
                "Table '${collectionName}' has less columns than row:\n" +
                    "table=${fields.joinToString(", ") { it }}\n" +
                    "row=${doc.keys.joinToString(", ") { it }}"
            }
            val values =
                doc.entries.associate { it.key.lowercase() to it.value } // I don't really like this
            fields.forEachIndexed { i, field ->
                stmt.setObject(i + 1, values[field])
            }
            stmt.addBatch()
        }
        return stmt.executeBatch().size
    }

    override fun close() {
        if (closeConnection) connection.close()
    }
}
