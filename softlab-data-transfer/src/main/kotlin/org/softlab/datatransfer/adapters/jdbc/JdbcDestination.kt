/**
 * Copyright (C) 2025-2026, Viktor Samokhin (wowyupiyo@gmail.com)
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

package org.softlab.datatransfer.adapters.jdbc

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.chunked
import kotlinx.coroutines.launch
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.jdbc.JdbcConnectionPool
import org.softlab.dataset.jdbc.withConnection
import org.softlab.dataset.jdbc.withStatement
import org.softlab.datatransfer.config.DataTypeMappings
import org.softlab.datatransfer.core.BATCH_SIZE
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.MAX_PENDING_ROWS
import org.softlab.datatransfer.core.REPORT_ON_INSERTS
import org.softlab.datatransfer.core.TransferDocument
import org.softlab.datatransfer.core.toSqlIdentifier
import org.softlab.datatransfer.util.reduce
import java.sql.PreparedStatement
import java.util.LinkedList
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.plusAssign


open class JdbcDestination(
    protected val pool: JdbcConnectionPool,
    protected val dialect: JdbcDialect,
    protected val dataTypeMappings: DataTypeMappings,
    private val closePool: Boolean
) : DatabaseDestination {
    private val logger = KotlinLogging.logger {}

    override fun getBackendName(): String = dialect.backendName

    override suspend fun createCollection(metadata: CollectionMetadata) {
        val tableName = dialect.parseTableName(metadata.name)
        logger.info { "Creating table '$tableName'" }
        val tableDefinitions = createTableDefinitions(metadata)
        pool.withConnection { connection ->
            dialect.beforeCreateTable(tableName, connection)
            connection.createStatement().use { stmt ->
                val createTableSql = "CREATE TABLE ${tableName.toSql()} ($tableDefinitions);"
                logger.trace { "Executing SQL: $createTableSql" }
                stmt.execute(createTableSql)
            }
        }
    }

    protected open fun createTableDefinitions(metadata: CollectionMetadata): String =
        createColumnDefinitions(metadata)

    protected open fun createColumnDefinitions(metadata: CollectionMetadata): String =
        metadata.fields.values.joinToString(", ") { createColumnDefinition(it, it.nullable) }

    protected fun createColumnDefinition(field: FieldDefinition, nullable: Boolean): String =
        "${field.name.toSqlIdentifier()} ${dataTypeMappings.map(field.type)}${if (!nullable) " NOT NULL" else ""}"

    override suspend fun dropCollection(collectionName: String) {
        val tableName = dialect.parseTableName(collectionName)
        logger.info { "Dropping table '$tableName'" }

        val sql = "DROP TABLE IF EXISTS ${tableName.toSql()};"
        pool.withStatement { stmt ->
            logger.trace { "Executing SQL: $sql" }
            stmt.execute(sql)
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class, ExperimentalAtomicApi::class)
    override suspend fun insertDocuments(collectionName: String, documents: Flow<TransferDocument>) {
        val tableName = dialect.parseTableName(collectionName)
        logger.info { "Inserting documents into '$tableName'" }

        val fields = pool.withConnection { dialect.listColumns(tableName, it) }
        val columns = fields.values.map { it.name.toSqlIdentifier() }.joinToString(separator = ", ")
        val placeholders = fields.values.joinToString(separator = ", ") { "?" }

        val total = AtomicInt(0)
        val jobs = LinkedList<Job>()
        val sql = "INSERT INTO ${tableName.toSql()} ($columns) VALUES ($placeholders);"
        logger.trace { "Insert SQL: $sql" }
        coroutineScope {
            documents.chunked(BATCH_SIZE).collect { docs ->
                val batch = this@coroutineScope.launch {
                    saveBatchAndReport(total, sql, docs, tableName.toString(), fields)
                }
                if (!dialect.supportsConcurrentInserts) {
                    batch.join()
                } else {
                    jobs += batch
                    jobs.reduce(MAX_PENDING_ROWS / BATCH_SIZE)
                }
            }
        }
        logger.info { "Total of $total rows inserted into '$tableName'" }
    }

    @OptIn(ExperimentalAtomicApi::class)
    private fun saveBatchAndReport(
        total: AtomicInt,
        sql: String,
        docs: List<TransferDocument>,
        tableName: String,
        fields: Map<String, FieldDefinition>
    ) {
        total += pool.withConnection {
            it.prepareStatement(sql).use { stmt ->
                saveBatch(stmt, docs, tableName, fields)
            }
        }
        if (total.load() % REPORT_ON_INSERTS == 0) {
            logger.info { "Inserted $total rows into '$tableName'" }
        }
    }

    private fun saveBatch(
        stmt: PreparedStatement,
        docs: List<TransferDocument>,
        tableName: String,
        fields: Map<String, FieldDefinition>
    ): Int {
        for (doc in docs) {
            check(doc.keys.size <= fields.size) {
                "Table '${tableName}' has less columns than row:\n" +
                    "table=${fields.values.joinToString(", ") { it.name }}\n" +
                    "row=${doc.keys.joinToString(", ") { it }}"
            }
            val values =
                doc.entries.associate { it.key.lowercase() to it.value } // I don't really like this
            var i = 1
            fields.forEach { field ->
                stmt.setObject(i++, castValue(field.value.type, values[field.key]))
            }
            stmt.addBatch()
        }
        return stmt.executeBatch().size
    }

    internal open fun castValue(type: String, value: Any?): Any? = value

    override fun close() {
        if (closePool) pool.close()
    }
}
