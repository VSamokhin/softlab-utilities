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
import kotlinx.coroutines.flow.Flow
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.TransferDocument
import java.sql.Connection
import java.sql.DriverManager


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
        val columnsDef = metadata.fields.joinToString(", ") {
            "${it.name} ${mapType(it.type)}${if (!it.nullable) " NOT NULL" else ""}"
        }
        val sql = "CREATE TABLE ${metadata.name} ($columnsDef);"
        connection.createStatement().use {
            logger.trace { "Executing SQL: $sql" }
            it.execute(sql)
        }
    }

    private fun mapType(type: String): String {
        return dataTypeMappings[type.lowercase()] ?: error("Unsupported type: $type")
    }

    override suspend fun insertDocuments(collectionName: String, documents: Flow<TransferDocument>) {
        documents.collect { doc ->
            val columns = doc.keys.joinToString(", ") { it }
            val placeholders = doc.keys.joinToString(", ") { "?" }
            connection.prepareStatement(
                "INSERT INTO $collectionName ($columns) VALUES ($placeholders);"
            ).use { stmt ->
                doc.values.forEachIndexed { i, value ->
                    stmt.setObject(i + 1, value)
                }
                stmt.executeUpdate()
            }
        }
    }

    override fun close() {
        if (closeConnection) connection.close()
    }
}
