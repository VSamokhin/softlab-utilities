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

package org.softlab.datatransfer.adapters.postgres

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.softlab.datatransfer.core.BATCH_SIZE
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DocumentCollection
import org.softlab.datatransfer.core.TransferDocument
import org.softlab.datatransfer.util.Postgres
import java.sql.Connection


class PostgresDocumentCollection(
    private val schemaName: String,
    private val tableName: String,
    private val connection: Connection
) : DocumentCollection {
    private val logger = KotlinLogging.logger {}

    private val metadata: CollectionMetadata by lazy {
        if (Postgres.tableExists(schemaName, tableName, connection)) {
            val columns = Postgres.readColumns(schemaName, tableName, connection)
            CollectionMetadata("$schemaName.$tableName", columns)
        } else error("Table '$schemaName.$tableName' does not exist")
    }

    override suspend fun fetchMetadata(): CollectionMetadata = metadata

    override fun readDocuments(): Flow<TransferDocument> = flow {
        val sql = "SELECT * FROM $schemaName.$tableName;"
        logger.trace { "Executing SQL: $sql" }
        val commit = connection.autoCommit
        connection.autoCommit = false
        connection.prepareStatement(sql).use { stmt ->
            stmt.fetchSize = BATCH_SIZE
            stmt.executeQuery().use { rs ->
                val columns = Array(rs.metaData.columnCount) {
                    rs.metaData.getColumnName(it + 1)
                }
                while (rs.next()) {
                    val row = mutableMapOf<String, Any?>()
                    columns.forEachIndexed { i, c ->
                        row[c] = rs.getObject(i + 1)
                    }
                    emit(row)
                }
            }
        }
        connection.autoCommit = commit
    }
}
