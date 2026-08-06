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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.softlab.dataset.jdbc.JdbcConnectionPool
import org.softlab.dataset.jdbc.withConnection
import org.softlab.datatransfer.core.BATCH_SIZE
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DocumentCollection
import org.softlab.datatransfer.core.TransferDocument
import java.sql.ResultSet


open class JdbcDocumentCollection(
    private val tableName: JdbcTableName,
    private val pool: JdbcConnectionPool,
    private val dialect: JdbcDialect
) : DocumentCollection {
    private val logger = KotlinLogging.logger {}

    private val metadata: CollectionMetadata by lazy {
        pool.withConnection { connection ->
            if (dialect.tableExists(tableName, connection)) {
                val columns = dialect.listColumns(tableName, connection)
                CollectionMetadata(
                    name = tableName.toString(),
                    fields = columns,
                    primaryKey = dialect.readPrimaryKey(tableName, connection),
                    indices = dialect.readIndices(tableName, connection)
                )
            } else error("Table '$tableName' does not exist")
        }
    }

    override suspend fun fetchMetadata(): CollectionMetadata = metadata

    override fun readDocuments(): Flow<TransferDocument> = flow {
        pool.withConnection { connection ->
            val sql = "SELECT * FROM $tableName;"
            logger.trace { "Executing SQL: $sql" }
            val commit = connection.autoCommit
            connection.autoCommit = false
            connection.prepareStatement(sql).use { stmt ->
                stmt.fetchSize = BATCH_SIZE
                stmt.executeQuery().use { rs ->
                    val columns = Array(rs.metaData.columnCount) {
                        rs.metaData.getColumnName(it + 1).lowercase()
                    }
                    while (rs.next()) {
                        val row = mutableMapOf<String, Any?>()
                        columns.forEachIndexed { i, c ->
                            row[c] = rs.getTypedObject(i + 1, metadata.fields[c]!!.type)
                        }
                        emit(row)
                    }
                }
            }
            connection.autoCommit = commit
        }
    }

    protected open fun ResultSet.getTypedObject(columnIndex: Int, type: String): Any? =
        getObject(columnIndex)
}
