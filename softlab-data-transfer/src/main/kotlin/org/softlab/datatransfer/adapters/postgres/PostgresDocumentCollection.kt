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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.Document
import org.softlab.datatransfer.core.DocumentCollection
import java.sql.Connection


class PostgresDocumentCollection(
    private val schemaName: String,
    private val tableName: String,
    private val connection: Connection
) : DocumentCollection {

    private val metadata: CollectionMetadata by lazy {
        if (PostgresHelper.tableExists(schemaName, tableName, connection)) {
            val columns = PostgresHelper.readColumns(schemaName, tableName, connection)
            CollectionMetadata("$schemaName.$tableName", columns)
        } else error("Table '$schemaName.$tableName' does not exist")
    }

    override suspend fun fetchMetadata(): CollectionMetadata = metadata

    override fun readDocuments(): Flow<Document> = flow {
        connection.createStatement().use { stmt ->
            stmt.executeQuery("SELECT * FROM $schemaName.$tableName;").use { rs ->
                val columns = Array(rs.metaData.columnCount) { rs.metaData.getColumnName(it + 1) }
                while (rs.next()) {
                    val row = mutableMapOf<String, Any?>()
                    columns.forEachIndexed { i, c ->
                        row[c] = rs.getObject(i + 1)
                    }
                    emit(row)
                }
            }
        }
    }
}
