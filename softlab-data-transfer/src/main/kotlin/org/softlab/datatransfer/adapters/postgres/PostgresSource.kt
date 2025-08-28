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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.softlab.datatransfer.core.Collection
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.core.Document
import org.softlab.datatransfer.core.FieldMetadata
import java.sql.Connection
import java.sql.DriverManager


class PostgresSource(private val connString: String) : DatabaseSource {
    private val connection: Connection = DriverManager.getConnection(connString)

    override fun listCollections(): List<Collection> {
        val tables = mutableListOf<Collection>()
        val rs = connection.metaData.getTables(null, null, "%", arrayOf("TABLE"))
        while (rs.next()) {
            val tableName = rs.getString("TABLE_NAME")
            tables.add(PostgresCollection(connection, tableName))
        }
        return tables
    }

    override fun close() {
        connection.close()
    }
}

class PostgresCollection(
    private val connection: Connection,
    private val tableName: String
) : Collection {
    override val metadata: CollectionMetadata by lazy {
        val columns = mutableListOf<FieldMetadata>()
        val rs = connection.metaData.getColumns(null, null, tableName, "%")
        while (rs.next()) {
            columns.add(FieldMetadata(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME")))
        }
        CollectionMetadata(tableName, columns)
    }

    override fun readDocuments(): Flow<Document> = flow {
        val stmt = connection.createStatement()
        val rs = stmt.executeQuery("SELECT * FROM $tableName")
        val columnCount = rs.metaData.columnCount
        while (rs.next()) {
            val row = mutableMapOf<String, Any?>()
            for (i in 1..columnCount) {
                row[rs.metaData.getColumnName(i)] = rs.getObject(i)
            }
            emit(row)
        }
        rs.close()
        stmt.close()
    }
}
