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
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.core.DocumentCollection
import java.sql.Connection
import java.sql.DriverManager


class PostgresSource(
    private val connection: Connection,
    private val closeConnection: Boolean = true
) : DatabaseSource {
    constructor(
        jdbcUrl: String,
        username: String,
        password: String
    ) : this(DriverManager.getConnection(jdbcUrl, username, password))

    constructor(jdbcUrl: String): this(DriverManager.getConnection(jdbcUrl))

    override fun listCollections(): Flow<DocumentCollection> = flow {
        connection.metaData
            .getTables(null, null, "%", arrayOf("TABLE"))
            .use { rs ->
                while (rs.next()) {
                    val schemaName = rs.getString("TABLE_SCHEM")!!
                    val tableName = rs.getString("TABLE_NAME")!!
                    emit(PostgresDocumentCollection(schemaName, tableName, connection))
                }
            }
    }

    override suspend fun countDocuments(collectionName: String): Long {
        connection.createStatement().use { stmt ->
            stmt.executeQuery("SELECT COUNT(*) FROM $collectionName;").use { rs ->
                return if (rs.next()) rs.getLong(1) else error("Could not count rows in $collectionName")
            }
        }
    }

    override fun close() {
        if (closeConnection) connection.close()
    }
}
