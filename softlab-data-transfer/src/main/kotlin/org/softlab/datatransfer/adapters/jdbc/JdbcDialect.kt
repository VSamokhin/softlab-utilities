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

import org.softlab.dataset.core.FieldDefinition
import java.sql.Connection


abstract class JdbcDialect(
    val backendName: String,
    val supportsConcurrentInserts: Boolean
) {
    abstract fun parseTableName(tableName: String): JdbcTableName

    open fun listTables(connection: Connection): Sequence<JdbcTableName> = sequence {
        connection.metaData.getTables(null, null, "%", arrayOf("TABLE")).use { rs ->
            while (rs.next()) {
                yield(
                    JdbcTableName(
                        schema = rs.getString("TABLE_SCHEM"),
                        table = rs.getString("TABLE_NAME")
                    )
                )
            }
        }
    }

    open fun listColumns(tableName: JdbcTableName, connection: Connection): List<FieldDefinition> {
        connection.metaData
            .getColumns(null, tableName.schema, tableName.table, "%")
            .use { rs ->
                val result = mutableListOf<FieldDefinition>()
                while (rs.next()) {
                    result.add(
                        FieldDefinition(
                            rs.getString("COLUMN_NAME"),
                            rs.getString("TYPE_NAME").lowercase(),
                            rs.getInt("NULLABLE") != java.sql.DatabaseMetaData.columnNoNulls
                        )
                    )
                }
                return result
            }
    }

    open fun tableExists(tableName: JdbcTableName, connection: Connection): Boolean =
        connection.metaData
            .getTables(null, tableName.schema, tableName.table, arrayOf("TABLE"))
            .use { rs -> rs.next() }

    open fun beforeCreateTable(tableName: JdbcTableName, connection: Connection) {
        // Default dialects do not need schema setup.
    }
}
