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

import org.softlab.dataset.core.FieldDefinition
import java.sql.Connection


object PostgresHelper {
    fun readColumns(schemaName: String, tableName: String, conn: Connection): List<FieldDefinition> =
        conn.createStatement().use { stmt ->
            stmt.executeQuery(
                """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = '$schemaName' AND table_name = '$tableName';
                    """.trimIndent()
            ).use { rs ->
                val result = mutableListOf<FieldDefinition>()
                while (rs.next()) {
                    result.add(
                        FieldDefinition(
                            rs.getString("column_name"),
                            rs.getString("data_type"),
                            rs.getString("is_nullable").lowercase() == "yes"
                        )
                    )
                }
                result
            }
        }

    fun tableExists(schemaName: String, tableName: String, conn: Connection): Boolean =
        conn.metaData
            .getTables(null, schemaName, tableName, arrayOf("TABLE"))
            .use { rs ->
                return rs.next()
            }
}
