/**
 * Copyright (C) 2026, Viktor Samokhin (wowyupiyo@gmail.com)
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

package org.softlab.datatransfer.adapters.sqlite

import org.softlab.dataset.jdbc.JdbcConnectionPool
import org.softlab.datatransfer.adapters.jdbc.JdbcDialect
import org.softlab.datatransfer.adapters.jdbc.JdbcDocumentCollection
import org.softlab.datatransfer.core.SqlTableName
import java.sql.ResultSet


class SqliteDocumentCollection(
    tableName: SqlTableName,
    pool: JdbcConnectionPool,
    dialect: JdbcDialect
) : JdbcDocumentCollection(
    tableName = tableName,
    pool = pool,
    dialect = dialect
) {
    override fun ResultSet.getTypedObject(columnIndex: Int, type: String): Any? =
        when (type.lowercase()) {
            "bigint", "int8", "long", "int64" -> getLong(columnIndex)
            "boolean", "bool", "bit" -> getBoolean(columnIndex)
            else -> getObject(columnIndex)
        }
}
