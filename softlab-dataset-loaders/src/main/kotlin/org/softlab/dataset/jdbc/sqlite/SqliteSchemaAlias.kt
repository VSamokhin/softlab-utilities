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

package org.softlab.dataset.jdbc.sqlite

import java.sql.Connection
import java.sql.DatabaseMetaData


fun Connection.sqliteAttach(dbFile: String, alias: String): Connection {
    assertSqlite(metaData)

    createStatement().use { stmt ->
        stmt.execute("ATTACH DATABASE '${dbFile.replace("\\", "/")}' AS $alias;")
    }
    return this
}

/**
 * Return pairs of database alias — database source strings,
 * where the source can be null e.g. for in-memory databases
 */
fun Connection.sqliteListDbs(): Map<String, String> {
    assertSqlite(metaData)

    val dbs = linkedMapOf<String, String>()
    createStatement().use { stmt ->
        stmt.executeQuery("PRAGMA database_list;").use { rs ->
            while (rs.next()) {
                dbs += rs.getString("name") to rs.getString("file")
            }
        }
    }
    return dbs
}

private fun assertSqlite(metaData: DatabaseMetaData) {
    require(metaData.driverName == "SQLite JDBC") {
        "Not an SQLite database connection: ${metaData.driverName}"
    }
}
