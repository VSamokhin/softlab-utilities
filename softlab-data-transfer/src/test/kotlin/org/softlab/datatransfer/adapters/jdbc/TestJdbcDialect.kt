package org.softlab.datatransfer.adapters.jdbc

import java.sql.Connection


internal class TestJdbcDialect(
    backendName: String = "jdbc-test",
    supportsConcurrentInserts: Boolean = false
) : JdbcDialect(
    backendName = backendName,
    supportsConcurrentInserts = supportsConcurrentInserts
) {
    var beforeCreateTableCalls: List<JdbcTableName> = emptyList()

    override fun parseTableName(tableName: String): JdbcTableName = JdbcTableName(null, tableName)

    override fun beforeCreateTable(tableName: JdbcTableName, connection: Connection) {
        beforeCreateTableCalls += tableName
    }
}
