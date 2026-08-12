package org.softlab.datatransfer.adapters.jdbc

import org.softlab.datatransfer.core.SqlTableName
import java.sql.Connection


internal class TestJdbcDialect(
    backendName: String = "jdbc-test",
    supportsConcurrentInserts: Boolean = false
) : JdbcDialect(
    backendName = backendName,
    supportsConcurrentInserts = supportsConcurrentInserts
) {
    var beforeCreateTableCalls: List<SqlTableName> = emptyList()

    override fun parseTableName(tableName: String): SqlTableName =
        SqlTableName.of(tableName).copy(schema = null)

    override fun beforeCreateTable(tableName: SqlTableName, connection: Connection) {
        beforeCreateTableCalls += tableName
    }
}
