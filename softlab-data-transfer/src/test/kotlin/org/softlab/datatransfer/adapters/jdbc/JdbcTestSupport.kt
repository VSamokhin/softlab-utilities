package org.softlab.datatransfer.adapters.jdbc

import org.softlab.dataset.jdbc.JdbcConnectionProvider
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager


internal class TestJdbcProvider private constructor(
    private val dbFile: Path
) : JdbcConnectionProvider {
    private var closedFlag = false

    override fun openConnection(): Connection = DriverManager.getConnection("jdbc:sqlite:$dbFile")

    override val closed: Boolean get() = closedFlag

    override fun close() {
        closedFlag = true
        Files.deleteIfExists(dbFile)
    }

    companion object {
        fun create(): TestJdbcProvider =
            TestJdbcProvider(Files.createTempFile("data-transfer-jdbc-test", ".db"))
    }
}

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
