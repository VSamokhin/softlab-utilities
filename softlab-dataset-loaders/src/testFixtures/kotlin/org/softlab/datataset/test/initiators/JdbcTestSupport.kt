package org.softlab.datataset.test.initiators

import org.softlab.dataset.jdbc.JdbcConnectionPool
import org.softlab.dataset.jdbc.withConnection
import org.softlab.dataset.jdbc.withStatement
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement


/**
 * SQLite-based version of the pool to use in tests
 */
class TestJdbcPool private constructor(
    private val dbFile: String
) : JdbcConnectionPool {
    private var closedFlag = false

    override fun openConnection(): Connection = DriverManager.getConnection("jdbc:sqlite:$dbFile")

    override val closed: Boolean get() = closedFlag

    override fun close() {
        closedFlag = true
    }

    companion object {
        fun create(): TestJdbcPool {
            val dbFile = Files
                .createTempFile("test-jdbc-database", ".db")
                .apply { toFile().deleteOnExit() }
            return TestJdbcPool(dbFile.toString())
        }
    }
}

/**
 * Return a [JdbcConnectionPool] wrapper
 */
fun getPool(initiator: JdbcInitiator): JdbcConnectionPool = object : JdbcConnectionPool {
        var closedFlag = false

        override fun openConnection() = initiator.getConnection()

        override val closed get() = closedFlag

        override fun close() {
            closedFlag = true
        }
    }

/**
 * Convenient method to work with a [JdbcConnectionPool] wrapper and close it afterward
 */
inline fun <T> JdbcInitiator.withTestPool(block: (JdbcConnectionPool) -> T): T =
    getPool(this).use(block)

/**
 * Convenience method to work with [TestJdbcPool] and close it afterward
 */
inline fun <T> withTestPool(block: (JdbcConnectionPool) -> T): T =
    TestJdbcPool.create().use(block)

/**
 * Convenience method to work with a statement retrieved with [TestJdbcPool] and close them both afterward
 */
inline fun <T> withTestStatement(block: (Statement) -> T): T =
    withTestPool { it.withStatement(block) }

/**
 * Convenience method to work with a connection retrieved with [TestJdbcPool] and close them both afterward
 */
inline fun <T> withTestConnection(block: (Connection) -> T): T =
    withTestPool { it.withConnection(block) }