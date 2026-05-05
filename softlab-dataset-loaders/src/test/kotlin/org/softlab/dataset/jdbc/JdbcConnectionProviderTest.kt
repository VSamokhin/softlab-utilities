package org.softlab.dataset.jdbc

import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Connection
import java.sql.Statement
import kotlin.test.assertEquals


class JdbcConnectionProviderTest {
    @Test
    fun `withConnection() opens connection closes it and returns block result`() {
        val connection = mockk<Connection>(relaxed = true) {
            every { close() } just Runs
        }
        val provider = TestJdbcConnectionProvider(connection)

        val result = provider.withConnection {
            assertEquals(connection, it)
            "done"
        }

        assertEquals("done", result)
        assertEquals(1, provider.openConnectionCalls)
        verify(exactly = 1) { connection.close() }
    }

    @Test
    fun `withConnection() closes connection when block throws`() {
        val connection = mockk<Connection>(relaxed = true) {
            every { close() } just Runs
        }
        val provider = TestJdbcConnectionProvider(connection)

        val exc = assertThrows<IllegalStateException> {
            provider.withConnection {
                throw IllegalStateException("failed")
            }
        }

        assertEquals("failed", exc.message)
        verify(exactly = 1) { connection.close() }
    }

    @Test
    fun `withStatement() opens connection creates statement and closes both`() {
        val statement = mockk<Statement>(relaxed = true) {
            every { close() } just Runs
        }
        val connection = mockk<Connection>(relaxed = true) {
            every { createStatement() } returns statement
            every { close() } just Runs
        }

        val provider = TestJdbcConnectionProvider(connection)

        val result = provider.withStatement {
            assertEquals(statement, it)
            42
        }

        assertEquals(42, result)
        assertEquals(1, provider.openConnectionCalls)
        verify(exactly = 1) { connection.createStatement() }
        verify(exactly = 1) { statement.close() }
        verify(exactly = 1) { connection.close() }
    }

    @Test
    fun `withStatement() closes statement and connection when block throws`() {
        val statement = mockk<Statement>(relaxed = true) {
            every { close() } just Runs
        }
        val connection = mockk<Connection>(relaxed = true) {
            every { createStatement() } returns statement
            every { close() } just Runs
        }
        val provider = TestJdbcConnectionProvider(connection)

        val exc = assertThrows<IllegalArgumentException> {
            provider.withStatement {
                throw IllegalArgumentException("bad statement")
            }
        }

        assertEquals("bad statement", exc.message)
        verify(exactly = 1) { statement.close() }
        verify(exactly = 1) { connection.close() }
    }

    private class TestJdbcConnectionProvider(
        private val connection: Connection
    ) : JdbcConnectionProvider {
        var openConnectionCalls = 0
            private set

        override fun openConnection(): Connection {
            openConnectionCalls++
            return connection
        }

        override val closed: Boolean
            get() = error("not implemented")

        override fun close() = error("not implemented")
    }
}
