package org.softlab.datatransfer.adapters.jdbc

import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.apache.commons.dbcp2.BasicDataSource
import org.junit.jupiter.api.Test
import java.sql.Connection
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue


class JdbcConnectionPoolTest {
    @Test
    fun `openConnection() returns connection from data source`() {
        val conn = mockk<Connection>()
        val dataSource = mockk<BasicDataSource>(relaxed = true) {
            every { connection } returns conn
        }

        val result = JdbcConnectionPool("", "", source = dataSource).openConnection()

        assertSame(conn, result)
        verify(exactly = 1) { dataSource.connection }
    }

    @Test
    fun `closed returns data source closed state`() {
        val dataSource = mockk<BasicDataSource>(relaxed = true) {
            every { isClosed } returns false andThen true
        }
        val pool = JdbcConnectionPool("", "", source = dataSource)

        assertFalse(pool.closed)
        assertTrue(pool.closed)
        verify(exactly = 2) { dataSource.isClosed }
    }

    @Test
    fun `close() closes data source`() {
        val dataSource = mockk<BasicDataSource>(relaxed = true) {
            every { close() } just Runs
        }

        JdbcConnectionPool("", "", source = dataSource).close()

        verify(exactly = 1) { dataSource.close() }
    }
}

