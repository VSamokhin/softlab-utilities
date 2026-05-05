package org.softlab.datatransfer.adapters.postgres

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


class ConnectionPoolTest {
    @Test
    fun `openConnection() returns connection from data source`() {
        val conn = mockk<Connection>()
        val dataSource = mockk<BasicDataSource>(relaxed = true) {
            every { connection } returns conn
        }

        val result = ConnectionPool("", source = dataSource).openConnection()

        assertSame(conn, result)
        verify(exactly = 1) { dataSource.connection }
    }

    @Test
    fun `closed returns data source closed state`() {
        val dataSource = mockk<BasicDataSource>(relaxed = true) {
            every { isClosed } returns false andThen true
        }
        val pool = ConnectionPool("", source = dataSource)

        assertFalse(pool.closed)
        assertTrue(pool.closed)
        verify(exactly = 2) { dataSource.isClosed }
    }

    @Test
    fun `close() closes data source`() {
        val dataSource = mockk<BasicDataSource>(relaxed = true) {
            every { close() } just Runs
        }

        ConnectionPool("", source = dataSource).close()

        verify(exactly = 1) { dataSource.close() }
    }

    @Test
    fun `constructor configures PostgreSQL data source`() {
        val dataSource = mockk<BasicDataSource> {
            every { driverClassName = any() } just Runs
            every { url = any() } just Runs
            every { username = any() } just Runs
            every { password = any() } just Runs
            every { maxTotal = any() } just Runs
        }

        ConnectionPool(
            jdbcUrl = "jdbc:postgresql://localhost:5432/db",
            username = "db-user",
            password = "db-pass",
            maxTotal = 3,
            dataSource
        )

        verify(exactly = 1) { dataSource.driverClassName = "org.postgresql.Driver" }
        verify(exactly = 1) { dataSource.url = "jdbc:postgresql://localhost:5432/db" }
        verify(exactly = 1) { dataSource.username = "db-user" }
        verify(exactly = 1) { dataSource.password = "db-pass" }
        verify(exactly = 1) { dataSource.maxTotal = 3 }
    }

    @Test
    fun `constructor uses default values`() {
        val dataSource = mockk<BasicDataSource> {
            every { driverClassName = any() } just Runs
            every { url = any() } just Runs
            every { username = any() } just Runs
            every { password = any() } just Runs
            every { maxTotal = any() } just Runs
        }

        ConnectionPool("jdbc:postgresql://localhost:5432/db", source = dataSource)

        verify(exactly = 1) { dataSource.driverClassName = "org.postgresql.Driver" }
        verify(exactly = 1) { dataSource.url = "jdbc:postgresql://localhost:5432/db" }
        verify(exactly = 1) { dataSource.username = null }
        verify(exactly = 1) { dataSource.password = null }
        verify(exactly = 1) { dataSource.maxTotal = 10 }
    }
}
