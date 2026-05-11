package org.softlab.datatransfer.adapters.sqlite

import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.apache.commons.dbcp2.BasicDataSource
import org.junit.jupiter.api.Test


class SqliteConnectionPoolTest {
    @Test
    fun `constructor configures SQLite data source`() {
        val dataSource = mockk<BasicDataSource> {
            every { driverClassName = any() } just Runs
            every { url = any() } just Runs
            every { username = any() } just Runs
            every { password = any() } just Runs
            every { maxTotal = any() } just Runs
        }

        SqliteConnectionPool(
            jdbcUrl = "jdbc:sqlite:/tmp/test.db",
            maxTotal = 3,
            source = dataSource
        )

        verify(exactly = 1) { dataSource.driverClassName = "org.sqlite.JDBC" }
        verify(exactly = 1) { dataSource.url = "jdbc:sqlite:/tmp/test.db" }
        verify(exactly = 1) { dataSource.username = null }
        verify(exactly = 1) { dataSource.password = null }
        verify(exactly = 1) { dataSource.maxTotal = 3 }
    }

    @Test
    fun `constructor uses single connection by default`() {
        val dataSource = mockk<BasicDataSource> {
            every { driverClassName = any() } just Runs
            every { url = any() } just Runs
            every { username = any() } just Runs
            every { password = any() } just Runs
            every { maxTotal = any() } just Runs
        }

        SqliteConnectionPool("jdbc:sqlite:/tmp/test.db", source = dataSource)

        verify(exactly = 1) { dataSource.maxTotal = 1 }
    }
}

