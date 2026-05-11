package org.softlab.datatransfer.adapters.postgres

import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.apache.commons.dbcp2.BasicDataSource
import org.junit.jupiter.api.Test


class PostgresConnectionPoolTest {
    @Test
    fun `constructor configures PostgreSQL data source`() {
        val dataSource = mockk<BasicDataSource> {
            every { driverClassName = any() } just Runs
            every { url = any() } just Runs
            every { username = any() } just Runs
            every { password = any() } just Runs
            every { maxTotal = any() } just Runs
        }

        PostgresConnectionPool(
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

        PostgresConnectionPool("jdbc:postgresql://localhost:5432/db", source = dataSource)

        verify(exactly = 1) { dataSource.driverClassName = "org.postgresql.Driver" }
        verify(exactly = 1) { dataSource.url = "jdbc:postgresql://localhost:5432/db" }
        verify(exactly = 1) { dataSource.username = null }
        verify(exactly = 1) { dataSource.password = null }
        verify(exactly = 1) { dataSource.maxTotal = 10 }
    }
}
