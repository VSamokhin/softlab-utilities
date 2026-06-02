package org.softlab.datatransfer.adapters.jdbc

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.contains
import org.junit.jupiter.api.Test
import org.softlab.datataset.test.initiators.withTestConnection
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class JdbcDialectTest {
    @Test
    fun `listTables() returns JDBC tables`() {
        withTestConnection { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE users (id INTEGER);")
                statement.execute("CREATE TABLE products (id INTEGER);")
            }

            val tables = TestJdbcDialect().listTables(connection).map { it.table }.toList()

            assertThat(tables, contains("products", "users"))
        }
    }

    @Test
    fun `listColumns() returns column definitions from JDBC metadata`() {
        withTestConnection { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE users (id INTEGER NOT NULL, name TEXT);")
            }

            val columns = TestJdbcDialect().listColumns(JdbcTableName(null, "users"), connection)

            assertThat(columns.map { it.name }, contains("id", "name"))
            assertThat(columns.map { it.type }, contains("integer", "text"))
            assertFalse(columns.first { it.name == "id" }.nullable)
            assertTrue(columns.first { it.name == "name" }.nullable)
        }
    }

    @Test
    fun `tableExists() returns table presence`() {
        withTestConnection { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE users (id INTEGER);")
            }

            val dialect = TestJdbcDialect()

            assertTrue(dialect.tableExists(JdbcTableName(null, "users"), connection))
            assertFalse(dialect.tableExists(JdbcTableName(null, "missing"), connection))
        }
    }
}
