package org.softlab.datatransfer.adapters.sqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.softlab.datataset.test.initiators.withTestConnection
import org.softlab.datatransfer.adapters.jdbc.JdbcTableName
import kotlin.test.assertEquals
import kotlin.test.assertFalse


class SqliteDialectTest {
    @ParameterizedTest
    @ValueSource(strings = [ "users", "schema.users" ])
    fun `parseTableName() treats full value as table name`(actualTable: String) {
        assertEquals(
            JdbcTableName(null, "users"),
            SqliteDialect().parseTableName(actualTable)
        )
    }

    @Test
    fun `properties describe SQLite behavior`() {
        val dialect = SqliteDialect()

        assertEquals("sqlite", dialect.backendName)
        assertFalse(dialect.supportsConcurrentInserts)
    }

    @Test
    fun `listTables() excludes SQLite internal tables`() {
        withTestConnection { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT);")
            }

            val tables = SqliteDialect().listTables(connection).map { it.table }.toList()

            assertEquals(listOf("users"), tables)
        }
    }
}
