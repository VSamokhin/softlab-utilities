package org.softlab.datatransfer.adapters.sqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.softlab.datatransfer.core.SqlTableName
import kotlin.test.assertEquals
import kotlin.test.assertFalse


class SqliteDialectTest {
    @ParameterizedTest
    @ValueSource(strings = [ "users", "schema.users" ])
    fun `parseTableName() treats full value as table name`(actualTable: String) {
        assertEquals(
            SqlTableName(null, "users"),
            SqliteDialect().parseTableName(actualTable)
        )
    }

    @Test
    fun `properties describe SQLite behavior`() {
        val dialect = SqliteDialect()

        assertEquals("sqlite", dialect.backendName)
        assertFalse(dialect.supportsConcurrentInserts)
    }
}
