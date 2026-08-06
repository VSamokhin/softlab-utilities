package org.softlab.datatransfer.adapters.postgres

import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.softlab.datatransfer.core.SqlTableName
import kotlin.test.assertEquals
import kotlin.test.assertTrue


class PostgresDialectTest {
    companion object {
        @JvmStatic
        fun tableNames(): List<Arguments> = listOf(
            Arguments.of("schema1.users", SqlTableName("schema1", "users")),
            Arguments.of("users", SqlTableName("public", "users")),
            Arguments.of("schema2.table.table", SqlTableName("schema2", "table.table"))
        )
    }

    @ParameterizedTest
    @MethodSource("tableNames")
    fun `parseTableName() returns expected schema and table`(
        tableName: String,
        expected: SqlTableName
    ) {
        assertEquals(expected, PostgresDialect().parseTableName(tableName))
    }

    @Test
    fun `properties describe Postgres behavior`() {
        val dialect = PostgresDialect()

        assertEquals("postgres", dialect.backendName)
        assertTrue(dialect.supportsConcurrentInserts)
    }
}
