package org.softlab.datatransfer.core

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SqlTableNameTest {
    @Test
    fun `toString() includes unquoted schema when present`() {
        assertEquals("schema1.users", SqlTableName("schema1", "users").toString())
    }

    @Test
    fun `toString() returns unquoted table only when schema is absent`() {
        assertEquals("users", SqlTableName(null, "users").toString())
    }

    @Test
    fun `toSql() quotes schema and table when schema is present`() {
        assertEquals("\"schema1\".\"users\"", SqlTableName("schema1", "users").toSql())
    }

    @Test
    fun `toSql() quotes table only when schema is absent`() {
        assertEquals("\"users\"", SqlTableName(null, "users").toSql())
    }

    @Test
    fun `toSql() escapes quotes in identifiers`() {
        assertEquals("\"my\"\"schema\".\"my\"\"table\"", SqlTableName("my\"schema", "my\"table").toSql())
    }
}
