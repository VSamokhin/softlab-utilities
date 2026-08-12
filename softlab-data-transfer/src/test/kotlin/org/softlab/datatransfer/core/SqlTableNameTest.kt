package org.softlab.datatransfer.core

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SqlTableNameTest {
    @Test
    fun `of() returns table without schema when value has no dot`() {
        assertEquals(SqlTableName(null, "users"), SqlTableName.of("users"))
    }

    @Test
    fun `of() splits schema from table name at first dot`() {
        assertEquals(SqlTableName("schema1", "users"), SqlTableName.of("schema1.users"))
    }

    @Test
    fun `of() keeps dots after schema in table name`() {
        assertEquals(SqlTableName("schema2", "table.table"), SqlTableName.of("schema2.table.table"))
    }

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
