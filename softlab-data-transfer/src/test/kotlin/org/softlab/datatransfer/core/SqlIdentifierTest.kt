package org.softlab.datatransfer.core

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals


class SqlIdentifierTest {
    @Test
    fun `toSqlIdentifier() quotes identifier`() {
        assertEquals("\"users\"", "users".toSqlIdentifier())
    }

    @Test
    fun `toSqlIdentifier() preserves spaces and case inside quoted identifier`() {
        assertEquals("\"User Accounts\"", "User Accounts".toSqlIdentifier())
    }

    @Test
    fun `toSqlIdentifier() escapes quotes in identifier`() {
        assertEquals("\"my\"\"table\"", "my\"table".toSqlIdentifier())
    }

    @Test
    fun `toSql() returns sort order keyword`() {
        assertEquals("ASC", IndexSortOrder.ASC.toSql())
        assertEquals("DESC", IndexSortOrder.DESC.toSql())
    }
}
