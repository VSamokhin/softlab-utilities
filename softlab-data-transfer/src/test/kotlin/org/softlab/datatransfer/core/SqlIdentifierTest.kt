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

    @Test
    fun `primary key sqlName() returns explicit name`() {
        assertEquals(
            "users_pk",
            PrimaryKeyDefinition("users_pk", listOf("id")).sqlName(SqlTableName(null, "users"))
        )
    }

    @Test
    fun `primary key sqlName() returns fallback name`() {
        assertEquals(
            "users_pkey",
            PrimaryKeyDefinition(null, listOf("id")).sqlName(SqlTableName(null, "users"))
        )
    }

    @Test
    fun `index sqlName() returns explicit name`() {
        assertEquals(
            "users_email_idx",
            IndexDefinition("users_email_idx", listOf(IndexFieldDefinition("email")))
                .sqlName(SqlTableName(null, "users"))
        )
    }

    @Test
    fun `index sqlName() returns fallback name`() {
        assertEquals(
            "users_tenant_id_email_idx",
            IndexDefinition(
                null,
                listOf(IndexFieldDefinition("tenant_id"), IndexFieldDefinition("email"))
            ).sqlName(SqlTableName(null, "users"))
        )
    }
}
