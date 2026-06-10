package org.softlab.datatransfer.adapters.jdbc

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals


class JdbcTableNameTest {
    @Test
    fun `toString() includes schema when present`() {
        assertEquals("schema1.users", JdbcTableName("schema1", "users").toString())
    }

    @Test
    fun `toString() returns table only when schema absent`() {
        assertEquals("users", JdbcTableName(null, "users").toString())
    }
}
