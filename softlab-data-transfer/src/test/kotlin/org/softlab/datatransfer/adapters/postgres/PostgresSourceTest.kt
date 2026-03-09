package org.softlab.datatransfer.adapters.postgres

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.SQLException
import kotlin.test.assertEquals


class PostgresSourceTest {
    @Test
    fun `test invalid connection string throws exception`() {
        val exc = assertThrows<SQLException> {
            PostgresSource("invalid://connection:string", "user", "pass")
        }
        assertEquals("08001", exc.sqlState)
    }
}
