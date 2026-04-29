package org.softlab.datatransfer.adapters.postgres

import kotlinx.coroutines.runBlocking
import org.hamcrest.CoreMatchers.containsString
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.SQLException


class PostgresSourceTest {
    @Test
    fun `invalid connection string throws on first use`() {
        val exc = assertThrows<SQLException> {
            runBlocking {
                PostgresSource(
                    ConnectionPool(
                        "invalid://connection:string",
                        "user",
                        "pass"
                    )
                ).use { cut ->
                    cut.countDocuments("public.missing")
                }
            }
        }
        assertThat(exc.message, containsString("'invalid://connection:string'"))
    }
}
