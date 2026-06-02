package org.softlab.dataset.redis

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith


class RedisTableMappingsTest {
    @Test
    fun `table() returns matching mapping by name`() {
        val expected = RedisTableMapping(table = "users")
        val mappings = RedisTableMappings(
            tables = listOf(expected, RedisTableMapping(table = "roles"))
        )

        assertEquals(expected, mappings.table("users"))
    }

    @Test
    fun `table() throws when mapping is missing`() {
        val mappings = RedisTableMappings(
            tables = listOf(RedisTableMapping(table = "users"))
        )

        val exc = assertFailsWith<IllegalStateException> {
            mappings.table("roles")
        }

        assertThat(exc.message, containsString("roles"))
    }
}
