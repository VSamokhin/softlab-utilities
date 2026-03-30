package org.softlab.dataset.redis

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith


class RedisTableMappingTest {
    @Test
    fun `anchorHash() prefers row-level hash without field and value`() {
        val mapping = RedisTableMapping(
            table = "users",
            hashes = listOf(
                RedisHashMapping(key = "users:\${id}:status", field = "status", value = "\${status}"),
                RedisHashMapping(key = "users:\${id}"),
                RedisHashMapping(key = "users:\${id}:flags", field = "flag", value = "\${flag}")
            )
        )

        assertEquals(RedisHashMapping(key = "users:\${id}"), mapping.anchorHash())
    }

    @Test
    fun `anchorHash() throws when no row-level hash exists`() {
        val mapping = RedisTableMapping(
            table = "users",
            hashes = listOf(
                RedisHashMapping(key = "users:\${id}:status", field = "status", value = "\${status}"),
                RedisHashMapping(key = "users:\${id}:flags", field = "flag", value = "\${flag}")
            )
        )

        val exc = assertFailsWith<IllegalStateException> {
            mapping.anchorHash()
        }

        assertEquals(
            "Redis table 'users' must define a row-level anchor hash without field/value mappings",
            exc.message
        )
    }

    @Test
    fun `table() returns matching mapping by name`() {
        val expected = RedisTableMapping(table = "users")
        val mappings = RedisTableMappings(
            tables = listOf(expected, RedisTableMapping(table = "roles"))
        )

        assertEquals(expected, mappings.table("users"))
    }
}
