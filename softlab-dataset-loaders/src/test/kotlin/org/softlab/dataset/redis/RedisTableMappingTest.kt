package org.softlab.dataset.redis

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.containsString
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

        assertThat(exc.message, allOf(
            containsString("users"),
            containsString("row-level anchor")
        ))
    }
}
