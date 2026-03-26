package org.softlab.dataset.redis

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals


class RedisDatasetMapperTest {
    @Test
    fun `mapRows() maps rows to hashes and sets`() {
        val rows = listOf(
            mapOf("id" to 1, "name" to "Alice", "payload" to "abc".toByteArray())
        )
        val mapping = RedisTableMapping(
            table = "users",
            hashes = listOf(RedisHashMapping(key = "users:\${id}")),
            sets = listOf(RedisSetMapping(key = "users", member = "\${id}"))
        )

        val result = RedisDatasetMapper.mapRows(rows, mapping)

        assertEquals(mapOf("users:1" to mapOf("id" to "1", "name" to "Alice", "payload" to "YWJj")), result.hashes)
        assertEquals(mapOf("users" to setOf("1")), result.sets)
    }

    @Test
    fun `mapRows() throws on duplicate hash field`() {
        val rows = listOf(
            mapOf("id" to 1, "name" to "Alice"),
            mapOf("id" to 1, "name" to "Bob")
        )
        val mapping = RedisTableMapping(
            table = "users",
            hashes = listOf(RedisHashMapping(key = "users:\${id}"))
        )

        val exc = assertThrows<IllegalStateException> {
            RedisDatasetMapper.mapRows(rows, mapping)
        }

        assertEquals(
            "Duplicate field found in hash, please assure the mapping is correct: users:1/id",
            exc.message
        )
    }

    @Test
    fun `mapRows() encodes binary values as base64`() {
        val result = RedisDatasetMapper.mapRows(
            rows = listOf(mapOf("id" to 1, "payload" to "abc".toByteArray())),
            mappings = RedisTableMapping(
                table = "users",
                hashes = listOf(RedisHashMapping(key = "users:\${id}"))
            )
        )

        assertEquals("YWJj", result.hashes.getValue("users:1").getValue("payload"))
    }
}
