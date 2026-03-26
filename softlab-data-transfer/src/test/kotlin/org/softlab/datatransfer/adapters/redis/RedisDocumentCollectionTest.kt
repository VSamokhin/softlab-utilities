package org.softlab.datatransfer.adapters.redis

import io.lettuce.core.api.sync.RedisCommands
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.redis.RedisHashMapping
import org.softlab.dataset.redis.RedisTableMapping
import java.sql.Timestamp
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals


class RedisDocumentCollectionTest {
    @Test
    fun `readDocuments() reconstructs rows and restores field types`() = runBlocking {
        val commands = mockk<RedisCommands<String, String>>()
        every { commands.keys("users:*") } returns listOf("users:1")
        every { commands.hgetall("users:1") } returns linkedMapOf(
            "name" to "Alice",
            "active" to "true",
            "created_at" to "2024-01-01T10:15:30Z",
            "payload" to "YWJj"
        )

        val cut = RedisDocumentCollection(
            RedisTableMapping(
                table = "users",
                fields = listOf(
                    FieldDefinition("id", "integer"),
                    FieldDefinition("name", "text"),
                    FieldDefinition("active", "boolean"),
                    FieldDefinition("created_at", "timestamp with time zone"),
                    FieldDefinition("payload", "bytea")
                ),
                hashes = listOf(RedisHashMapping(key = "users:\${id}"))
            ),
            commands
        )

        val documents = cut.readDocuments().toList()

        assertEquals(1, documents.size)
        assertEquals(1, documents.single()["id"])
        assertEquals("Alice", documents.single()["name"])
        assertEquals(true, documents.single()["active"])
        assertEquals(Timestamp.from(java.time.Instant.parse("2024-01-01T10:15:30Z")), documents.single()["created_at"])
        assertContentEquals("abc".toByteArray(), documents.single()["payload"] as ByteArray)
    }
}
