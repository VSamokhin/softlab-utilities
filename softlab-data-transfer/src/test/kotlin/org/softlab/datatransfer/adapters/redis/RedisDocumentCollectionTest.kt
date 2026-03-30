package org.softlab.datatransfer.adapters.redis

import io.lettuce.core.KeyScanCursor
import io.lettuce.core.ScanArgs
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
    fun `readDocuments() reconstructs rows and restores field types via scan`() = runBlocking {
        val commands = mockk<RedisCommands<String, String>>()
        val firstPage = mockk<KeyScanCursor<String>>()
        every { firstPage.keys } returns listOf("users:1")
        every { firstPage.isFinished } returns true
        every { commands.scan(any<ScanArgs>()) } returns firstPage
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

    @Test
    fun `readDocuments() follows scan cursor across multiple pages`() = runBlocking {
        val commands = mockk<RedisCommands<String, String>>()
        val firstPage = mockk<KeyScanCursor<String>>()
        val secondPage = mockk<KeyScanCursor<String>>()
        every { firstPage.keys } returns listOf("users:1")
        every { firstPage.isFinished } returns false
        every { secondPage.keys } returns listOf("users:2")
        every { secondPage.isFinished } returns true
        every { commands.scan(any<ScanArgs>()) } returns firstPage
        every { commands.scan(firstPage, any<ScanArgs>()) } returns secondPage
        every { commands.hgetall("users:1") } returns linkedMapOf("name" to "Alice")
        every { commands.hgetall("users:2") } returns linkedMapOf("name" to "Bob")

        val cut = RedisDocumentCollection(
            RedisTableMapping(
                table = "users",
                fields = listOf(
                    FieldDefinition("id", "integer"),
                    FieldDefinition("name", "text")
                ),
                hashes = listOf(RedisHashMapping(key = "users:\${id}"))
            ),
            commands
        )

        val documents = cut.readDocuments().toList()

        assertEquals(2, documents.size)
        assertEquals(listOf(1, 2), documents.map { it["id"] })
        assertEquals(listOf("Alice", "Bob"), documents.map { it["name"] })
    }

    @Test
    fun `readDocuments() resolves additional hashes for the current row only`() = runBlocking {
        val commands = mockk<RedisCommands<String, String>>()
        val firstPage = mockk<KeyScanCursor<String>>()
        every { firstPage.keys } returns listOf("users:1")
        every { firstPage.isFinished } returns true
        every { commands.scan(any<ScanArgs>()) } returns firstPage
        every { commands.hgetall("users:1") } returns linkedMapOf("name" to "Alice")
        every { commands.hgetall("users:1:meta") } returns linkedMapOf("status" to "active")

        val cut = RedisDocumentCollection(
            RedisTableMapping(
                table = "users",
                fields = listOf(
                    FieldDefinition("id", "integer"),
                    FieldDefinition("name", "text"),
                    FieldDefinition("status", "text")
                ),
                hashes = listOf(
                    RedisHashMapping(key = "users:\${id}"),
                    RedisHashMapping(key = "users:\${id}:meta")
                )
            ),
            commands
        )

        val documents = cut.readDocuments().toList()

        assertEquals(1, documents.size)
        assertEquals(1, documents.single()["id"])
        assertEquals("Alice", documents.single()["name"])
        assertEquals("active", documents.single()["status"])
    }
}
