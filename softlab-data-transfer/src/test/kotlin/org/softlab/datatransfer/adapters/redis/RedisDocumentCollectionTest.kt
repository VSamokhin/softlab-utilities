package org.softlab.datatransfer.adapters.redis

import io.lettuce.core.KeyScanCursor
import io.lettuce.core.RedisFuture
import io.lettuce.core.ScanArgs
import io.lettuce.core.api.async.RedisAsyncCommands
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.hamcrest.CoreMatchers.containsString
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.redis.RedisHashMapping
import org.softlab.dataset.redis.RedisTableMapping
import org.softlab.datatransfer.config.DataTypeMappings
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.CompletableFuture
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs


class RedisDocumentCollectionTest {
    @Test
    fun `readDocuments() reconstructs rows and restores field types via scan`() {
        val firstPage = mockk<KeyScanCursor<String>> {
            every { keys } returns listOf("users:1")
            every { isFinished } returns true
        }
        val commands = mockk<RedisAsyncCommands<String, String>> {
            every { scan(any<ScanArgs>()) } returns completedRedisFuture(firstPage)
            every { hgetall("users:1") } returns completedRedisFuture(
                linkedMapOf(
                    "name" to "Alice",
                    "active" to "true",
                    "created_at" to "2024-01-01T10:15:30Z",
                    "payload" to "YWJj"
                )
            )
        }

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
            commands,
            dataTypeMappings = testDataTypeMappings
        )

        val documents = runBlocking { cut.readDocuments().toList() }

        assertEquals(1, documents.size)
        assertEquals(1, documents.single()["id"])
        assertEquals("Alice", documents.single()["name"])
        assertEquals(true, documents.single()["active"])
        assertEquals(Timestamp.from(java.time.Instant.parse("2024-01-01T10:15:30Z")), documents.single()["created_at"])
        assertContentEquals("abc".toByteArray(), documents.single()["payload"] as ByteArray)
    }

    @Test
    fun `readDocuments() follows scan cursor across multiple pages`() {
        val firstPage = mockk<KeyScanCursor<String>> {
            every { keys } returns listOf("users:1")
            every { isFinished } returns false
        }
        val secondPage = mockk<KeyScanCursor<String>> {
            every { keys } returns listOf("users:2")
            every { isFinished } returns true
        }
        val commands = mockk<RedisAsyncCommands<String, String>> {
            every { scan(any<ScanArgs>()) } returns completedRedisFuture(firstPage)
            every { scan(firstPage, any<ScanArgs>()) } returns completedRedisFuture(secondPage)
            every { hgetall("users:1") } returns completedRedisFuture(linkedMapOf("name" to "Alice"))
            every { hgetall("users:2") } returns completedRedisFuture(linkedMapOf("name" to "Bob"))
        }

        val cut = RedisDocumentCollection(
            RedisTableMapping(
                table = "users",
                fields = listOf(
                    FieldDefinition("id", "integer"),
                    FieldDefinition("name", "text")
                ),
                hashes = listOf(RedisHashMapping(key = "users:\${id}"))
            ),
            commands,
            dataTypeMappings = testDataTypeMappings
        )

        val documents = runBlocking { cut.readDocuments().toList() }

        assertEquals(2, documents.size)
        assertEquals(listOf(1, 2), documents.map { it["id"] })
        assertEquals(listOf("Alice", "Bob"), documents.map { it["name"] })
    }

    @Test
    fun `readDocuments() resolves additional hashes for the current row only`() {

        val firstPage = mockk<KeyScanCursor<String>> {
            every { keys } returns listOf("users:1")
            every { isFinished } returns true
        }
        val commands = mockk<RedisAsyncCommands<String, String>> {
            every { scan(any<ScanArgs>()) } returns completedRedisFuture(firstPage)
            every { hgetall("users:1") } returns completedRedisFuture(linkedMapOf("name" to "Alice"))
            every { hgetall("users:1:meta") } returns completedRedisFuture(linkedMapOf("status" to "active"))
        }

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
            commands,
            dataTypeMappings = testDataTypeMappings
        )

        val documents = runBlocking { cut.readDocuments().toList() }

        assertEquals(1, documents.size)
        assertEquals(1, documents.single()["id"])
        assertEquals("Alice", documents.single()["name"])
        assertEquals("active", documents.single()["status"])
    }

    @Test
    fun `readDocuments() converts field values using configured data type mappings`() {
        val firstPage = mockk<KeyScanCursor<String>> {
            every { keys } returns listOf("users:1")
            every { isFinished } returns true
        }
        val commands = mockk<RedisAsyncCommands<String, String>>() {
            every { scan(any<ScanArgs>()) } returns completedRedisFuture(firstPage)
            every { hgetall("users:1") } returns completedRedisFuture(linkedMapOf("enabled" to "true"))
        }

        val cut = RedisDocumentCollection(
            RedisTableMapping(
                table = "users",
                fields = listOf(
                    FieldDefinition("id", "integer"),
                    FieldDefinition("enabled", "truthy")
                ),
                hashes = listOf(RedisHashMapping(key = "users:\${id}"))
            ),
            commands,
            dataTypeMappings = DataTypeMappings(mapOf(
                "integer" to "int",
                "truthy" to "bool"
            ))
        )

        val documents = runBlocking { cut.readDocuments().toList() }

        assertEquals(1, documents.single()["id"])
        assertEquals(true, documents.single()["enabled"])
    }

    @Test
    fun `readDocuments() reconstructs hash field templates`() {
        val firstPage = mockk<KeyScanCursor<String>> {
            every { keys } returns listOf("users:1")
            every { isFinished } returns true
        }
        val commands = mockk<RedisAsyncCommands<String, String>> {
            every { scan(any<ScanArgs>()) } returns completedRedisFuture(firstPage)
            every { hgetall("users:1") } returns completedRedisFuture(linkedMapOf("name" to "Alice"))
            every { hgetall("users:1:profile") } returns completedRedisFuture(
                linkedMapOf(
                    "profile:created_on" to "2024-01-02"
                )
            )
        }

        val cut = RedisDocumentCollection(
            RedisTableMapping(
                table = "users",
                fields = listOf(
                    FieldDefinition("id", "integer"),
                    FieldDefinition("name", "text"),
                    FieldDefinition("field", "text"),
                    FieldDefinition("value", "timestamp")
                ),
                hashes = listOf(
                    RedisHashMapping(key = "users:\${id}"),
                    RedisHashMapping(
                        key = "users:\${id}:profile",
                        field = "profile:\${field}",
                        value = "\${value}"
                    )
                )
            ),
            commands,
            dataTypeMappings = testDataTypeMappings
        )

        val document = runBlocking { cut.readDocuments().toList().single() }

        assertEquals(1, document["id"])
        assertEquals("Alice", document["name"])
        assertEquals("created_on", document["field"])
        assertEquals(java.sql.Date.valueOf("2024-01-02"), document["value"])
    }

    @Test
    fun `readDocuments() converts numeric and UUID values`() {
        val uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000")
        val firstPage = mockk<KeyScanCursor<String>> {
            every { keys } returns listOf("numbers:1")
            every { isFinished } returns true
        }
        val commands = mockk<RedisAsyncCommands<String, String>> {
            every { scan(any<ScanArgs>()) } returns completedRedisFuture(firstPage)
            every { hgetall("numbers:1") } returns completedRedisFuture(
                linkedMapOf(
                    "small" to "2",
                    "large" to "1234567890123",
                    "ratio" to "1.5",
                    "amount" to "2.75",
                    "login_time" to "10:15:30",
                    "external_id" to uuid.toString()
                )
            )
        }

        val cut = RedisDocumentCollection(
            RedisTableMapping(
                table = "numbers",
                fields = listOf(
                    FieldDefinition("id", "integer"),
                    FieldDefinition("small", "smallint"),
                    FieldDefinition("large", "bigint"),
                    FieldDefinition("ratio", "real"),
                    FieldDefinition("amount", "double precision"),
                    FieldDefinition("login_time", "time"),
                    FieldDefinition("external_id", "uuid")
                ),
                hashes = listOf(RedisHashMapping(key = "numbers:\${id}"))
            ),
            commands,
            dataTypeMappings = testDataTypeMappings
        )

        val document = runBlocking { cut.readDocuments().toList().single() }

        assertIs<Short>(document["small"])
        assertEquals(2.toShort(), document["small"])
        assertEquals(1234567890123L, document["large"])
        assertEquals(1.5f, document["ratio"])
        assertEquals(2.75, document["amount"])
        assertEquals(java.sql.Time.valueOf("10:15:30"), document["login_time"])
        assertEquals(uuid, document["external_id"])
    }

    @Test
    fun `readDocuments() throws when a dependent hash key cannot be rendered`() {
        val firstPage = mockk<KeyScanCursor<String>> {
            every { keys } returns listOf("users:1")
            every { isFinished } returns true
        }
        val commands = mockk<RedisAsyncCommands<String, String>> {
            every { scan(any<ScanArgs>()) } returns completedRedisFuture(firstPage)
            every { hgetall("users:1") } returns completedRedisFuture(linkedMapOf("name" to "Alice"))
        }

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
                    RedisHashMapping(key = "users:\${missing}:status")
                )
            ),
            commands,
            dataTypeMappings = testDataTypeMappings
        )

        val exc = assertThrows<IllegalStateException> {
            runBlocking { cut.readDocuments().toList() }
        }

        assertThat(exc.message, allOf(
            containsString("users"),
            containsString("users:\${missing}:status")
        ))
    }

    @Test
    fun `readDocuments() throws when Redis returns an empty hash`() {
        val firstPage = mockk<KeyScanCursor<String>> {
            every { keys } returns listOf("users:1")
            every { isFinished } returns true
        }
        val commands = mockk<RedisAsyncCommands<String, String>> {
            every { scan(any<ScanArgs>()) } returns completedRedisFuture(firstPage)
            every { hgetall("users:1") } returns completedRedisFuture(emptyMap())
        }

        val cut = RedisDocumentCollection(
            RedisTableMapping(
                table = "users",
                fields = listOf(FieldDefinition("id", "integer")),
                hashes = listOf(RedisHashMapping(key = "users:\${id}"))
            ),
            commands,
            dataTypeMappings = testDataTypeMappings
        )

        val exc = assertThrows<IllegalArgumentException> {
            runBlocking { cut.readDocuments().toList() }
        }

        assertThat(exc.message, containsString("users:1"))
    }

    @Test
    fun `readDocuments() throws when merged Redis values conflict`() {
        val firstPage = mockk<KeyScanCursor<String>> {
            every { keys } returns listOf("users:1")
            every { isFinished } returns true
        }
        val commands = mockk<RedisAsyncCommands<String, String>> {
            every { scan(any<ScanArgs>()) } returns completedRedisFuture(firstPage)
            every { hgetall("users:1") } returns completedRedisFuture(linkedMapOf("id" to "2"))
        }

        val cut = RedisDocumentCollection(
            RedisTableMapping(
                table = "users",
                fields = listOf(FieldDefinition("id", "integer")),
                hashes = listOf(RedisHashMapping(key = "users:\${id}"))
            ),
            commands,
            dataTypeMappings = testDataTypeMappings
        )

        val exc = assertThrows<IllegalStateException> {
            runBlocking { cut.readDocuments().toList() }
        }

        assertThat(exc.message, containsString("users.id"))
    }

    @Test
    fun `readDocuments() throws when Redis mapping yields undeclared fields`() {
        val firstPage = mockk<KeyScanCursor<String>> {
            every { keys } returns listOf("users:1")
            every { isFinished } returns true
        }
        val commands = mockk<RedisAsyncCommands<String, String>> {
            every { scan(any<ScanArgs>()) } returns completedRedisFuture(firstPage)
            every { hgetall("users:1") } returns completedRedisFuture(linkedMapOf("name" to "Alice"))
        }

        val cut = RedisDocumentCollection(
            RedisTableMapping(
                table = "users",
                fields = listOf(FieldDefinition("id", "integer")),
                hashes = listOf(RedisHashMapping(key = "users:\${id}"))
            ),
            commands,
            dataTypeMappings = testDataTypeMappings
        )

        val exc = assertThrows<IllegalStateException> {
            runBlocking { cut.readDocuments().toList() }
        }

        assertThat(exc.message, allOf(
            containsString("users"),
            containsString("name")
        ))
    }

    @Test
    fun `readDocuments() throws when field type cannot be resolved to a document type`() {
        val firstPage = mockk<KeyScanCursor<String>> {
            every { keys } returns listOf("users:1")
            every { isFinished } returns true
        }
        val commands = mockk<RedisAsyncCommands<String, String>> {
            every { scan(any<ScanArgs>()) } returns completedRedisFuture(firstPage)
            every { hgetall("users:1") } returns completedRedisFuture(linkedMapOf("external_id" to "1"))
        }

        val cut = RedisDocumentCollection(
            RedisTableMapping(
                table = "users",
                fields = listOf(
                    FieldDefinition("id", "integer"),
                    FieldDefinition("external_id", "identifier")
                ),
                hashes = listOf(RedisHashMapping(key = "users:\${id}"))
            ),
            commands,
            dataTypeMappings = DataTypeMappings(mapOf("integer" to "int"))
        )

        val exc = assertThrows<IllegalStateException> {
            runBlocking { cut.readDocuments().toList() }
        }

        assertThat(exc.message, containsString("identifier"))
    }

    private fun <T> completedRedisFuture(value: T): RedisFuture<T> =
        mockk {
            every { toCompletableFuture() } returns CompletableFuture.completedFuture(value)
        }

    private companion object {
        val testDataTypeMappings = DataTypeMappings(mapOf(
            "integer" to "int",
            "smallint" to "short",
            "bigint" to "long",
            "real" to "float",
            "double precision" to "double",
            "text" to "string",
            "boolean" to "bool",
            "timestamp" to "timestamp",
            "timestamp with time zone" to "timestamp",
            "time" to "time",
            "uuid" to "uuid",
            "bytea" to "bytearray"
        ))
    }
}
