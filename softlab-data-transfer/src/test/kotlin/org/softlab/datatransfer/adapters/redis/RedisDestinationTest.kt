package org.softlab.datatransfer.adapters.redis

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisFuture
import io.lettuce.core.ScanArgs
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.redis.RedisHashMapping
import org.softlab.dataset.redis.RedisSetMapping
import org.softlab.dataset.redis.RedisTableMapping
import org.softlab.dataset.redis.RedisTableMappings
import org.softlab.datatransfer.core.CollectionMetadata
import java.util.concurrent.CompletableFuture


class RedisDestinationTest {
    @Test
    fun `createCollection() throws when mapping refers to unknown field`() {
        val commands = mockk<RedisAsyncCommands<String, String>>(relaxed = true)
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.async() } returns commands
        val client = mockk<RedisClient>(relaxed = true)
        every { client.connect() } returns connection

        val cut = RedisDestination(
            uri = "redis://localhost:6379/0",
            mappingsFile = "unused",
            client = client,
            mappings = RedisTableMappings(
                tables = listOf(
                    RedisTableMapping(
                        table = "users",
                        hashes = listOf(RedisHashMapping(key = "users:\${missing}"))
                    )
                )
            )
        )

        val exc = assertThrows<IllegalStateException> {
            runBlocking {
                cut.createCollection(
                    CollectionMetadata(
                        "users", listOf(FieldDefinition("id", "integer"))
                    )
                )
            }
        }
        assertThat(exc.message, allOf(
            containsString("'users'"),
            containsString("missing")
        ))
    }

    @Test
    fun `dropCollection() deletes all mapped keys`() {
        val commands = mockk<RedisAsyncCommands<String, String>>(relaxed = true)
        val hashPage = scanCursor(listOf("users:1", "users:2"), finished = true)
        val setPage = scanCursor(listOf("users"), finished = true)
        every { commands.scan(any<ScanArgs>()) } returnsMany listOf(
            completedRedisFuture(hashPage),
            completedRedisFuture(setPage)
        )
        every { commands.del("users:1", "users:2") } returns completedRedisFuture(2L)
        every { commands.del("users") } returns completedRedisFuture(1L)
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.async() } returns commands
        val client = mockk<RedisClient>(relaxed = true)
        every { client.connect() } returns connection

        val cut = RedisDestination(
            uri = "redis://localhost:6379/0",
            mappingsFile = "unused",
            client = client,
            mappings = RedisTableMappings(
                tables = listOf(
                    RedisTableMapping(
                        table = "users",
                        hashes = listOf(RedisHashMapping(key = "users:\${id}")),
                        sets = listOf(RedisSetMapping(key = "users"))
                    )
                )
            )
        )

        runBlocking { cut.dropCollection("users") }

        verify(exactly = 1) { commands.del("users:1", "users:2") }
        verify(exactly = 1) { commands.del("users") }
    }

    @Test
    fun `insertDocuments() writes hashes and sets from rows`() {
        val commands = mockk<RedisAsyncCommands<String, String>>(relaxed = true)
        every { commands.hset("users:1", mapOf("id" to "1", "name" to "Alice")) } returns completedRedisFuture(2L)
        every { commands.sadd("users", "1") } returns completedRedisFuture(1L)
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.async() } returns commands
        val client = mockk<RedisClient>(relaxed = true)
        every { client.connect() } returns connection

        val cut = RedisDestination(
            uri = "redis://localhost:6379/0",
            mappingsFile = "unused",
            client = client,
            mappings = RedisTableMappings(
                tables = listOf(
                    RedisTableMapping(
                        table = "users",
                        hashes = listOf(RedisHashMapping(key = "users:\${id}")),
                        sets = listOf(RedisSetMapping(key = "users", member = "\${id}"))
                    )
                )
            )
        )

        runBlocking {
            cut.insertDocuments(
                "users",
                flowOf(mapOf("id" to 1, "name" to "Alice"))
            )
        }

        verify(exactly = 1) {
            commands.hset("users:1", mapOf("id" to "1", "name" to "Alice"))
        }
        verify(exactly = 1) { commands.sadd("users", "1") }
    }

    @Test
    fun `insertDocuments() detects duplicate hash fields across batches`() {
        val commands = mockk<RedisAsyncCommands<String, String>>(relaxed = true)
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.async() } returns commands
        val client = mockk<RedisClient>(relaxed = true)
        every { client.connect() } returns connection

        val cut = RedisDestination(
            uri = "redis://localhost:6379/0",
            mappingsFile = "unused",
            client = client,
            mappings = RedisTableMappings(
                tables = listOf(
                    RedisTableMapping(
                        table = "users",
                        hashes = listOf(RedisHashMapping(key = "users:\${id}")),
                        sets = listOf(RedisSetMapping(key = "users", member = "\${id}"))
                    )
                )
            )
        )

        val exc = assertThrows<IllegalStateException> { runBlocking {
            cut.insertDocuments(
                "users",
                flowOf(
                    mapOf("id" to 1, "name" to "Alice"),
                    mapOf("id" to 1, "name" to "Bob")
                )
            )
        } }
        assertThat(exc.message, containsString("users:1/id"))
    }

    private fun scanCursor(keys: List<String>, finished: Boolean): io.lettuce.core.KeyScanCursor<String> =
        mockk {
            every { this@mockk.keys } returns keys
            every { isFinished } returns finished
        }

    private fun <T> completedRedisFuture(value: T): RedisFuture<T> =
        mockk {
            every { toCompletableFuture() } returns CompletableFuture.completedFuture(value)
        }
}
