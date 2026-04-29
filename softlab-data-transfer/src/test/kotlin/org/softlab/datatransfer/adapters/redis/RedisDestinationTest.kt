package org.softlab.datatransfer.adapters.redis

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
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


class RedisDestinationTest {
    @Test
    fun `createCollection() throws when mapping refers to unknown field`() {
        val commands = mockk<RedisCommands<String, String>>()
        every { commands.keys(any()) } returns emptyList()
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.sync() } returns commands
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
        val commands = mockk<RedisCommands<String, String>>(relaxed = true)
        every { commands.keys("users:*") } returns listOf("users:1", "users:2")
        every { commands.keys("users") } returns listOf("users")
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.sync() } returns commands
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

        verify(exactly = 1) { commands.del("users:1", "users:2", "users") }
    }

    @Test
    fun `insertDocuments() writes hashes and sets from rows`() {
        val commands = mockk<RedisCommands<String, String>>(relaxed = true)
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.sync() } returns commands
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
        val commands = mockk<RedisCommands<String, String>>(relaxed = true)
        every { commands.hexists("users:1", "id") } returnsMany listOf(false, true)
        every { commands.hexists("users:1", "name") } returns false
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.sync() } returns commands
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
}
