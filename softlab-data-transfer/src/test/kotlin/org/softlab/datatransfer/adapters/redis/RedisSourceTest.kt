package org.softlab.datatransfer.adapters.redis

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.redis.RedisHashMapping
import org.softlab.dataset.redis.RedisTableMapping
import org.softlab.dataset.redis.RedisTableMappings
import kotlin.test.assertEquals


class RedisSourceTest {
    @Test
    fun `constructor throws when schema fields are missing`() {
        val commands = mockk<RedisCommands<String, String>>(relaxed = true)
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.sync() } returns commands
        val client = mockk<RedisClient>(relaxed = true)

        val exc = assertThrows<IllegalArgumentException> {
            RedisSource(
                uri = "redis://localhost:6379/0",
                mappingsFile = "unused",
                client = client,
                connection = connection,
                mappings = RedisTableMappings(
                    tables = listOf(
                        RedisTableMapping(
                            table = "users",
                            hashes = listOf(RedisHashMapping(key = "users:\${id}"))
                        )
                    )
                )
            )
        }

        assertEquals(
            "Redis source mapping for table 'users' must define fields because Redis has no schema",
            exc.message
        )
    }

    @Test
    fun `constructor throws when hash key has no placeholders`() {
        val commands = mockk<RedisCommands<String, String>>(relaxed = true)
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.sync() } returns commands
        val client = mockk<RedisClient>(relaxed = true)

        val exc = assertThrows<IllegalStateException> {
            RedisSource(
                uri = "redis://localhost:6379/0",
                mappingsFile = "unused",
                client = client,
                connection = connection,
                mappings = RedisTableMappings(
                    tables = listOf(
                        RedisTableMapping(
                            table = "users",
                            fields = listOf(FieldDefinition("name", "text")),
                            hashes = listOf(RedisHashMapping(key = "users"))
                        )
                    )
                )
            )
        }

        assertEquals(
            "Redis source mapping for table 'users' must use at least one placeholder in anchor hash key",
            exc.message
        )
    }

    @Test
    fun `constructor throws when mapping has no row-level anchor hash`() {
        val commands = mockk<RedisCommands<String, String>>(relaxed = true)
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.sync() } returns commands
        val client = mockk<RedisClient>(relaxed = true)

        val exc = assertThrows<IllegalStateException> {
            RedisSource(
                uri = "redis://localhost:6379/0",
                mappingsFile = "unused",
                client = client,
                connection = connection,
                mappings = RedisTableMappings(
                    tables = listOf(
                        RedisTableMapping(
                            table = "users",
                            fields = listOf(
                                FieldDefinition("id", "integer"),
                                FieldDefinition("status", "text")
                            ),
                            hashes = listOf(
                                RedisHashMapping(
                                    key = "users:\${id}:status",
                                    field = "status",
                                    value = "\${status}"
                                )
                            )
                        )
                    )
                )
            )
        }

        assertEquals(
            "Redis table 'users' must define a row-level anchor hash without field/value mappings",
            exc.message
        )
    }

    @Test
    fun `listCollections() returns mapped Redis collections`() {
        val commands = mockk<RedisCommands<String, String>>(relaxed = true)
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.sync() } returns commands
        val client = mockk<RedisClient>(relaxed = true)

        val cut = RedisSource(
            uri = "redis://localhost:6379/0",
            mappingsFile = "unused",
            client = client,
            connection = connection,
            mappings = RedisTableMappings(
                tables = listOf(
                    RedisTableMapping(
                        table = "users",
                        fields = listOf(FieldDefinition("id", "integer")),
                        hashes = listOf(RedisHashMapping(key = "users:\${id}"))
                    )
                )
            )
        )

        val collections = runBlocking { cut.listCollections().map { it.fetchMetadata().name }.toList() }

        assertEquals(listOf("users"), collections)
    }

    @Test
    fun `countDocuments() counts reconstructed Redis rows`() {
        val commands = mockk<RedisCommands<String, String>>()
        every { commands.keys("users:*") } returns listOf("users:1", "users:2")
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.sync() } returns commands
        val client = mockk<RedisClient>(relaxed = true)

        val cut = RedisSource(
            uri = "redis://localhost:6379/0",
            mappingsFile = "unused",
            client = client,
            connection = connection,
            mappings = RedisTableMappings(
                tables = listOf(
                    RedisTableMapping(
                        table = "users",
                        fields = listOf(
                            FieldDefinition("id", "integer"),
                            FieldDefinition("name", "text")
                        ),
                        hashes = listOf(RedisHashMapping(key = "users:\${id}"))
                    )
                )
            )
        )

        assertEquals(2L, runBlocking { cut.countDocuments("users") })
    }

    @Test
    fun `close() closes connection and client`() {
        val commands = mockk<RedisCommands<String, String>>(relaxed = true)
        val connection = mockk<StatefulRedisConnection<String, String>>(relaxed = true)
        every { connection.sync() } returns commands
        val client = mockk<RedisClient>(relaxed = true)

        val cut = RedisSource(
            uri = "redis://localhost:6379/0",
            mappingsFile = "unused",
            client = client,
            connection = connection,
            mappings = RedisTableMappings()
        )

        cut.close()

        verify(exactly = 1) { connection.close() }
        verify(exactly = 1) { client.shutdown() }
    }
}
