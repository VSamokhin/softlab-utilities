package org.softlab.dataset.redis

import com.redis.testcontainers.RedisContainer
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertIterableEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.softlab.dataset.redis.lettuce.LettuceRedis
import org.testcontainers.utility.DockerImageName
import java.lang.Thread.sleep


class RedisYamlDatasetLoaderTest {
    companion object {
        private val redisContainer: RedisContainer = RedisContainer(DockerImageName.parse("redis:latest"))

        private lateinit var redisClient: RedisClient
        private lateinit var redisConnection: StatefulRedisConnection<String, String>

        @BeforeAll
        @JvmStatic
        fun setup() {
            redisContainer.start()
            // Workaround for Rancher Desktop on Mac, somehow the container is not ready while the tests start
            val isMac = System.getProperty("os.name").contains("Mac", ignoreCase = true)
            if (isMac) sleep(3000) // Wait for the container to be fully ready

            redisClient = RedisClient.create(redisContainer.redisURI)
            redisConnection = redisClient.connect()
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            redisConnection.close()
            redisClient.shutdown()
            redisContainer.stop()
        }
    }

    @Test
    fun `load() should map dataset into redis set and hashes`() {
        val cut = RedisYamlDatasetLoader(
            LettuceRedis(redisConnection),
            "mappings/dbunit-to-redis-mapping.yml"
        )
        cut.load("datasets/test-dataset.yml", cleanBefore = true)

        val commands = redisConnection.sync()
        val mainSet = commands.smembers("test.test_table")
        assertIterableEquals(listOf("1", "2"), mainSet.toList().sorted())

        // Validate hash: test.test_table:1
        val row1 = commands.hgetall("test.test_table:1")
        assertEquals("1", row1["int_column_pk"])
        assertEquals("123.456789", row1["double_column"])
        assertEquals("Alice", row1["text_column"])
        assertEquals("true", row1["bool_column"])
        assertEquals("2023-10-01T12:00:00Z", row1["timestamp_column"])
        assertEquals("1234567890123456789", row1["long_column"])
        // bin_column is base64 decoded, so check its value
        assertEquals(
            "a small brown fox jumps over the lazy dog",
            org.dbunit.util.Base64.decodeToString(row1["bin_column"])
        )

        // Validate hash: test.test_table:2
        val row2 = commands.hgetall("test.test_table:2")
        assertEquals("2", row2["int_column_pk"])
        assertEquals("Bob", row2["text_column"])
        assertEquals("true", row2["bool_column"])
    }
}

