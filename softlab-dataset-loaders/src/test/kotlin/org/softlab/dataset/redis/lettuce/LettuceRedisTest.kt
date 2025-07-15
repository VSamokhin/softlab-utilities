package org.softlab.dataset.redis.lettuce

import com.redis.testcontainers.RedisContainer
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.utility.DockerImageName
import java.lang.Thread.sleep


class LettuceRedisTest {
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
    fun `hashSet() and deleteAllKeys should work correctly`() {
        val cut = LettuceRedis(redisConnection)

        val key = "testKey"
        val entries = mapOf("field1" to "value1", "field2" to "value2")
        cut.hashSet(key, entries)

        val actual = redisConnection.sync().hgetall(key)
        assertEquals(entries.size, actual.size)
        assertThat(actual.keys, containsInAnyOrder(*entries.keys.toTypedArray()))
        assertThat(actual.values, containsInAnyOrder(*entries.values.toTypedArray()))
    }

    @Test
    fun `deleteAllKeys() should work correctly`() {
        val cut = LettuceRedis(redisConnection)

        val commands = redisConnection.sync()

        val key = "testKey"
        val entries = mapOf("field1" to "value1", "field2" to "value2")
        commands.hset(key, entries)

        assertTrue(commands.hgetall(key).isNotEmpty())

        cut.flushDb()

        assertTrue(commands.hgetall(key).isEmpty())
    }
}

