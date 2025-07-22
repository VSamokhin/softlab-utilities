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
    fun `hashSet() should set fields and values to the hash`() {
        val cut = LettuceRedis(redisConnection)

        val key = "testKey"
        val expectedEntries = mapOf("field1" to "value1", "field2" to "value2")

        cut.hashSet(key, expectedEntries)

        val actual = redisConnection.sync().hgetall(key)
        assertEquals(expectedEntries.size, actual.size)
        assertThat(
            actual.keys,
            containsInAnyOrder(*expectedEntries.keys.toTypedArray())
        )
        assertThat(
            actual.values,
            containsInAnyOrder(*expectedEntries.values.toTypedArray())
        )
    }

    @Test
    fun `flushDb() should clean up all the data`() {
        val cut = LettuceRedis(redisConnection)

        val commands = redisConnection.sync()

        val hashKey = "hashKey"
        commands.hset(hashKey, mapOf("field1" to "value1", "field2" to "value2"))
        assertTrue(commands.hgetall(hashKey).isNotEmpty())
        val setKey = "setKey"
        commands.sadd(setKey, "member1", "member2", "member3")
        assertTrue(commands.smembers(setKey).isNotEmpty())

        cut.flushDb()

        assertTrue(commands.hgetall(hashKey).isEmpty())
        assertTrue(commands.smembers(setKey).isEmpty())
    }

    @Test
    fun `setAdd() should add all members to the set correctly`() {
        val cut = LettuceRedis(redisConnection)

        val key = "testSetKey"
        val expectedMembers = setOf("member1", "member2", "member3")

        cut.setAdd(key, expectedMembers)

        val actualMembers = redisConnection.sync().smembers(key)
        assertEquals(
            expectedMembers.size,
            actualMembers.size
        )
        assertThat(
            actualMembers,
            containsInAnyOrder(*expectedMembers.toTypedArray())
        )
    }
}
