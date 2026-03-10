package org.softlab.dataset.redis.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test


class LettuceRedisTest {
    @Test
    fun `flushDb delegates to redis commands`() {
        val commands = mockk<RedisCommands<String, String>>(relaxed = true)
        val connection = mockk<StatefulRedisConnection<String, String>>(relaxed = true)
        every { connection.sync() } returns commands

        val cut = LettuceRedis(connection)
        cut.flushDb()

        verify(exactly = 1) { commands.flushdb() }
    }

    @Test
    fun `hashSet delegates all entries`() {
        val commands = mockk<RedisCommands<String, String>>(relaxed = true)
        val connection = mockk<StatefulRedisConnection<String, String>>(relaxed = true)
        every { connection.sync() } returns commands

        val cut = LettuceRedis(connection)
        val entries = mapOf("field1" to "value1", "field2" to "value2")

        cut.hashSet("testKey", entries)

        verify(exactly = 1) { commands.hset("testKey", entries) }
    }

    @Test
    fun `setAdd splits members into chunks and delegates`() {
        val commands = mockk<RedisCommands<String, String>>(relaxed = true)
        val connection = mockk<StatefulRedisConnection<String, String>>(relaxed = true)
        every { connection.sync() } returns commands

        val cut = LettuceRedis(connection)
        val members = (1..120).map { "member$it" }.toSet()

        cut.setAdd("testSet", members)

        verify(exactly = 3) { commands.sadd(eq("testSet"), *anyVararg()) }
    }
}
