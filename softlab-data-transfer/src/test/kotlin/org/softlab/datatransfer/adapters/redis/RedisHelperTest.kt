package org.softlab.datatransfer.adapters.redis

import io.lettuce.core.KeyScanCursor
import io.lettuce.core.RedisFuture
import io.lettuce.core.ScanArgs
import io.lettuce.core.api.async.RedisAsyncCommands
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import kotlin.test.assertContentEquals


class RedisHelperTest {
    @Test
    fun `scanKeys() emits keys from a finished first page`() {
        val commands = mockk<RedisAsyncCommands<String, String>>()
        val page = scanCursor(listOf("users:1", "users:2"), finished = true)
        every { commands.scan(any<ScanArgs>()) } returns completedRedisFuture(page)

        val keys = runBlocking { commands.scanKeys("users:*").toList() }

        assertContentEquals(listOf(listOf("users:1", "users:2")), keys)
        verify(exactly = 1) { commands.scan(any<ScanArgs>()) }
    }

    @Test
    fun `scanKeys() emits the final page after following the cursor`() {
        val commands = mockk<RedisAsyncCommands<String, String>>()
        val firstPage = scanCursor(listOf("users:1"), finished = false)
        val finalPage = scanCursor(listOf("users:2", "users:3"), finished = true)
        every { commands.scan(any<ScanArgs>()) } returns completedRedisFuture(firstPage)
        every { commands.scan(firstPage, any<ScanArgs>()) } returns completedRedisFuture(finalPage)

        val keys = runBlocking { commands.scanKeys("users:*").toList() }

        assertContentEquals(
            listOf(
                listOf("users:1"),
                listOf("users:2", "users:3")
            ),
            keys
        )
        verify(exactly = 1) { commands.scan(any<ScanArgs>()) }
        verify(exactly = 1) { commands.scan(firstPage, any<ScanArgs>()) }
    }

    @Test
    fun `scanKeys() skips empty pages and continues scanning`() {
        val commands = mockk<RedisAsyncCommands<String, String>>()
        val emptyPage = scanCursor(emptyList(), finished = false)
        val finalPage = scanCursor(listOf("users:1"), finished = true)
        every { commands.scan(any<ScanArgs>()) } returns completedRedisFuture(emptyPage)
        every { commands.scan(emptyPage, any<ScanArgs>()) } returns completedRedisFuture(finalPage)

        val keys = runBlocking { commands.scanKeys("users:*").toList() }

        assertContentEquals(listOf(listOf("users:1")), keys)
    }

    private fun scanCursor(keys: List<String>, finished: Boolean): KeyScanCursor<String> =
        mockk {
            every { this@mockk.keys } returns keys
            every { isFinished } returns finished
        }

    private fun <T> completedRedisFuture(value: T): RedisFuture<T> =
        mockk {
            every { toCompletableFuture() } returns CompletableFuture.completedFuture(value)
        }
}
