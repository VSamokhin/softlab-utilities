package org.softlab.datatransfer.util

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertTrue


class StopWatchTest {
    @Test
    fun `formatTime() formats zero`() {
        assertEquals("0:00:00.000", StopWatch.formatTime(0))
    }

    @Test
    fun `formatTime() formats mixed hours minutes and milliseconds`() {
        val input = (2 * 3_600_000L) + (15 * 60_000L) + 12_345L
        assertEquals("2:15:12.345", StopWatch.formatTime(input))
    }

    @Test
    fun `formatTime() wraps minutes after one hour`() {
        assertEquals("1:01:00.000", StopWatch.formatTime(3_660_000L))
    }

    @Test
    fun `formatTime() throws for negative values`() {
        assertThrows<IllegalArgumentException> {
            StopWatch.formatTime(-1)
        }
    }

    @Test
    fun `formatTime() formats exact minute`() {
        assertEquals("0:01:00.000", StopWatch.formatTime(60_000L))
    }

    @Test
    fun `formatTime() formats exact hour`() {
        assertEquals("1:00:00.000", StopWatch.formatTime(3_600_000L))
    }

    @Test
    fun `formatTime() formats one millisecond before hour`() {
        assertEquals("0:59:59.999", StopWatch.formatTime(3_599_999L))
    }

    @Test
    fun `formatTime() formats large values`() {
        assertEquals("100:00:00.000", StopWatch.formatTime(360_000_000L))
    }

    @Test
    fun `start() throws when called twice without reset`() {
        val stopWatch = StopWatch().start()

        assertThrows<IllegalStateException> {
            stopWatch.start()
        }
    }

    @Test
    fun `stop() throws when called before start`() {
        val stopWatch = StopWatch()

        assertThrows<IllegalStateException> {
            stopWatch.stop()
        }
    }

    @Test
    fun `getStopTime() is zero initially`() {
        val stopWatch = StopWatch()
        assertEquals(0L, stopWatch.getStopTime())
    }

    @Test
    fun `start-stop updates duration and stop returns same value as getStopTime`() {
        val stopWatch = StopWatch().start()
        Thread.sleep(20)

        val stopped = stopWatch.stop()
        val stored = stopWatch.getStopTime()

        assertEquals(stopped, stored)
        assertTrue(stopped >= 10L)
    }

    @Test
    fun `stopAndGetTimeStr() returns formatted stop result`() {
        val stopWatch = StopWatch().start()
        Thread.sleep(10)

        val text = stopWatch.stopAndGetTimeStr()
        val expected = StopWatch.formatTime(stopWatch.getStopTime())

        assertEquals(expected, text)
    }

    @Test
    fun `getStopTimeStr() returns formatted stored duration`() {
        val stopWatch = StopWatch().start()
        Thread.sleep(10)
        stopWatch.stop()

        assertEquals(
            StopWatch.formatTime(stopWatch.getStopTime()),
            stopWatch.getStopTimeStr()
        )
    }

    @Test
    fun `reset() clears duration and allows starting again`() {
        val stopWatch = StopWatch().start()
        Thread.sleep(10)
        stopWatch.stop()
        stopWatch.reset()

        assertEquals(0L, stopWatch.getStopTime())
        assertEquals("0:00:00.000", stopWatch.getStopTimeStr())

        stopWatch.start()
        Thread.sleep(10)
        val secondRun = stopWatch.stop()
        assertTrue(secondRun >= 5L)
    }

    @Test
    fun `stopAndGetTimeStr() throws when stopwatch is not started`() {
        val stopWatch = StopWatch()

        assertThrows<IllegalStateException> {
            stopWatch.stopAndGetTimeStr()
        }
    }
}
