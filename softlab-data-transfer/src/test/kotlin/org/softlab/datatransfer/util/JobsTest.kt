package org.softlab.datatransfer.util

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyOrder
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.LinkedList
import kotlin.test.assertContentEquals


class JobsTest {
    @ParameterizedTest
    @ValueSource(ints = [0, -1])
    fun `reduce() rejects non-positive limits`(limit: Int) {
        val exc = assertThrows<IllegalArgumentException> {
            LinkedList<Job>().reduce(limit)
        }
        assertThat(exc.message, containsString("> 0"))
    }

    @Test
    fun `reduce() removes completed jobs`() {
        val firstActive = CompletableDeferred<Unit>()
        val completed = CompletableDeferred(Unit)
        val secondActive = CompletableDeferred<Unit>()
        val jobs = LinkedList<Job>(listOf(firstActive, completed, secondActive))

        jobs.reduce(2)

        assertContentEquals(listOf(firstActive, secondActive), jobs)
    }

    @Test
    fun `reduce() joins and removes oldest jobs until limit is reached`() {
        val first = mockk<Job>()
        val second = mockk<Job>()
        val third = mockk<Job>()
        val fourth = mockk<Job>()
        val jobs = LinkedList(listOf(first, second, third, fourth))
        jobs.forEach {
            every { it.isCompleted } returns false
            coEvery { it.join() } returns Unit
        }

       jobs.reduce(2)

        assertContentEquals(listOf(third, fourth), jobs)
        coVerifyOrder {
            first.join()
            second.join()
        }
        coVerify(exactly = 0) {
            third.join()
            fourth.join()
        }
    }
}
