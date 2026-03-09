/**
 * Copyright (C) 2026, Viktor Samokhin (wowyupiyo@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.softlab.datatransfer.util

import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.time.TimeSource


class StopWatch {
    companion object {
        private const val MILLIS_IN_SECOND = 1_000L
        private const val MILLIS_IN_MINUTE = 60_000L
        private const val MILLIS_IN_HOUR = 3_600_000L

        @Suppress("MagicNumber")
        fun formatTime(milliseconds: Long): String {
            require(milliseconds >= 0) { "milliseconds must be non-negative" }

            val hours = milliseconds / MILLIS_IN_HOUR
            val minutes = (milliseconds % MILLIS_IN_HOUR) / MILLIS_IN_MINUTE
            val seconds = (milliseconds % MILLIS_IN_MINUTE) / MILLIS_IN_SECOND
            val millis = milliseconds % MILLIS_IN_SECOND

            return buildString {
                append(hours)
                append(':')
                append(minutes.toString().padStart(2, '0'))
                append(':')
                append(seconds.toString().padStart(2, '0'))
                append('.')
                append(millis.toString().padStart(3, '0'))
            }
        }
    }

    private val timeSource = TimeSource.Monotonic

    private var startedAt: TimeSource.Monotonic.ValueTimeMark? = null

    @OptIn(ExperimentalAtomicApi::class)
    private val duration = AtomicLong(0)

    fun start(): StopWatch {
        synchronized(this) {
            if (startedAt == null) {
                startedAt = timeSource.markNow()
            } else error("StopWatch already started")
        }
        return this
    }

    @OptIn(ExperimentalAtomicApi::class)
    fun stop(): Long = synchronized(this) {
        if (startedAt != null) {
            val elapsedMils = (timeSource.markNow() - startedAt!!).inWholeMilliseconds
            duration.addAndFetch(elapsedMils)
        } else error("StopWatch not started")
    }

    fun stopAndGetTimeStr(): String = formatTime(stop())

    @OptIn(ExperimentalAtomicApi::class)
    fun getStopTime(): Long = duration.load()

    @OptIn(ExperimentalAtomicApi::class)
    fun getStopTimeStr(): String = formatTime(duration.load())

    @OptIn(ExperimentalAtomicApi::class)
    fun reset() {
        synchronized(this) {
            startedAt = null
            duration.store(0)
        }
    }
}
