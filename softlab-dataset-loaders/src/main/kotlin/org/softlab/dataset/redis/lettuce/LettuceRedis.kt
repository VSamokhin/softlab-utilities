/**
 * Copyright (C) 2023-2025, Viktor Samokhin (wowyupiyo@gmail.com)
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

package org.softlab.dataset.redis.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import org.softlab.dataset.redis.RedisFacade


class LettuceRedis(connection: StatefulRedisConnection<String, String>) : RedisFacade {
    companion object {
        private const val CHUNK_SIZE: Int = 50
    }

    private val syncCommands = connection.sync()

    override fun flushDb() {
        syncCommands.flushdb()
    }

    override fun hashSet(key: String, entries: Map<String, String>) {
        syncCommands.hset(key, entries)
    }

    @Suppress("SpreadOperator")
    override fun setAdd(key: String, members: Set<String>) {
        members.chunked(CHUNK_SIZE).forEach { chunk ->
            syncCommands.sadd(key, *chunk.toTypedArray())
        }
    }
}
