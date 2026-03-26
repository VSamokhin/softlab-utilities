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

package org.softlab.datatransfer.adapters.redis

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.softlab.dataset.redis.RedisMappingsLoader
import org.softlab.dataset.redis.RedisTableMappings
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.core.DocumentCollection


class RedisSource(
    uri: String,
    mappingsFile: String,
    private val client: RedisClient = RedisClient.create(uri),
    private val connection: StatefulRedisConnection<String, String> = client.connect(),
    private val mappings: RedisTableMappings = RedisMappingsLoader.load(mappingsFile)
) : DatabaseSource {
    companion object {
        const val BACKEND = "redis"
    }

    private val commands = connection.sync()

    init {
        RedisMappingValidator.validateSourceMappings(mappings)
    }

    override fun getBackendName(): String = BACKEND

    override fun listCollections(): Flow<DocumentCollection> = flow {
        mappings.tables.forEach { table ->
            emit(RedisDocumentCollection(table, commands))
        }
    }

    override suspend fun countDocuments(collectionName: String): Long =
        RedisDocumentCollection(
            RedisMappingValidator.requireTable(mappings, collectionName),
            commands
        ).countDocuments()

    override fun close() {
        connection.close()
        client.shutdown()
    }
}
