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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.softlab.dataset.redis.RedisMappingTemplate
import org.softlab.dataset.redis.RedisMappingsLoader
import org.softlab.dataset.redis.RedisTableMappings
import org.softlab.datatransfer.config.DataTypeMappingsConfig
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.core.DocumentCollection


class RedisSource(
    private val client: RedisClient,
    private val tableMappings: RedisTableMappings,
    private val dataTypeMappings: Map<String, String>,
    private val closeClient: Boolean
) : DatabaseSource {
    constructor(
        uri: String,
        tableMappingsFile: String,
        typeMappingsConfig: DataTypeMappingsConfig
    ) : this(
        RedisClient.create(uri),
        RedisMappingsLoader.load(tableMappingsFile),
        typeMappingsConfig.source(BACKEND),
        true
    )
    companion object {
        private const val BACKEND = "redis"
    }

    private val connection = client.connect()
    private val commands = connection.async()

    init {
        RedisMappingValidator.validateSourceMappings(tableMappings)
    }

    override fun getBackendName(): String = BACKEND

    override fun listCollections(): Flow<DocumentCollection> = flow {
        tableMappings.tables.forEach { table ->
            emit(RedisDocumentCollection(table, commands, dataTypeMappings))
        }
    }

    /**
     * This is a heavy call, avoid using it in the production
     */
    override suspend fun countDocuments(collectionName: String): Long {
        var count = 0L
        val table = tableMappings.table(collectionName)
        commands.scanKeys(RedisMappingTemplate.of(table).toGlob()).collect {  keys ->
            count += keys.size
        }
        return count
    }

    override fun close() {
        connection.close()
        if (closeClient) client.shutdown()
    }
}
