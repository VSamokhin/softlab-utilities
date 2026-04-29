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

import io.github.oshai.kotlinlogging.KotlinLogging
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.chunked
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import org.softlab.dataset.redis.RedisDatasetMapper
import org.softlab.dataset.redis.RedisMappingTemplate
import org.softlab.dataset.redis.RedisMappingsLoader
import org.softlab.dataset.redis.RedisTableMapping
import org.softlab.dataset.redis.RedisTableMappings
import org.softlab.datatransfer.core.BATCH_SIZE
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.REPORT_ON_INSERTS
import org.softlab.datatransfer.core.TransferDocument
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.plusAssign


class RedisDestination(
    uri: String,
    mappingsFile: String,
    private val client: RedisClient = RedisClient.create(uri),
    private val mappings: RedisTableMappings = RedisMappingsLoader.load(mappingsFile),
    private val closeClient: Boolean = true
) : DatabaseDestination {
    companion object {
        const val BACKEND = "redis"
        private const val KEYS_CHUNK_SIZE = 200
    }

    private val logger = KotlinLogging.logger {}
    private val connection: StatefulRedisConnection<String, String> = client.connect()
    private val commands = connection.async()

    override fun getBackendName(): String = BACKEND

    override suspend fun createCollection(metadata: CollectionMetadata) {
        RedisMappingGenerator.generate(metadata)?.run {
            logger.trace { "Generated Redis mapping:\n${RedisMappingGenerator.asYaml(this)}" }
        }
        // Basically, nothing to create, only a sanity check
        val matchedTable = mappings.table(metadata.name)
        RedisMappingValidator.validateDestinationMapping(matchedTable, metadata)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Suppress("SpreadOperator")
    override suspend fun dropCollection(collectionName: String) {
        val table = mappings.table(collectionName)
        logger.debug { "Dropping Redis keys for '$collectionName'..." }
        mapKeys(table).chunked(KEYS_CHUNK_SIZE).collect { chunk ->
            commands.del(*chunk.toTypedArray())
        }
    }

    private fun mapKeys(table: RedisTableMapping): Flow<String> {
        val keyPatterns = buildSet {
            table.hashes.forEach { add(RedisMappingTemplate.of(it.key ?: table.table).toGlob()) }
            table.sets.forEach { add(RedisMappingTemplate.of(it.key ?: table.table).toGlob()) }
        }
        return flow {
            keyPatterns.forEach { key ->
                commands
                    .keys(key)
                    .await()
                    .forEach { emit(it) }
            }
        }
    }

    @OptIn(ExperimentalAtomicApi::class, ExperimentalCoroutinesApi::class)
    override suspend fun insertDocuments(collectionName: String, documents: Flow<TransferDocument>) {
        logger.debug { "Inserting documents into '$collectionName'" }

        val total = AtomicInt(0)
        coroutineScope {
            val matchedTable = mappings.table(collectionName)
            documents.chunked(BATCH_SIZE).collect { batch ->
                total += saveBatch(matchedTable, batch)
                if (total.load() % REPORT_ON_INSERTS == 0) {
                    logger.info { "Inserted $total logical rows into '$collectionName'" }
                }
            }
        }
        logger.info { "Total of $total logical rows inserted into '$collectionName'" }
    }

    override fun close() {
        connection.close()
        if (closeClient) client.shutdown()
    }

    private fun saveBatch(table: RedisTableMapping, batch: List<TransferDocument>): Int {
        val seedData = RedisDatasetMapper.mapRows(batch, table)
        writeSeedData(seedData.hashes, seedData.sets)
        return batch.size
    }

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    @Suppress("SpreadOperator")
    private fun writeSeedData(
        hashes: Map<String, Map<String, String>>,
        sets: Map<String, Set<String>>
    ) {
        hashes.forEach { (key, entries) ->
            commands.hset(key, entries)
        }
        sets.forEach { (key, members) ->
            members.chunked(KEYS_CHUNK_SIZE).forEach { chunk ->
                commands.sadd(key, *chunk.toTypedArray())
            }
        }
    }
}
