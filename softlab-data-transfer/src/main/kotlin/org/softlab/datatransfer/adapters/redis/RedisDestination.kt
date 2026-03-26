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
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.chunked
import org.softlab.dataset.redis.RedisDatasetMapper
import org.softlab.dataset.redis.RedisMappingTemplate
import org.softlab.dataset.redis.RedisMappingsLoader
import org.softlab.dataset.redis.RedisSeedData
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


@Suppress("TooManyFunctions")
class RedisDestination(
    uri: String,
    mappingsFile: String,
    private val mappings: RedisTableMappings = RedisMappingsLoader.load(mappingsFile),
    private val client: RedisClient = RedisClient.create(uri),
    private val connection: StatefulRedisConnection<String, String> = client.connect()
) : DatabaseDestination {
    companion object {
        const val BACKEND = "redis"
        private const val DELETE_CHUNK_SIZE = 200
        private const val MAX_KEYS_IN_ERROR = 5
    }

    private val logger = KotlinLogging.logger {}
    private val commands = connection.sync()

    override fun getBackendName(): String = BACKEND

    override suspend fun createCollection(metadata: CollectionMetadata) {
        val table = requireTable(metadata.name)
        RedisMappingValidator.validateDestinationMapping(table, metadata)

        val existingKeys = mappedKeys(table)
        check(existingKeys.isEmpty()) {
            "Redis keys already exist for '${metadata.name}', please drop them before proceeding: " +
                existingKeys.take(MAX_KEYS_IN_ERROR).joinToString(", ")
        }
    }

    @Suppress("SpreadOperator")
    override suspend fun dropCollection(collectionName: String) {
        val table = mappings.table(collectionName) ?: return
        val keys = mappedKeys(table)
        if (keys.isNotEmpty()) {
            logger.debug { "Dropping ${keys.size} Redis keys for '$collectionName'" }
            keys.chunked(DELETE_CHUNK_SIZE).forEach { chunk ->
                commands.del(*chunk.toTypedArray())
            }
        }
    }

    @OptIn(ExperimentalAtomicApi::class, ExperimentalCoroutinesApi::class)
    override suspend fun insertDocuments(collectionName: String, documents: Flow<TransferDocument>) {
        val table = requireTable(collectionName)
        logger.debug { "Inserting documents into '$collectionName'" }

        val total = AtomicInt(0)
        coroutineScope {
            documents.chunked(BATCH_SIZE).collect { batch ->
                total += saveBatch(table, batch)
                if (total.load() % REPORT_ON_INSERTS == 0) {
                    logger.info { "Inserted $total logical rows into '$collectionName'" }
                }
            }
        }
        logger.info { "Total of $total logical rows inserted into '$collectionName'" }
    }

    override fun close() {
        connection.close()
        client.shutdown()
    }

    private fun requireTable(collectionName: String): RedisTableMapping =
        RedisMappingValidator.requireTable(mappings, collectionName)

    private fun mappedKeys(table: RedisTableMapping): Set<String> {
        val keyPatterns = buildSet {
            table.hashes.forEach { add(RedisMappingTemplate.of(it.key ?: table.table).toGlob()) }
            table.sets.forEach { add(RedisMappingTemplate.of(it.key ?: table.table).toGlob()) }
        }
        return keyPatterns.flatMapTo(linkedSetOf()) { commands.keys(it) }
    }

    private fun saveBatch(
        table: RedisTableMapping,
        batch: List<TransferDocument>
    ): Int {
        val rows = batch.map { it.withoutNullValues() }
        val seedData = RedisDatasetMapper.mapRows(rows, table)
        ensureNoExistingSeedData(seedData)
        writeSeedData(seedData)
        return rows.size
    }

    private fun ensureNoExistingSeedData(seedData: RedisSeedData) {
        seedData.hashes.forEach { (key, entries) ->
            entries.keys.forEach { field ->
                check(!commands.hexists(key, field)) {
                    "Duplicate field found in hash, please assure the mapping is correct: $key/$field"
                }
            }
        }
        seedData.sets.forEach { (key, members) ->
            members.forEach { member ->
                check(!commands.sismember(key, member)) {
                    "Duplicate member found in set, please assure the mapping is correct: $key/$member"
                }
            }
        }
    }

    @Suppress("SpreadOperator")
    private fun writeSeedData(seedData: RedisSeedData) {
        seedData.hashes.forEach { (key, entries) ->
            commands.hset(key, entries)
        }
        seedData.sets.forEach { (key, members) ->
            members.chunked(DELETE_CHUNK_SIZE).forEach { chunk ->
                commands.sadd(key, *chunk.toTypedArray())
            }
        }
    }

    private fun TransferDocument.withoutNullValues(): Map<String, Any> =
        buildMap {
            this@withoutNullValues.forEach { (key, value) ->
                if (value != null) {
                    put(key, value)
                }
            }
        }
}
