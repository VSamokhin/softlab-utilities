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

import io.lettuce.core.KeyScanCursor
import io.lettuce.core.RedisFuture
import io.lettuce.core.api.async.RedisAsyncCommands
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.redis.RedisHashMapping
import org.softlab.dataset.redis.RedisMappingTemplate
import org.softlab.dataset.redis.RedisTableMapping
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DocumentCollection
import org.softlab.datatransfer.core.TransferDocument
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import java.util.Base64
import java.util.UUID
import kotlin.uuid.ExperimentalUuidApi


class RedisDocumentCollection(
    private val table: RedisTableMapping,
    private val commands: RedisAsyncCommands<String, String>
) : DocumentCollection {
    private val metadata: CollectionMetadata by lazy {
        CollectionMetadata(table.table, table.fields)
    }

    private val fieldNames: Set<String> by lazy {
        metadata.fields.mapTo(linkedSetOf()) { it.name }
    }

    override suspend fun fetchMetadata(): CollectionMetadata = metadata

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun readDocuments(): Flow<TransferDocument> =
        commands.scanKeys(RedisMappingTemplate.of(table).toGlob()).flatMapMerge { keys ->
            val futures = mutableListOf<Pair<String, RedisFuture<Map<String, String>>>>()
            for (key in keys) {
                futures += key to commands.hgetall(key)
            }
            flow {
                for ((key, future) in futures) {
                    val entries = future.await()
                    emit(readRow(key, entries))
                }
            }
        }

    private suspend fun readRow(
        anchorKey: String,
        anchorEntries: Map<String, String>
    ): TransferDocument {
        val row = linkedMapOf<String, String>()
        val anchorHash = table.anchorHash()
        mergeHashAtKey(anchorHash, anchorKey, anchorEntries, row)

        val remaining = table.hashes.filterNot { it == anchorHash }.toMutableList()
        while (remaining.isNotEmpty()) {
            val futures = mutableListOf<Triple<RedisHashMapping, String, RedisFuture<Map<String, String>>>>()
            val iterator = remaining.iterator()
            while (iterator.hasNext()) {
                val hash = iterator.next()
                val key = renderHashKey(hash, row) ?: continue
                iterator.remove()
                futures += Triple(hash, key, commands.hgetall(key))
            }
            check(futures.isNotEmpty()) {
                val unresolved = remaining.joinToString(", ") { it.key ?: table.table }
                "Redis source mapping for table '${table.table}' cannot resolve hash keys for: $unresolved"
            }
            for ((hash, key, future) in futures) {
                val entries = future.await()
                mergeHashAtKey(hash, key, entries, row)
            }
        }
        return toDocument(row)
    }

    private fun renderHashKey(
        hash: RedisHashMapping,
        row: Map<String, String>
    ): String? {
        val keyTemplate = RedisMappingTemplate.of(hash.key ?: table.table)
        return if (keyTemplate.placeholders().all { it in row }) {
            keyTemplate.render(row)
        } else {
            null
        }
    }

    private fun mergeHashAtKey(
        hash: RedisHashMapping,
        key: String,
        hashEntries: Map<String, String>,
        row: MutableMap<String, String>
    ) {
        require(hashEntries.isNotEmpty()) {
            "Expected Redis hash at key '$key' for table '${table.table}'"
        }

        val keyTemplate = hash.key ?: table.table
        val keyPattern = RedisMappingTemplate.of(keyTemplate)
        val keyValues = checkNotNull(keyPattern.match(key)) {
            "Could not match Redis key '$key' against template '$keyTemplate'"
        }
        mergeValues(row, keyValues, "key '$key'")

        val valueTemplate = hash.value
        if (valueTemplate == null) {
            mergeValues(row, hashEntries, "hash '$key'")
        } else {
            mergeValueTemplateEntries(row, key, hash, valueTemplate, hashEntries)
        }
    }

    private fun mergeValueTemplateEntries(
        row: MutableMap<String, String>,
        key: String,
        hash: RedisHashMapping,
        valueTemplate: String,
        hashEntries: Map<String, String>
    ) {
        val valueColumn = RedisMappingTemplate.exactPlaceholder(valueTemplate)
        val fieldTemplate = hash.field
        if (fieldTemplate == null) {
            hashEntries[valueColumn]?.let {
                mergeValue(row, valueColumn, it, "hash '$key'")
            }
        } else {
            val fieldPattern = RedisMappingTemplate.of(fieldTemplate)
            hashEntries.forEach { (fieldName, value) ->
                val fieldValues = fieldPattern.match(fieldName) ?: return@forEach
                mergeValues(row, fieldValues, "hash field '$key/$fieldName'")
                mergeValue(row, valueColumn, value, "hash field '$key/$fieldName'")
            }
        }
    }

    private fun mergeValues(target: MutableMap<String, String>, source: Map<String, String>, sourceName: String) {
        source.forEach { (field, value) ->
            mergeValue(target, field, value, sourceName)
        }
    }

    private fun mergeValue(target: MutableMap<String, String>, field: String, value: String, sourceName: String) {
        val previous = target.putIfAbsent(field, value)
        check(previous == null || previous == value) {
            "Conflicting Redis values found for '${table.table}.$field' while reading $sourceName"
        }
    }

    private fun toDocument(row: Map<String, String>): TransferDocument {
        val unexpectedFields = row.keys - fieldNames
        check(unexpectedFields.isEmpty()) {
            "Redis source mapping for table '${table.table}' yielded fields not declared in schema: " +
                unexpectedFields.joinToString(", ")
        }
        return metadata.fields.associate { field ->
            field.name to row[field.name]?.let { convertValue(it, field) }
        }
    }

    @OptIn(ExperimentalUuidApi::class)
    private fun convertValue(value: String, field: FieldDefinition): Any =
        when (field.type.lowercase()) {
            "smallint", "short", "int16" -> value.toShort()
            "integer", "int", "int32" -> value.toInt()
            "bigint", "long", "int64", "timestamp" -> value.toLong()
            "real", "float" -> value.toFloat()
            "double", "double precision", "decimal", "decimal128" -> value.toDouble()
            "boolean", "bool" -> value.toBooleanStrict()
            "timestamp with time zone", "timestamp without time zone",
            "timestamp with timezone", "timestamptz", "date" -> timestampValue(value)
            "time", "time with time zone", "time without time zone" -> timeValue(value)
            "bytea", "blob", "bindata" -> Base64.getDecoder().decode(value)
            "uuid" -> UUID.fromString(value)
            else -> value
        }

    private fun timestampValue(value: String): Any =
        runCatching { Timestamp.from(Instant.parse(value)) }
            .recoverCatching { Timestamp.from(OffsetDateTime.parse(value).toInstant()) }
            .recoverCatching { Timestamp.from(ZonedDateTime.parse(value).toInstant()) }
            .recoverCatching { Timestamp.valueOf(value) }
            .recoverCatching { Date.valueOf(LocalDate.parse(value)) }
            .getOrThrow()

    private fun timeValue(value: String): Any =
        runCatching { Time.valueOf(value) }
            .recoverCatching { Time.valueOf(LocalTime.parse(value)) }
            .getOrThrow()

    private fun KeyScanCursor<String>.keys(): List<String> = keys
}
