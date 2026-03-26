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
import io.lettuce.core.api.sync.RedisCommands
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
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


@Suppress("TooManyFunctions")
class RedisDocumentCollection(
    private val table: RedisTableMapping,
    private val commands: RedisCommands<String, String>
) : DocumentCollection {
    private val logger = KotlinLogging.logger {}

    private val metadata: CollectionMetadata by lazy {
        CollectionMetadata(table.table, table.fields)
    }

    override suspend fun fetchMetadata(): CollectionMetadata = metadata

    override fun readDocuments(): Flow<TransferDocument> = flow {
        readRows().forEach { emit(it) }
    }

    fun countDocuments(): Long = readRows().size.toLong()

    private fun readRows(): List<TransferDocument> {
        val rawRows = linkedMapOf<String, MutableMap<String, String>>()
        table.hashes.forEach { hash -> mergeHashRows(hash, rawRows) }

        val fieldNames = metadata.fields.map { it.name }.toSet()
        return rawRows.values.map { row ->
            val unexpectedFields = row.keys - fieldNames
            check(unexpectedFields.isEmpty()) {
                "Redis source mapping for table '${table.table}' yielded fields not declared in schema: " +
                    unexpectedFields.joinToString(", ")
            }
            metadata.fields.associate { field ->
                field.name to row[field.name]?.let { convertValue(it, field) }
            }
        }.also {
            logger.debug { "Read ${it.size} logical rows from Redis table '${table.table}'" }
        }
    }

    private fun mergeHashRows(
        hash: RedisHashMapping,
        rawRows: LinkedHashMap<String, MutableMap<String, String>>
    ) {
        val keyTemplate = hash.key ?: table.table
        val keyPattern = RedisMappingTemplate.of(keyTemplate)

        commands.keys(keyPattern.toGlob())
            .sorted()
            .forEach { key -> mergeSingleHashRow(hash, keyTemplate, keyPattern, key, rawRows) }
    }

    private fun mergeSingleHashRow(
        hash: RedisHashMapping,
        keyTemplate: String,
        keyPattern: RedisMappingTemplate,
        key: String,
        rawRows: LinkedHashMap<String, MutableMap<String, String>>
    ) {
        val keyValues = checkNotNull(keyPattern.match(key)) {
            "Could not match Redis key '$key' against template '$keyTemplate'"
        }
        val row = rawRows.computeIfAbsent(rowId(keyValues)) { linkedMapOf() }
        mergeValues(row, keyValues, "key '$key'")
        mergeHashEntries(hash, key, row)
    }

    private fun mergeHashEntries(
        hash: RedisHashMapping,
        key: String,
        row: MutableMap<String, String>
    ) {
        val hashEntries = commands.hgetall(key)
        check(hashEntries.isNotEmpty()) {
            "Expected Redis hash at key '$key' for table '${table.table}'"
        }

        val valueTemplate = hash.value
        if (valueTemplate == null) {
            mergeValues(row, hashEntries, "hash '$key'")
            return
        }

        mergeValueTemplateEntries(row, key, hash, valueTemplate, hashEntries)
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
            hashEntries[valueColumn]?.let { mergeValue(row, valueColumn, it, "hash '$key'") }
            return
        }

        val fieldPattern = RedisMappingTemplate.of(fieldTemplate)
        hashEntries.forEach { (fieldName, value) ->
            val fieldValues = fieldPattern.match(fieldName) ?: return@forEach
            mergeValues(row, fieldValues, "hash field '$key/$fieldName'")
            mergeValue(row, valueColumn, value, "hash field '$key/$fieldName'")
        }
    }

    private fun rowId(values: Map<String, String>): String =
        values.entries.sortedBy { it.key }.joinToString("|") { (key, value) -> "$key=$value" }

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
}
