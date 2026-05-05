/**
 * Copyright (C) 2023-2026, Viktor Samokhin (wowyupiyo@gmail.com)
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

package org.softlab.dataset.redis

import java.util.Base64


data class RedisSeedData(
    val hashes: Map<String, Map<String, String>>,
    val sets: Map<String, Set<String>>
)

object RedisDatasetMapper {
    fun mapRows(rows: List<Map<String, Any?>>, mappings: RedisTableMapping): RedisSeedData {
        // Not the most efficient way memory-wise, but this approach helps to avoid silently overwriting existing values
        // when a non-unique combination of key-field in hash or member in set appears
        val hashes = linkedMapOf<String, LinkedHashMap<String, String>>()
        val sets = linkedMapOf<String, LinkedHashSet<String>>()
        // For every source record
        rows.forEach { row ->
            // And every mapped hash
            mappings.hashes.forEach { hash ->
                seedHash(row, hash, mappings.table, hashes)
            }
            // And every mapped set
            mappings.sets.forEach { set ->
                seedSet(row, set, mappings.table, sets)
            }
        }
        return RedisSeedData(hashes, sets)
    }

    private fun asString(value: Any): String =
        when (value) {
            is String -> value
            is ByteArray -> Base64.getEncoder().encodeToString(value)
            else -> value.toString()
        }

    private fun seedHash(
        row: Map<String, Any?>,
        hash: RedisHashMapping,
        table: String,
        results: LinkedHashMap<String, LinkedHashMap<String, String>>
    ) {
        val rowStrings = row.mapNotNullValues { value -> asString(value) }
        val key = hash.key?.let {
            RedisMappingTemplate.of(it).render(rowStrings)
        } ?: table
        val field = hash.field?.let {
            RedisMappingTemplate.of(it).render(rowStrings)
        }
        val columns = hash.value?.let {
            listOf(RedisMappingTemplate.exactPlaceholder(it))
        } ?: rowStrings.keys.toList()  // Or all columns otherwise
        columns.forEach { column ->
            val fieldName = field ?: column
            val value: String? = rowStrings[column]
            if (value != null) {
                val previousValue =
                    results.computeIfAbsent(key) { linkedMapOf() }
                        .put(fieldName, value)
                check(previousValue == null) {
                    "Duplicate field found in hash, please assure the mapping is correct: $key/$fieldName"
                }
            }
        }
    }

    private fun seedSet(
        row: Map<String, Any?>,
        set: RedisSetMapping,
        table: String,
        results: LinkedHashMap<String, LinkedHashSet<String>>
    ) {
        val rowStrings = row.mapNotNullValues { value -> asString(value) }
        val key = set.key?.let {
            RedisMappingTemplate.of(it).render(rowStrings)
        } ?: table
        val columns = set.member?.let {
            listOf(RedisMappingTemplate.exactPlaceholder(it))
        } ?: rowStrings.keys.toList() // Or all columns otherwise
        columns.forEach { column ->
            val member: String? = rowStrings[column]
            if (member != null) {
                val added =
                    results.computeIfAbsent(key) { linkedSetOf() }
                        .add(member)
                check(added) {
                    "Duplicate member found in set, please assure the mapping is correct: $key/$member"
                }
            }
        }
    }

    private inline fun <K, V : Any, R : Any> Map<K, V?>.mapNotNullValues(transform: (V) -> R): Map<K, R> =
        buildMap {
            this@mapNotNullValues.forEach { (key, value) ->
                if (value != null) {
                    put(key, transform(value))
                }
            }
        }
}
