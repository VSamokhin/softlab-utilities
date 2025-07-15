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

package org.softlab.dataset.redis

import io.github.oshai.kotlinlogging.KotlinLogging
import org.softlab.dataset.YamlDatasetLoading
import org.softlab.dataset.YamlTablesRows
import java.util.regex.Pattern


/**
 * Map DBUnit's YAML dataset to Redis' hash or set entries using a user-provided mapping
 * This approach has obvious schema limitations and thus the mapping must be well-thought-out
 */
class RedisYamlDatasetLoader(
    private val redisHash: RedisFacade,
    private val mappingsFile: String
) : YamlDatasetLoading() {

    data class Mappings(val tables: List<Table>)

    data class Table(val table: String, val hashes: List<Hash>?, val sets: List<Set>?)

    data class Hash(val key: String?, val field: String?, val value: String?)

    data class Set(val key: String?, val member: String?)

    companion object {
        private val VALUE_PLACEHOLDER: Pattern = "\\$\\{([A-Za-z0-9@_-]+)\\}".toPattern()
    }

    override val logger = KotlinLogging.logger {}

    override fun loadImpl(dataset: YamlTablesRows, cleanBefore: Boolean) {
        logger.info { "Loading DBUnit -> Redis mappings from: $mappingsFile" }
        val mappings = readResource(mappingsFile).yamlAsObject<Mappings>()

        dataset.entries.forEach { table ->
            val matchedTable = mappings.tables.firstOrNull() { it.table == table.key }
            if (matchedTable != null) {
                mapTable(table.value, matchedTable, cleanBefore)
            } else {
                error("Could not find corresponding mapping for table: ${table.key}")
            }
        }
    }

    private fun mapTable(rows: List<Map<String, Any>>, mappings: Table, cleanBefore: Boolean) {
        // Not the most efficient way memory-wise, but this approach helps to avoid silently overwriting existing values
        // when a non-unique combination of key/field or key appears
        val hashes = linkedMapOf<String, LinkedHashMap<String, String>>()
        val sets = linkedMapOf<String, LinkedHashSet<String>>()
        // For every source record
        rows.forEach { row ->
            // And every mapped hash
            mappings.hashes?.forEach { hash ->
                seedHash(row, hash, mappings.table, hashes)
            }
            // And every mapped set
            mappings.sets?.forEach { set ->
                seedSet(row, set, mappings.table, sets)
            }
        }
        if (cleanBefore) redisHash.flushDb()
        hashes.forEach { key, entries ->
            redisHash.hashSet(key, entries)
        }
        sets.forEach { key, members ->
            redisHash.setAdd(key, members)
        }
    }

    private fun seedHash(
        row: Map<String, Any>,
        hash: Hash,
        table: String,
        results: LinkedHashMap<String, LinkedHashMap<String, String>>
    ) {
        val key = hash.key?.let { evalAll(it, row) } ?: table
        val columns = hash.value?.let { listOf(evalOne(it)) } ?: row.keys.toList() // All columns
        // Iterate through all columns
        columns.forEach { column ->
            val field = hash.field?.let { evalAll(it, row) } ?: column
            val value: Any? = row[column]
            if (value != null) {
                val previousValue =
                    results.computeIfAbsent(key) { _ -> linkedMapOf() }
                        .put(field, asString(value))
                check(previousValue == null) {
                    "Duplicate key/field found, please assure the mapping is correct: $key/$field"
                }
            }
        }
    }

    private fun seedSet(
        row: Map<String, Any>,
        set: Set,
        table: String,
        results: LinkedHashMap<String, LinkedHashSet<String>>
    ) {
        val key = set.key?.let { evalAll(it, row) } ?: table
        val columns = set.member?.let { listOf(evalOne(it)) } ?: row.keys.toList() // All columns
        // Iterate through all columns
        columns.forEach { column ->
            val member: Any? = row[column]
            if (member != null) {
                val strMember = asString(member)
                val added =
                    results.computeIfAbsent(key) { _ -> linkedSetOf() }
                        .add(strMember)
                check(added) {
                    "Duplicate key/member found, please assure the mapping is correct: $key/$strMember"
                }
            }
        }
    }

    private fun evalOne(template: String): String {
        val matcher = VALUE_PLACEHOLDER.matcher(template)
        var result = template
        if (matcher.find()) {
            result = matcher.group(1)
        }
        check(!matcher.find()) {
            "Only one placeholder is expected, but: $template"
        }
        return result
    }

    private fun evalAll(template: String, values: Map<String, Any>): String {
        val matcher = VALUE_PLACEHOLDER.matcher(template)
        val placeholders = mutableMapOf<String, String>()
        while (matcher.find()) {
            val placeholder = matcher.group(1)
            val value: Any = values[placeholder]
                ?: error("Could not find value for placeholder '$placeholder' among: $values")
            placeholders.put(placeholder, asString(value))
        }
        var result = template
        placeholders.entries.forEach { entry ->
            result = result.replace("\${${entry.key}}", entry.value)
        }
        return result
    }

    private fun asString(value: Any): String =
        value as? String ?: value.toString()
}
