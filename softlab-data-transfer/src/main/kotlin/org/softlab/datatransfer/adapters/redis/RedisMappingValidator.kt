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

import org.softlab.dataset.redis.RedisMappingTemplate
import org.softlab.dataset.redis.RedisTableMapping
import org.softlab.dataset.redis.RedisTableMappings
import org.softlab.datatransfer.core.CollectionMetadata


object RedisMappingValidator {
    fun validateSourceMappings(mappings: RedisTableMappings) {
        mappings.tables.forEach(::validateSourceMapping)
    }

    fun validateSourceMapping(table: RedisTableMapping) {
        require(table.fields.isNotEmpty()) {
            "Redis source mapping for table '${table.table}' must define fields because Redis has no schema"
        }
        require(table.hashes.isNotEmpty()) {
            "Redis source mapping for table '${table.table}' must define at least one hash mapping"
        }
        table.hashes.forEach { hash ->
            val keyTemplate = RedisMappingTemplate.of(hash.key ?: table.table)
            check(keyTemplate.placeholders().isNotEmpty()) {
                "Redis source mapping for table '${table.table}' must use at least one placeholder in hash.key"
            }
            check(hash.value != null || hash.field == null) {
                "Redis source mapping for table '${table.table}' " +
                    "cannot reconstruct rows from hash.field without hash.value"
            }
        }
    }

    fun validateDestinationMapping(table: RedisTableMapping, metadata: CollectionMetadata) {
        if (metadata.fields.isEmpty()) {
            return
        }
        val fields = metadata.fields.map { it.name }.toSet()
        table.hashes.forEach { hash ->
            hash.key?.let { template ->
                requireKnownFields(fields, RedisMappingTemplate.of(template).placeholders(), "hash key", table.table)
            }
            hash.field?.let { template ->
                requireKnownFields(fields, RedisMappingTemplate.of(template).placeholders(), "hash field", table.table)
            }
            hash.value?.let { value ->
                check(RedisMappingTemplate.exactPlaceholder(value) in fields) {
                    "Redis hash value '$value' refers to an unknown field for table '${table.table}'"
                }
            }
        }
        table.sets.forEach { set ->
            set.key?.let { template ->
                requireKnownFields(fields, RedisMappingTemplate.of(template).placeholders(), "set key", table.table)
            }
            set.member?.let { value ->
                check(RedisMappingTemplate.exactPlaceholder(value) in fields) {
                    "Redis set member '$value' refers to an unknown field for table '${table.table}'"
                }
            }
        }
    }

    fun requireTable(mappings: RedisTableMappings, collectionName: String): RedisTableMapping =
        checkNotNull(mappings.table(collectionName)) {
            "Could not find Redis mapping for collection '$collectionName'"
        }

    private fun requireKnownFields(
        fields: Set<String>,
        placeholders: Set<String>,
        mappingPart: String,
        tableName: String
    ) {
        val unknown = placeholders - fields
        check(unknown.isEmpty()) {
            "Redis $mappingPart for table '$tableName' refers to unknown fields: ${unknown.joinToString(", ")}"
        }
    }
}
