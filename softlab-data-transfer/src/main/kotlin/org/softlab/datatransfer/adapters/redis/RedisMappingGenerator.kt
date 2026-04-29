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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.redis.RedisHashMapping
import org.softlab.dataset.redis.RedisSetMapping
import org.softlab.dataset.redis.RedisTableMapping
import org.softlab.dataset.redis.RedisTableMappings
import org.softlab.datatransfer.core.CollectionMetadata


object RedisMappingGenerator {
    private val yamlMapper: ObjectMapper = ObjectMapper(YAMLFactory()).registerKotlinModule()
    private val preferredIdNames = setOf("id", "_id")

    fun generate(metadata: CollectionMetadata): RedisTableMappings? {
        val fields = metadata.fields.toList()
        if (fields.isEmpty()) {
            return null
        }
        val anchorField = chooseAnchorField(fields)
        return RedisTableMappings(
            tables = listOf(
                RedisTableMapping(
                    table = metadata.name,
                    fields = fields,
                    sets = listOf(RedisSetMapping(key = metadata.name, member = "\${$anchorField}")),
                    hashes = listOf(RedisHashMapping(key = "${metadata.name}:\${$anchorField}"))
                )
            )
        )
    }

    fun asYaml(mappings: RedisTableMappings): String =
        yamlMapper.writeValueAsString(mappings).trim()

    private fun chooseAnchorField(fields: List<FieldDefinition>): String {
        val nonNullable = fields.filterNot { it.nullable }
        return exactIdField(nonNullable)
            ?: exactIdField(fields)
            ?: suffixIdField(nonNullable)
            ?: suffixIdField(fields)
            ?: nonNullable.firstOrNull()?.name
            ?: fields.first().name
    }

    private fun exactIdField(fields: List<FieldDefinition>): String? =
        fields.firstOrNull { it.name.lowercase() in preferredIdNames }?.name

    private fun suffixIdField(fields: List<FieldDefinition>): String? =
        fields.firstOrNull { field ->
            val lower = field.name.lowercase()
            lower.endsWith("_id") || lower.endsWith("_pk")
        }?.name
}
