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

import org.softlab.dataset.core.FieldDefinition


data class RedisTableMappings(
    val tables: List<RedisTableMapping> = emptyList()
) {
    fun table(tableName: String): RedisTableMapping =
        checkNotNull(tables.firstOrNull { it.table == tableName }) {
            "Could not find Redis mapping for table: $tableName"
        }
}

data class RedisTableMapping(
    val table: String,
    val fields: List<FieldDefinition> = emptyList(),
    val hashes: List<RedisHashMapping> = emptyList(),
    val sets: List<RedisSetMapping> = emptyList()
) {
    fun anchorHash(): RedisHashMapping =
        hashes.firstOrNull { it.value == null && it.field == null } ?: error(
            "Redis table '$table' must define a row-level anchor hash without field/value mappings"
        )
}

data class RedisHashMapping(
    val key: String? = null,
    val field: String? = null,
    val value: String? = null
)

data class RedisSetMapping(
    val key: String? = null,
    val member: String? = null
)
