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


/**
 * Map DBUnit's YAML dataset to Redis' hash or set entries using a user-provided mapping
 * This approach has obvious schema limitations and thus the mapping must be well-thought-out
 */
class RedisYamlDatasetLoader(
    private val redisHash: RedisFacade,
    private val mappingsFile: String
) : YamlDatasetLoading() {

    override val logger = KotlinLogging.logger {}

    override fun loadImpl(dataset: YamlTablesRows, cleanBefore: Boolean) {
        logger.info { "Loading DBUnit -> Redis mappings from: $mappingsFile" }
        val mappings = RedisMappingsLoader.load(mappingsFile)

        if (cleanBefore) redisHash.flushDb()

        dataset.entries.forEach { (tableName, rows) ->
            val matchedTable = mappings.table(tableName)
            val seedData = RedisDatasetMapper.mapRows(rows, matchedTable)
            seedData.hashes.forEach { (key, entries) ->
                redisHash.hashSet(key, entries)
            }
            seedData.sets.forEach { (key, members) ->
                redisHash.setAdd(key, members)
            }
        }
    }
}
