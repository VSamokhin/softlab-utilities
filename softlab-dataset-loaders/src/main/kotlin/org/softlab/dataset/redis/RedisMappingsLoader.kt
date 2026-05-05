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

package org.softlab.dataset.redis

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.softlab.dataset.core.FileLoader


object RedisMappingsLoader {
    private val yamlMapper: ObjectMapper = ObjectMapper(YAMLFactory()).registerKotlinModule()

    fun load(pathOrResource: String): RedisTableMappings {
        val content = FileLoader.readText(pathOrResource)
        return checkNotNull(content) {
            "Could not read Redis mapping file: $pathOrResource"
        }.let { yamlMapper.readValue<RedisTableMappings>(it) }
    }
}
