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

package org.softlab.datatransfer.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths


class ConfigLoader {
    companion object {
        private val YAML_MAPPER: ObjectMapper = ObjectMapper(YAMLFactory()).registerKotlinModule()
    }

    private val logger = KotlinLogging.logger {}

    private lateinit var root: DataTypeMappingsRoot

    fun load(fileName: String): ConfigLoader {
        root = openConfigFile(fileName).use { input ->
            YAML_MAPPER.readValue(input, DataTypeMappingsRoot::class.java)
        }
        return this
    }

    fun getDataTypeMappings(): DataTypeMappingsConfig =
        DataTypeMappingsConfig(
            destinationMappings = root.dataTransfer.dataTypeMappings.destination
                .mapKeys { (backendName, _) -> backendName.lowercase() }
                .mapValues { (_, targetMappings) ->
                    toSourceLookup(targetMappings)
                }
        )

    private fun toSourceLookup(targetMappings: Map<String, List<String>>): Map<String, String> {
        val result = mutableMapOf<String, String>()
        targetMappings.forEach { (targetType, sourceTypes) ->
            mapSourceType(result, targetType.lowercase(), targetType)
            sourceTypes.forEach { sourceType ->
                mapSourceType(result, sourceType.lowercase(), targetType)
            }
        }
        return result
    }

    private fun mapSourceType(
        result: MutableMap<String, String>,
        sourceType: String,
        targetType: String
    ) {
        val previous = result.putIfAbsent(sourceType, targetType)
        check(previous == null || previous == targetType) {
            "Ambiguous mapping for source type '$sourceType': '$previous' and '$targetType'"
        }
    }

    private fun openConfigFile(fileName: String): InputStream =
        // Custom config file next to application JAR takes precedence over resource config
        userFile(fileName)
            ?.let { Files.newInputStream(it) }
            ?.also { logger.debug { "Loading user-defined config file: $fileName" } }
            ?: ConfigLoader::class.java.classLoader.getResourceAsStream(fileName)
                ?.also { logger.debug { "Loading embedded config file '$fileName'" } }
            ?: error("Could not find config '$fileName' in resources or next to application JAR")

    private fun userFile(fileName: String): Path? {
        val cwd = Paths.get(System.getProperty("user.dir"))
        val candidate = cwd.resolve(fileName)
        return if (Files.isRegularFile(candidate)) candidate else null
    }
}

private data class DataTypeMappingsRoot(
    @param:JsonProperty("data-transfer")
    val dataTransfer: DataTransferSection = DataTransferSection()
)

private data class DataTransferSection(
    @param:JsonProperty("data-type-mappings")
    val dataTypeMappings: DataTypeMappingsSection = DataTypeMappingsSection()
)

private data class DataTypeMappingsSection(
    val destination: Map<String, Map<String, List<String>>> = emptyMap()
)
