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

package org.softlab.dataset

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.oshai.kotlinlogging.KLogger
import java.io.File

/**
 * Definition of "Tables -> Rows -> Columns (name/value)" mappings.
 * I never expect a column value to be NULL, simply omit NULL-columns from the dataset.
 */
typealias YamlTablesRows = Map<String, List<Map<String, Any>>>

abstract class YamlDatasetLoading : DatasetLoader {
    companion object {
        protected val YAML_MAPPER: ObjectMapper = ObjectMapper(YAMLFactory()).registerKotlinModule()
    }
    protected abstract val logger: KLogger

    protected abstract fun loadImpl(dataset: YamlTablesRows, cleanBefore: Boolean)

    override fun load(datasetPath: String, cleanBefore: Boolean) {
        logger.info { "Loading dataset: $datasetPath" }
        val content = readResource(datasetPath).yamlAsObject<YamlTablesRows>()
        loadImpl(content, cleanBefore)
    }

    protected fun readResource(resourcePath: String): String {
        val fileUrl = checkNotNull(javaClass.getClassLoader().getResource(resourcePath)) {
            "Could not read resource file: $resourcePath"
        }
        return File(fileUrl.toURI()).readText()
    }

    protected inline fun <reified T> String.yamlAsObject(): T = YAML_MAPPER.readValue<T>(this)
}
