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

import java.util.regex.Pattern


/**
 * Encapsulates placeholder-based Redis mapping templates like `users:${id}`.
 */
class RedisMappingTemplate private constructor(
    private val template: String,
    private val placeholders: List<String>,
    private val regex: Regex
) {
    companion object {
        private val VALUE_PLACEHOLDER: Pattern = "\\$\\{([A-Za-z0-9@_-]+)\\}".toPattern()

        fun of(template: String): RedisMappingTemplate {
            val matcher = VALUE_PLACEHOLDER.matcher(template)
            val placeholders = mutableListOf<String>()
            val regex = StringBuilder("^")
            var currentStart = 0
            while (matcher.find()) {
                regex.append(Regex.escape(template.substring(currentStart, matcher.start())))
                regex.append("(.+?)")
                placeholders += matcher.group(1)
                currentStart = matcher.end()
            }
            regex.append(Regex.escape(template.substring(currentStart)))
            regex.append("$")
            return RedisMappingTemplate(template, placeholders, Regex(regex.toString()))
        }

        fun exactPlaceholder(value: String): String {
            val matcher = VALUE_PLACEHOLDER.matcher(value)
            if (matcher.matches()) {
                return matcher.group(1)
            } else {
                error("Value must either be empty or contain a single column placeholder, but was: $value")
            }
        }
    }

    fun placeholders(): Set<String> =
        placeholders.toCollection(linkedSetOf())

    fun render(values: Map<String, String>): String {
        var result = template
        placeholders().forEach { placeholder ->
            val matchedValue = values[placeholder]
                ?: error("Could not find value for placeholder '$placeholder' among: $values")
            result = result.replace("\${$placeholder}", matchedValue)
        }
        return result
    }

    fun toGlob(): String =
        VALUE_PLACEHOLDER.matcher(template).replaceAll("*")

    fun match(value: String): Map<String, String>? =
        regex.matchEntire(value)?.let { matched ->
            val result = linkedMapOf<String, String>()
            var hasConflict = false
            placeholders.forEachIndexed { index, placeholder ->
                val matchedValue = matched.groupValues[index + 1]
                val previous = result.putIfAbsent(placeholder, matchedValue)
                if (previous != null && previous != matchedValue) {
                    hasConflict = true
                }
            }
            if (hasConflict) {
                null
            } else {
                result
            }
        }
}
