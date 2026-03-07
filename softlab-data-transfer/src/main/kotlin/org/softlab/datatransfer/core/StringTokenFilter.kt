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

package org.softlab.datatransfer.core

import java.nio.file.Files
import java.nio.file.Path


class StringTokenFilter private constructor(
    val tokens: List<String>
) {
    companion object {
        fun from(rawTokens: List<String>): StringTokenFilter {
            if (rawTokens.size == 1) {
                val candidate = Path.of(rawTokens.single())
                if (Files.isRegularFile(candidate)) {
                    return StringTokenFilter(normalize(Files.readAllLines(candidate)))
                }
            }
            return StringTokenFilter(normalize(rawTokens))
        }

        private fun normalize(rawTokens: List<String>): List<String> =
            rawTokens
                .map { it.trim() }
                .filter { it.isNotEmpty() }
    }

    /**
     * If not tokens present, this method matches everything
     */
    fun startsWith(value: String): Boolean =
        tokens.isEmpty() || tokens.any { token -> value.startsWith(token) }
}
