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

package org.softlab.dataset.core

import java.nio.file.Files
import java.nio.file.Paths


object FileLoader {
    /**
     * Read a text file from (1) file system or (2) resources
     */
    fun readText(pathOrResource: String, classLoader: () -> ClassLoader = { defaultClassLoader() }): String? {
        val path = Paths.get(pathOrResource)
        return when {
            Files.isRegularFile(path) -> Files.readString(path)
            else -> classLoader().getResourceAsStream(pathOrResource)?.bufferedReader()?.use { it.readText() }
        }
    }

    private fun defaultClassLoader(): ClassLoader =
        Thread.currentThread().contextClassLoader ?: FileLoader::class.java.classLoader
}
