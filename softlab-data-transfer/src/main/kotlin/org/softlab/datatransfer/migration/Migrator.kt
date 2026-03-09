/**
 * Copyright (C) 2025-2026, Viktor Samokhin (wowyupiyo@gmail.com)
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

package org.softlab.datatransfer.migration

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.withContext
import org.softlab.datatransfer.core.StringTokenFilter
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.core.DocumentCollection


class Migrator(
    private val workerThreads: Int = Runtime.getRuntime().availableProcessors().coerceAtLeast(1),
    private val sourceFilter: StringTokenFilter = StringTokenFilter.from(emptyList())
) {
    private val dispatcher: CoroutineDispatcher
    init {
        require(workerThreads > 0) { "workerThreads must be greater than 0" }
        dispatcher = Dispatchers.IO.limitedParallelism(workerThreads)
    }

    suspend fun migrate(source: DatabaseSource, destination: DatabaseDestination) = withContext(dispatcher) {
        val tasks = mutableListOf<kotlinx.coroutines.Deferred<Unit>>()
        source.listCollections().collect { collection ->
            tasks += async {
                migrateCollection(collection, destination)
            }
        }
        tasks.awaitAll()
    }

    private suspend fun migrateCollection(
        collection: DocumentCollection,
        destination: DatabaseDestination
    ) {
        val metadata = collection.fetchMetadata()
        if (!sourceFilter.startsWith(metadata.name)) {
            return
        }
        destination.createCollection(metadata)
        destination.insertDocuments(metadata.name, collection.readDocuments())
    }
}
