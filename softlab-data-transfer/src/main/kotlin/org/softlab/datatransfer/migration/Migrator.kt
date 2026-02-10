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

import kotlinx.coroutines.runBlocking
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.DatabaseSource


class Migrator {
    suspend fun migrate(source: DatabaseSource, destination: DatabaseDestination) {
        source.listCollections().collect { collection ->
            runBlocking { destination.createCollection(collection.metadata) }
            destination.insertDocuments(
                collection.metadata.name, collection.readDocuments()
            )
        }
    }
}
