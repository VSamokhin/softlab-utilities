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

package org.softlab.datatransfer.core

import kotlinx.coroutines.flow.Flow
import org.softlab.dataset.core.FieldDefinition


/**
 * Represents metadata for a collection (table)
 */
data class CollectionMetadata(
    val name: String,
    /**
     * Can be an empty list if schema is not known
     */
    val fields: Collection<FieldDefinition>
)

/**
 * Represents a document (row)
 */
typealias TransferDocument = Map<String, Any?>

/**
 * A collection (table in RDBMS, collection in NoSQL).
 * Its methods shall throw exceptions if collection does not exist or is not accessible.
 */
interface DocumentCollection {
    suspend fun fetchMetadata(): CollectionMetadata
    fun readDocuments(): Flow<TransferDocument>
}

/**
 * Source database interface
 */
interface DatabaseSource : AutoCloseable {
    fun getBackendName(): String
    fun listCollections(): Flow<DocumentCollection>
    suspend fun countDocuments(collectionName: String): Long
}

/**
 * Report progress on every X inserts
 */
const val REPORT_ON_INSERTS: Int = 20_000
/**
 * For where read/write batches are applicable, use this value
 */
const val BATCH_SIZE: Int = 10_000

/**
 * Destination database interface
 */
interface DatabaseDestination : AutoCloseable {
    fun getBackendName(): String
    /**
     * Must fail if collection already exists
     */
    suspend fun createCollection(metadata: CollectionMetadata)
    /**
     * Must not fail if collection does not exist
     */
    suspend fun dropCollection(collectionName: String)
    suspend fun insertDocuments(collectionName: String, documents: Flow<TransferDocument>)
}
