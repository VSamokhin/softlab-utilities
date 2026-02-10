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


/**
 * Represents metadata for a collection (table)
 */
data class CollectionMetadata(
    val name: String,
    val fields: List<FieldMetadata>
)

/**
 * Represents metadata for a field (column)
 */
data class FieldMetadata(
    val name: String,
    val type: String // Could be a standardized type name
)

/**
 * Represents a document (row)
 */
typealias Document = Map<String, Any?>

/**
 * A collection (table in RDBMS, collection in NoSQL)
 */
interface DocumentCollection {
    val metadata: CollectionMetadata
    fun readDocuments(): Flow<Document>
}

/**
 * Source database interface
 */
interface DatabaseSource : AutoCloseable {
    fun listCollections(): Flow<DocumentCollection>
}

/**
 * Destination database interface
 */
interface DatabaseDestination : AutoCloseable {
    suspend fun createCollection(metadata: CollectionMetadata)
    suspend fun insertDocuments(collectionName: String, documents: Flow<Document>)
}
