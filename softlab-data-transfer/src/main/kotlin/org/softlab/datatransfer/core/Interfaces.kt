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
    /**
     * Collection name
     */
    val name: String,
    /**
     * Field name (in lower case!) to field definition mappings.
     *
     * Can be an empty list if schema is not known.
     */
    val fields: Map<String, FieldDefinition>,
    /**
     * Primary key definition, if known.
     */
    val primaryKey: PrimaryKeyDefinition? = null,
    /**
     * Secondary index definitions, if known.
     */
    val indices: Collection<IndexDefinition> = emptyList()
) {
    init {
        require(name.isNotBlank()) { "Collection name must not be blank" }
        require(fields.keys.none { it.isBlank() }) { "Collection field names must not be blank" }
        require(fields.values.map { it.name.lowercase() }.toSet().size == fields.size) {
            "Collection field names must not contain duplicates"
        }
        val namedIndices = indices.mapNotNull { it.name }
        require(namedIndices.toSet().size == namedIndices.size) {
            "Index names must not contain duplicates"
        }
        validateKnownFields(primaryKey?.fields.orEmpty(), "Primary key")
        indices.forEach { index ->
            validateKnownFields(index.fields.map { it.name }, index.displayName())
        }
    }

    private fun validateKnownFields(fieldNames: Collection<String>, owner: String) {
        if (fields.isEmpty()) {
            return
        }
        val knownFieldNames = fields.keys.map { it.lowercase() }.toSet()
        val unknownFieldNames = fieldNames.filterNot { it.lowercase() in knownFieldNames }
        require(unknownFieldNames.isEmpty()) {
            "$owner contains unknown field names: ${unknownFieldNames.joinToString(", ")}"
        }
    }
}

private fun IndexDefinition.displayName(): String =
    name?.let { "Index '$it'" } ?: "Unnamed index"

/**
 * Represents a primary key definition.
 */
data class PrimaryKeyDefinition(
    val name: String?,
    val fields: List<String>
) {
    init {
        require(name == null || name.isNotBlank()) { "Primary key name must not be blank" }
        validateFieldNames(fields, "Primary key")
    }
}

/**
 * Represents a secondary index definition.
 */
data class IndexDefinition(
    val name: String?,
    val fields: List<IndexFieldDefinition>,
    val unique: Boolean = false
) {
    init {
        require(name == null || name.isNotBlank()) { "Index name must not be blank" }
        validateFieldNames(fields.map { it.name }, "Index")
    }
}

/**
 * Represents a field within a secondary index definition.
 */
data class IndexFieldDefinition(
    val name: String,
    val order: IndexSortOrder = IndexSortOrder.ASC
) {
    init {
        require(name.isNotBlank()) { "Index field name must not be blank" }
    }
}

private fun validateFieldNames(fields: Collection<String>, owner: String) {
    require(fields.isNotEmpty()) { "$owner fields must not be empty" }
    require(fields.none { it.isBlank() }) { "$owner field names must not be blank" }
    require(fields.map { it.lowercase() }.toSet().size == fields.size) {
        "$owner field names must not contain duplicates"
    }
}

/**
 * Represents portable index field sort order.
 */
enum class IndexSortOrder {
    ASC,
    DESC
}

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
 * How many rows may remain pending for writing
 */
const val MAX_PENDING_ROWS: Int = 50_000

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
    suspend fun createIndexesAndConstraints(metadata: CollectionMetadata) = Unit
}
