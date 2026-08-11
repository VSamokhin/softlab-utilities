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

package org.softlab.datatransfer.adapters.mongo

import com.mongodb.client.model.Projections
import com.mongodb.kotlin.client.coroutine.MongoDatabase
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.any
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.bson.BsonDocument
import org.bson.Document
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.mongo.MongoTypesMapper
import org.softlab.dataset.mongo.MongoTypesMapper.asNormalizedMap
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DocumentCollection
import org.softlab.datatransfer.core.IndexDefinition
import org.softlab.datatransfer.core.IndexFieldDefinition
import org.softlab.datatransfer.core.IndexSortOrder


class MongoDocumentCollection(
    private val collectionName: String,
    private val mongoDb: MongoDatabase
) : DocumentCollection {
    private val logger = KotlinLogging.logger {}

    private val metadata: CollectionMetadata by lazy {
        val colInfo = runBlocking {
            mongoDb.listCollections()
                .first { it["name"] == collectionName }
        }

        // Try to get schema from validator if exists
        val types = MongoTypesMapper.listValidatorTypes(colInfo)
            ?: runBlocking {
                // Well, I have slim chances to get proper types from a sample document
                mongoDb.getCollection<BsonDocument>(collectionName)
                    .find()
                    .firstOrNull()
            }?.let {
                MongoTypesMapper.listDocumentTypes(it)
            }.also {
                logger.warn {
                    "Mongo collection '$collectionName' does not have validator" +
                        " so I could not determine field types.\n" +
                        "I will try to sample a document and get proper types from it," +
                        " however it is an error prone approach" +
                        " because some documents may miss some fields."
                }
            }
            ?: emptyMap<String, FieldDefinition>().also { // Give up
                logger.warn {
                    "There is no way to determine field types, later I may fail creating a collection/JDBC table"
                }
            }

        CollectionMetadata(
            name = collectionName,
            fields = types,
            indices = readIndices()
        )
    }

    override suspend fun fetchMetadata(): CollectionMetadata = metadata

    override fun readDocuments(): Flow<org.softlab.datatransfer.core.TransferDocument> = flow {
        if (!mongoDb.listCollectionNames().any { it == collectionName })
            error("Collection '$collectionName' does not exist in database '${mongoDb.name}'")
        mongoDb.getCollection<Document>(collectionName)
            .find().projection(Projections.excludeId()).collect { doc ->
                emit(doc.asNormalizedMap())
            }
    }

    private fun readIndices(): List<IndexDefinition> =
        runBlocking {
            val indices = mutableListOf<IndexDefinition>()
            mongoDb.getCollection<Document>(collectionName).listIndexes().collect { index ->
                readIndex(index)?.let(indices::add)
            }
            indices.sortedBy { it.name }
        }

    private fun readIndex(index: Document): IndexDefinition? {
        val name = index.getString(INDEX_NAME_FIELD)
        val keys = index[INDEX_KEY_FIELD] as? Document
        return when {
            name == ID_INDEX_NAME -> null
            name.isNullOrBlank() || keys == null -> {
                logger.warn { "Skipping Mongo index without a portable name or key document: $index" }
                null
            }
            !isSupportedIndexOptions(index) -> {
                logger.warn {
                    "Skipping Mongo index '$name' on collection '$collectionName' because it has unsupported options"
                }
                null
            }
            else -> readIndexFields(name, keys)?.let { fields ->
                IndexDefinition(
                    name = name,
                    fields = fields,
                    unique = index.getBoolean(INDEX_UNIQUE_FIELD, false)
                )
            }
        }
    }

    private fun readIndexFields(indexName: String, keys: Document): List<IndexFieldDefinition>? {
        val fields = keys.map { (fieldName, order) ->
            readIndexField(indexName, fieldName, order) ?: return null
        }
        return fields.ifEmpty {
            logger.warn { "Skipping Mongo index '$indexName' on collection '$collectionName' because it has no fields" }
            null
        }
    }

    private fun readIndexField(indexName: String, fieldName: String, order: Any?): IndexFieldDefinition? =
        when {
            fieldName == WILDCARD_INDEX_FIELD -> {
                logger.warn { "Skipping Mongo wildcard index '$indexName' on collection '$collectionName'" }
                null
            }
            order == ASCENDING_INDEX_VALUE ->
                IndexFieldDefinition(fieldName.lowercase(), IndexSortOrder.ASC)
            order == DESCENDING_INDEX_VALUE ->
                IndexFieldDefinition(fieldName.lowercase(), IndexSortOrder.DESC)
            else -> {
                logger.warn {
                    "Skipping Mongo index '$indexName' on collection '$collectionName' because field " +
                        "'$fieldName' has unsupported order or type '$order'"
                }
                null
            }
        }

    private fun isSupportedIndexOptions(index: Document): Boolean =
        listOf(
            INDEX_PARTIAL_FILTER_FIELD,
            INDEX_SPARSE_FIELD,
            INDEX_EXPIRE_AFTER_SECONDS_FIELD,
            INDEX_COLLATION_FIELD
        ).none(index::containsKey)

    companion object {
        private const val ASCENDING_INDEX_VALUE = 1
        private const val DESCENDING_INDEX_VALUE = -1
        private const val ID_INDEX_NAME = "_id_"
        private const val INDEX_COLLATION_FIELD = "collation"
        private const val INDEX_EXPIRE_AFTER_SECONDS_FIELD = "expireAfterSeconds"
        private const val INDEX_KEY_FIELD = "key"
        private const val INDEX_NAME_FIELD = "name"
        private const val INDEX_PARTIAL_FILTER_FIELD = "partialFilterExpression"
        private const val INDEX_SPARSE_FIELD = "sparse"
        private const val INDEX_UNIQUE_FIELD = "unique"
        private const val WILDCARD_INDEX_FIELD = "\$**"
    }
}
