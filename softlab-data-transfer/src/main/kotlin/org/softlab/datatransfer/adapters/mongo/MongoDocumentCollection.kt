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
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DocumentCollection


class MongoDocumentCollection(
    private val collectionName: String,
    private val source: MongoSource
) : DocumentCollection {
    private val logger = KotlinLogging.logger {}

    private val metadata: CollectionMetadata by lazy {
        val mongoDb = source.getClient().getDatabase(source.dbName)
        val colInfo = runBlocking {
            mongoDb.listCollections()
                .first { it["name"] == collectionName }
        }

        // Try to get schema from validator if exists
        val types = MongoTypesMapper.listValidatorTypes(colInfo)
            ?: runBlocking {
                // Well, I have slim chances to get proper types from a sample document
                source.getClient()
                    .getDatabase(source.dbName)
                    .getCollection<BsonDocument>(collectionName)
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

        CollectionMetadata(collectionName, types.values)
    }

    override suspend fun fetchMetadata(): CollectionMetadata = metadata

    override fun readDocuments(): Flow<org.softlab.datatransfer.core.TransferDocument> = flow {
        val mongoDb = source.getClient().getDatabase(source.dbName)
        if (!mongoDb.listCollectionNames().any { it == collectionName })
            error("Collection '$collectionName' does not exist in database '${source.dbName}'")
        mongoDb.getCollection<Document>(collectionName)
            .find().collect { doc ->
                emit(doc.toMap())
            }
    }
}
