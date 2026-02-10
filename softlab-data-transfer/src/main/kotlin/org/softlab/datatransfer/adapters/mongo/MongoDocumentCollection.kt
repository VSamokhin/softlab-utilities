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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.bson.BsonDocument
import org.bson.Document
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DocumentCollection
import org.softlab.datatransfer.core.FieldMetadata


class MongoDocumentCollection(
    private val dbName: String,
    private val collectionName: String,
    private val source: MongoSource
) : DocumentCollection {
    override val metadata: CollectionMetadata by lazy {
        // No strict schema in Mongo — just sample first document
        val sampleDoc = runBlocking {
            source.getClient()
                .getDatabase(dbName)
                .getCollection<BsonDocument>(collectionName)
                .find()
                .firstOrNull()
        }

        val fields = sampleDoc?.keys?.map { FieldMetadata(it, "string") } ?: emptyList()
        CollectionMetadata(collectionName, fields)
    }

    override fun readDocuments(): Flow<org.softlab.datatransfer.core.Document> = flow {
        val mongoCollection = source.getClient()
            .getDatabase(dbName)
            .getCollection(collectionName, Document::class.java)

        mongoCollection.find().collect { doc ->
            emit(doc.toMap())
        }
    }
}
