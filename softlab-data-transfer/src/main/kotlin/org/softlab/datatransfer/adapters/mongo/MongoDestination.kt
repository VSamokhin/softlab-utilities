/**
 * Copyright (C) 2025, Viktor Samokhin (wowyupiyo@gmail.com)
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

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import kotlinx.coroutines.flow.Flow
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.Document
import org.bson.Document as BsonDocument


class MongoDestination(
    private val connString: String,
    private val dbName: String = connString.substringAfterLast("/")
) : DatabaseDestination {
    private val client: MongoClient = MongoClients.create(connString)
    private val db = client.getDatabase(dbName)

    override suspend fun createCollection(metadata: CollectionMetadata) {
        if (!db.listCollectionNames().contains(metadata.name)) {
            db.createCollection(metadata.name)
        }
    }

    override suspend fun insertDocuments(collectionName: String, documents: Flow<Document>) {
        val collection = db.getCollection(collectionName)
        documents.collect { doc ->
            collection.insertOne(BsonDocument(doc.mapValues { BsonDocument.parse("{\"value\": \"${it.value}\"}") }))
        }
    }

    override fun close() {
        client.close()
    }
}
