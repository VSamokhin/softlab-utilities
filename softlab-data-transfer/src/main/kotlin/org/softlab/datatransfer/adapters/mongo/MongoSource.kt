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
import kotlinx.coroutines.flow.flow
import org.softlab.datatransfer.core.Collection
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.core.Document
import org.softlab.datatransfer.core.FieldMetadata
import org.bson.Document as BsonDocument


class MongoSource(
    private val connString: String,
    private val dbName: String = connString.substringAfterLast("/")
) : DatabaseSource {
    private val client: MongoClient = MongoClients.create(connString)
    private val db = client.getDatabase(dbName)

    override fun listCollections(): List<Collection> {
        return db.listCollectionNames().toList().map { MongoCollection(dbName, it, this) }
    }

    override fun close() {
        client.close()
    }

    fun getClient(): MongoClient = client
}

class MongoCollection(
    private val dbName: String,
    private val collectionName: String,
    private val source: MongoSource
) : Collection {
    override val metadata: CollectionMetadata by lazy {
        // No strict schema in Mongo — just sample first document
        val sampleDoc = source.getClient()
            .getDatabase(dbName)
            .getCollection(collectionName)
            .find()
            .firstOrNull()

        val fields = sampleDoc?.keys?.map { FieldMetadata(it, "string") } ?: emptyList()
        CollectionMetadata(collectionName, fields)
    }

    override fun readDocuments(): Flow<Document> = flow {
        val mongoCollection = source.getClient()
            .getDatabase(dbName)
            .getCollection(collectionName, BsonDocument::class.java)

        for (doc in mongoCollection.find()) {
            emit(doc.toMap())
        }
    }
}
