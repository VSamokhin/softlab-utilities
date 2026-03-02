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

import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.ValidationOptions
import com.mongodb.kotlin.client.coroutine.MongoClient
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.any
import org.bson.BsonDocument
import org.bson.Document
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.TransferDocument


class MongoDestination(
    private val connString: String,
    private val dataTypeMappings: Map<String, String>,
    private val databaseName: String = connString.substringAfterLast("/")
) : DatabaseDestination {
    companion object {
        const val BACKEND = "mongo"
    }

    private val client: MongoClient = MongoClient.create(connString)
    private val db = client.getDatabase(databaseName)

    override fun getBackendName(): String = BACKEND

    override suspend fun createCollection(metadata: CollectionMetadata) {
        if (!db.listCollectionNames().any { it == metadata.name }) {
            if (metadata.fields.isNotEmpty()) {
                val options = CreateCollectionOptions()
                    .validationOptions(
                        ValidationOptions().validator(buildValidator(metadata))
                    )
                db.createCollection(metadata.name, options)
            } else {
                db.createCollection(metadata.name)
            }
        }
    }

    private fun buildValidator(metadata: CollectionMetadata): Document {
        val properties = metadata.fields.associate { field ->
            field.name to Document(mapOf(
                "bsonType" to bsonTypeFor(field.type, field.nullable)
            ))
        }
        val required = metadata.fields
            .filterNot { it.nullable }
            .map { it.name }
        val jsonSchema = Document(mapOf(
            "bsonType" to "object", "properties" to Document(properties)
        ))
        if (required.isNotEmpty()) {
            jsonSchema["required"] = required
        }
        return Document(mapOf("\$jsonSchema" to jsonSchema))
    }

    private fun bsonTypeFor(type: String, nullable: Boolean): Any {
        val bsonType = dataTypeMappings[type.lowercase()] ?: error("Unsupported type: $type")
        return if (nullable) {
            listOf(bsonType, "null")
        } else {
            bsonType
        }
    }

    override suspend fun insertDocuments(collectionName: String, documents: Flow<TransferDocument>) {
        val collection = db.getCollection<BsonDocument>(collectionName)
        documents.collect { doc ->
            collection.insertOne(Document(doc).toBsonDocument())
        }
    }

    override fun close() {
        client.close()
    }
}
