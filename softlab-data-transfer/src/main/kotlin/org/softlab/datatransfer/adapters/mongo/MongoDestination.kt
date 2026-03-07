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
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.any
import kotlinx.coroutines.flow.chunked
import kotlinx.coroutines.launch
import org.bson.BsonDocument
import org.bson.Document
import org.softlab.dataset.mongo.MongoTypesMapper.asBsonDocument
import org.softlab.datatransfer.core.BATCH_SIZE
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.REPORT_ON_INSERTS
import org.softlab.datatransfer.core.TransferDocument
import org.softlab.datatransfer.util.Mongo
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.plusAssign


class MongoDestination(
    private val uri: String,
    private val dataTypeMappings: Map<String, String>,
    private val databaseName: String = Mongo.getDatabaseName(uri)
) : DatabaseDestination {
    companion object {
        const val BACKEND = "mongo"
    }

    private val logger = KotlinLogging.logger {}
    private val client: MongoClient = MongoClient.create(uri)
    private val db = client.getDatabase(databaseName)

    override fun getBackendName(): String = BACKEND

    override suspend fun createCollection(metadata: CollectionMetadata) {
        check(!db.listCollectionNames().any { it == metadata.name }) {
            "Collection '${metadata.name}' already exists, please drop it before proceeding"
        }

        logger.debug { "Creating collection '$metadata.name'" }
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

    override suspend fun dropCollection(collectionName: String) {
        logger.debug { "Dropping collection '$collectionName'" }
        db.getCollection<Document>(collectionName).drop()
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

    @OptIn(ExperimentalCoroutinesApi::class, ExperimentalAtomicApi::class)
    override suspend fun insertDocuments(collectionName: String, documents: Flow<TransferDocument>) {
        logger.debug { "Inserting documents into '$collectionName'" }

        val collection = db.getCollection<BsonDocument>(collectionName)
        val total = AtomicInt(0)
        coroutineScope {
            documents.chunked(BATCH_SIZE).collect { batch ->
                launch {
                    val docs = batch.map { doc -> doc.asBsonDocument() }
                    collection.insertMany(docs)
                    total += docs.size
                    if (total.load() % REPORT_ON_INSERTS == 0) {
                        logger.info { "Inserted ${total.load()} documents into '$collectionName'" }
                    }
                }
            }
        }
        logger.info { "Total of ${total.load()} documents inserted into '$collectionName'" }
    }

    override fun close() {
        client.close()
    }
}
