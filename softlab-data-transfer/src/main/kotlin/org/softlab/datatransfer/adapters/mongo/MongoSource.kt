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

import com.mongodb.kotlin.client.coroutine.MongoClient
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.core.DocumentCollection
import org.softlab.datatransfer.util.Mongo


/**
 * In order to pass authentication you may need to do like:
 * ```
 * MongoSource("mongodb://user:pass@example.com:27017/admin", databaseName = "targetDb")
 * ```
 */
class MongoSource(
    uri: String,
    private val client: MongoClient = MongoClient.create(uri),
    val databaseName: String = Mongo.getDatabaseName(uri)
) : DatabaseSource {
    companion object {
        private const val BACKEND = "mongo"
    }

    private val db = client.getDatabase(databaseName)

    override fun getBackendName(): String = BACKEND

    override fun listCollections(): Flow<DocumentCollection> {
        return runBlocking {
            db.listCollectionNames().map {
                MongoDocumentCollection(it, this@MongoSource)
            }
        }
    }

    override suspend fun countDocuments(collectionName: String): Long {
        return db.getCollection<Document>(collectionName).countDocuments()
    }

    override fun close() {
        client.close()
    }

    fun getClient(): MongoClient = client
}
