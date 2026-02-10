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
import org.softlab.datatransfer.core.DocumentCollection
import org.softlab.datatransfer.core.DatabaseSource


class MongoSource(
    dbUrl: String,
    private val client: MongoClient = MongoClient.create(dbUrl),
    private val dbName: String = dbUrl.substringAfterLast("/")
) : DatabaseSource {
    private val db = client.getDatabase(dbName)

    override fun listCollections(): Flow<DocumentCollection> {
        return runBlocking {
            db.listCollectionNames().map {
                MongoDocumentCollection(dbName, it, this@MongoSource)
            }
        }
    }

    override fun close() {
        client.close()
    }

    fun getClient(): MongoClient = client
}

