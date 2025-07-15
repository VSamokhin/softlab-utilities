/**
 * Copyright (C) 2023-2025, Viktor Samokhin (wowyupiyo@gmail.com)
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

package org.softlab.dataset.mongo.coroutine

import com.mongodb.BasicDBObject
import com.mongodb.kotlin.client.coroutine.MongoCollection
import kotlinx.coroutines.runBlocking
import org.bson.BsonDocument
import org.softlab.dataset.mongo.MongoCollectionFacade


class CoroutineMongoCollection(private val wrappedCol: MongoCollection<BsonDocument>) : MongoCollectionFacade {
    override val name: String
        get() = wrappedCol.namespace.collectionName

    override fun deleteAll() {
        runBlocking { wrappedCol.deleteMany(BasicDBObject()) }
    }

    override fun insertMany(docs: List<BsonDocument>) {
        runBlocking { wrappedCol.insertMany(docs) }
    }
}
