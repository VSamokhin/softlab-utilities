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

package org.softlab.datataset.test.initiators

import com.mongodb.kotlin.client.coroutine.MongoClient
import com.mongodb.kotlin.client.coroutine.MongoDatabase
import kotlinx.coroutines.runBlocking
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.resource.ClassLoaderResourceAccessor
import org.softlab.dataset.mongo.MongoDatabaseFacade
import org.softlab.dataset.mongo.MongoYamlDatasetLoader
import org.softlab.dataset.mongo.coroutine.CoroutineMongoDatabase
import org.testcontainers.mongodb.MongoDBContainer


const val MONGO_CONTAINER = "mongo:8.2"

fun createMongoContainer(container: String = MONGO_CONTAINER): MongoDBContainer =
    MongoDBContainer(container)

class MongoInitiator(override val dbUrl: String) : DatabaseInitiator<MongoDatabase> {
    val mongoClient: MongoClient = MongoClient.create(dbUrl)
    val mongoDb: MongoDatabase
    private val mongoFacade: MongoDatabaseFacade

    init {
        val dbName = dbUrl.substringAfterLast("/")
        mongoDb = runBlocking { mongoClient.getDatabase(dbName) }
        mongoFacade = CoroutineMongoDatabase(mongoDb)
    }

    override fun cleanup(additionalSteps: (MongoDatabase) -> Unit) {
        runBlocking { mongoDb.drop() }
        additionalSteps(mongoDb)
    }

    override fun initSchema(changelogPath: String, additionalSteps: (MongoDatabase) -> Unit) {
            DatabaseFactory.getInstance().openDatabase(
                dbUrl,
                null,
                null,
                null,
                null
            ).use { database ->
                Liquibase(
                    changelogPath,
                    ClassLoaderResourceAccessor(),
                    database
                ).use { liquibase -> liquibase.update() }
            }
        additionalSteps(mongoDb)
    }

    override fun seedData(datasetPath: String) {
        MongoYamlDatasetLoader(mongoFacade).load(datasetPath)
    }

    override fun close() {
        mongoClient.close()
    }
}
