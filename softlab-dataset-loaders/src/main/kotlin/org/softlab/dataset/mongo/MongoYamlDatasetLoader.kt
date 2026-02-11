/**
 * Copyright (C) 2023-2026, Viktor Samokhin (wowyupiyo@gmail.com)
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

package org.softlab.dataset.mongo

import io.github.oshai.kotlinlogging.KotlinLogging
import org.bson.BsonDocument
import org.bson.Document
import org.softlab.dataset.YamlDatasetLoading
import org.softlab.dataset.YamlTablesRows
import org.softlab.dataset.mongo.MongoTypesMapper.asBsonDocument
import java.time.ZoneId
import java.time.format.DateTimeFormatter


/**
 * Map DBUnit's YAML dataset to Mongo's document 1:1
 * Where each dataset table turns into a Mongo collection and each table column to a [BsonDocument] property
 *
 * Prior to using this class, ensure that the corresponding schema has been already created in Mongo
 */
class MongoYamlDatasetLoader(
    private val mongoDb: MongoDatabaseFacade,
    dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT
) : YamlDatasetLoading() {
    companion object {
        private const val BATCH_SIZE: Int = 500
    }

    override val logger = KotlinLogging.logger {}

    private val formatter = dateTimeFormatter.withZone(ZoneId.of("UTC"))

    override fun loadImpl(dataset: YamlTablesRows, cleanBefore: Boolean) {
        val collectionDefinitions = mongoDb.mapCollections()
        dataset.entries.forEach { table ->
            val collectionName = table.key
            logger.info { "Loading dataset for collection: $collectionName" }
            val collection = mongoDb.getCollection(collectionName)
            if (cleanBefore) collection.deleteAll()
            val collectionDefinition = checkNotNull(collectionDefinitions[collectionName]) {
                "Collection does not exist in the database: $collectionName"
            }
            val fieldTypes = MongoTypesMapper.listValidatorTypes(collectionDefinition)
                ?: error("Could not retrieve validator options for collection: $collectionDefinition")
            table.value.asSequence()
                .map { it.asBsonDocument(fieldTypes, formatter) }
                .chunked(BATCH_SIZE)
                .forEach {
                    collection.insertMany(it)
                }
        }
    }

    private fun MongoDatabaseFacade.mapCollections(): Map<String, Document> =
        this.listCollections().toList().associateBy { it.getString("name") }
}
