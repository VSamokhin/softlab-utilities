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

package org.softlab.dataset.mongo

import io.github.oshai.kotlinlogging.KotlinLogging
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.BsonValue
import org.bson.Document
import org.softlab.dataset.YamlDatasetLoading
import org.softlab.dataset.YamlTablesRows
import java.time.ZoneId
import java.time.ZonedDateTime
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
            val collection = mongoDb.getCollection(collectionName)
            if (cleanBefore) collection.deleteAll()
            val collectionDefinition = checkNotNull(collectionDefinitions[collectionName]) {
                "Collection does not exist: $collectionName"
            }
            val fieldTypes = listFieldTypes(collectionDefinition)
            table.value.asSequence()
                .map { row ->
                    asBsonDocument(row, fieldTypes)
                }
                .chunked(BATCH_SIZE)
                .forEach {
                    collection.insertMany(it)
                }
        }
    }

    private fun MongoDatabaseFacade.mapCollections(): Map<String, Document> =
        this.listCollections().toList().associateBy { it.getString("name") }

    private fun listFieldTypes(collectionInfo: Document): Map<String, Any> =
        collectionInfo
            .get("options", Document::class.java)
            ?.get("validator", Document::class.java)
            ?.get("\$jsonSchema", Document::class.java)
            ?.get("properties", Document::class.java)
            ?.entries
            ?.associate { it.key to ((it.value as Document).get("bsonType"))!! }
            ?: error("Could not list types for collection: $collectionInfo")

    private fun asBsonDocument(fields: Map<String, Any>, fieldTypes: Map<String, Any>): BsonDocument =
        fields.entries.map { field ->
            val fieldType: Any = fieldTypes[field.key]
                ?: error("Could not find field '${field.key}' among defined in the schema: ${fieldTypes.keys}")
            val value = convertValue(field, fieldType)
            BsonElement(field.key, value)
        }.let { BsonDocument(it) }

    @Suppress("MagicNumber")
    private fun convertValue(field: Map.Entry<String, Any>, fieldType: Any): BsonValue =
        when (fieldType) {
            "double", 1 -> BsonDouble(field.value as Double)
            "string", 2 -> BsonString(field.value.toString())
            "binData", 5 -> BsonBinary(
                org.dbunit.util.Base64.decode(field.value.toString()
                ))
            "bool", 8 -> BsonBoolean(field.value as Boolean)
            "date", 9 -> {
                val dateTime = ZonedDateTime.parse(field.value.toString(), formatter)
                BsonDateTime(dateTime.toInstant().toEpochMilli())
            }
            "int", 16 -> BsonInt32(field.value as Int)
            "long", 18 -> BsonInt64(field.value as Long)
            is List<*> -> {
                // I may expect a list of no more than two types, e.g. ["string", "null"]
                val meaningfulType = fieldType.filterNotNull().single { it != "null" }
                convertValue(field, meaningfulType)
            }
            else -> error("Could not yet handle type '$fieldType' of: ${field.key}")
        }
}
