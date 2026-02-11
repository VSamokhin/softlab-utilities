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
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Date


object MongoTypesMapper {
    fun listValidatorTypes(collectionInfo: Document): Map<String, Any>? =
        collectionInfo
            .get("options", Document::class.java)
            ?.get("validator", Document::class.java)
            ?.get("\$jsonSchema", Document::class.java)
            ?.get("properties", Document::class.java)
            ?.entries
            ?.associate { it.key to ((it.value as Document)["bsonType"])!! }

    /**
     * This is a preferred method to convert a map of field values to a [BsonDocument],
     * because it relies on the defined via the validator schema types.
     */
    fun Map<String, Any>.asBsonDocument(
        fieldTypes: Map<String, Any>,
        dateTimeFormatter: DateTimeFormatter
    ): BsonDocument {
        return this.entries.map { field ->
            val fieldType: Any = fieldTypes[field.key]
                ?: error("Could not find field '${field.key}' among defined in the schema: ${fieldTypes.keys}")
            val value = convertValue(field, fieldType, dateTimeFormatter)
            BsonElement(field.key, value)
        }.let { BsonDocument(it) }
    }

    /**
     * See [BSON Types](https://www.mongodb.com/docs/manual/reference/bson-types/)
     */
    @Suppress("MagicNumber")
    private fun convertValue(
        field: Map.Entry<String, Any>,
        fieldType: Any,
        dateTimeFormatter: DateTimeFormatter
    ): BsonValue {
        return when (fieldType) {
            "double", 1 -> BsonDouble(field.value as Double)
            "string", 2 -> BsonString(field.value.toString())
            "binData", 5 -> BsonBinary(
                org.dbunit.util.Base64.decode(
                    field.value.toString()
                )
            )
            "bool", 8 -> BsonBoolean(field.value as Boolean)
            "date", 9 -> {
                val dateTime = ZonedDateTime.parse(field.value.toString(), dateTimeFormatter)
                BsonDateTime(dateTime.toInstant().toEpochMilli())
            }
            "int", 16 -> BsonInt32(field.value as Int)
            "long", 18 -> BsonInt64(field.value as Long)
            is List<*> -> {
                // I may expect a list of no more than two types, e.g. ["string", "null"]
                val intendedType = fieldType.filterNotNull().single { it != "null" }
                convertValue(field, intendedType, dateTimeFormatter)
            }

            else -> error("Could not yet handle type '$fieldType' of: ${field.key}")
        }
    }

    fun Map<String, Any>.asBsonDocument(
        dateTimeFormatter: DateTimeFormatter
    ): BsonDocument {
        return this.entries.map { field ->
            val value = convertValue(field, dateTimeFormatter)
            BsonElement(field.key, value)
        }.let { BsonDocument(it) }
    }

    private fun convertValue(
        field: Map.Entry<String, Any>,
        dateTimeFormatter: DateTimeFormatter
    ): BsonValue {
        return when (val value = field.value) {
            is Double, is Float -> BsonDouble(value.toDouble())
            is Int, is Short, is Byte -> BsonInt32(value.toInt())
            is Long -> BsonInt64(value )
            is String -> BsonString(value)
            is Boolean -> BsonBoolean(value)
            is Date, is ZonedDateTime -> {
                val dateTime = ZonedDateTime.parse(value.toString(), dateTimeFormatter)
                BsonDateTime(dateTime.toInstant().toEpochMilli())
            }
            is ByteArray -> BsonBinary(value)

            else -> error("Could not yet handle type '${field.value::class.simpleName}' of: ${field.key}")
        }
    }
}
