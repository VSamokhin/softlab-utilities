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

import com.fasterxml.jackson.databind.ObjectMapper
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.BsonType
import org.bson.BsonValue
import org.bson.Document
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Date


object MongoTypesMapper {
    private fun unknownType(type: Any, field: String): Nothing =
        error("Unknown type '$type' of: $field")

    /**
     * Return a mapping of columns to their types as defined in the collection validator, if any
     */
    fun listValidatorTypes(collectionInfo: Document): Map<String, Any>? =
        collectionInfo
            .get("options", Document::class.java)
            ?.get("validator", Document::class.java)
            ?.get("\$jsonSchema", Document::class.java)
            ?.get("properties", Document::class.java)
            ?.entries
            ?.associate { it.key to ((it.value as Document)["bsonType"])!! }

    fun listDocumentTypes(document: BsonDocument): Map<String, Any> {
        return document.entries.associate { (key, value) ->
            val type = when (value.bsonType) {
                BsonType.DOUBLE -> "double"
                BsonType.STRING,
                BsonType.SYMBOL,
                BsonType.OBJECT_ID -> "string"
                BsonType.BINARY -> "binData"
                BsonType.BOOLEAN -> "bool"
                BsonType.DATE_TIME -> "date"
                BsonType.INT32 -> "int"
                BsonType.INT64 -> "long"
                BsonType.DOCUMENT -> "object"
                BsonType.ARRAY -> "array"
                BsonType.DECIMAL128 -> "decimal"
                BsonType.TIMESTAMP -> "timestamp"
                else -> unknownType(value.bsonType.name, key)
            }
            key to type
        }
    }

    /**
     * Convert a map of field-value pairs as it comes out of [ObjectMapper.readValue] to a [BsonDocument]
     */
    fun Map<String, Any>.asBsonDocument(
        typeHints: Map<String, Any>,
        dateTimeFormatter: DateTimeFormatter
    ): BsonDocument {
        return this.entries.map { field ->
            val fieldType: Any = typeHints[field.key]
                ?: error("Could not find field '${field.key}' among defined in the schema: ${typeHints.keys}")
            val value = convertValue(field.key, field.value, fieldType, dateTimeFormatter)
            BsonElement(field.key, value)
        }.let { BsonDocument(it) }
    }

    /**
     * See [BSON Types](https://www.mongodb.com/docs/manual/reference/bson-types/)
     */
    @Suppress("MagicNumber")
    private fun convertValue(
        name: String,
        value: Any,
        type: Any,
        dateTimeFormatter: DateTimeFormatter
    ): BsonValue {
        return when (type) {
            "double", 1 -> BsonDouble((value as Number).toDouble())
            "string", 2 -> BsonString(value.toString())
            "binData", 5 -> BsonBinary(
                org.dbunit.util.Base64.decode(
                    value.toString()
                )
            )
            "bool", 8 -> BsonBoolean(value as Boolean)
            "date", 9 -> {
                val dateTime = ZonedDateTime.parse(value.toString(), dateTimeFormatter)
                BsonDateTime(dateTime.toInstant().toEpochMilli())
            }
            "int", 16 -> BsonInt32((value as Number).toInt())
            "long", 18 -> BsonInt64((value as Number).toLong())
            is List<*> -> {
                // I may expect a list of no more than two types, e.g. ["string", "null"]
                val intendedType = type.filterNotNull().single { it != "null" }
                convertValue(name, value, intendedType, dateTimeFormatter)
            }

            else -> unknownType(type, name)
        }
    }

    fun Map<String, Any>.asBsonDocument(): BsonDocument {
        return this.entries.map { field ->
            val value = convertValue(field.key, field.value)
            BsonElement(field.key, value)
        }.let { BsonDocument(it) }
    }

    private fun convertValue(
        name: String,
        value: Any
    ): BsonValue {
        return when (value) {
            is Double, is Float -> BsonDouble(value.toDouble())
            is Int, is Short, is Byte -> BsonInt32(value.toInt())
            is Long -> BsonInt64(value )
            is String -> BsonString(value)
            is Boolean -> BsonBoolean(value)
            is Date -> BsonDateTime(value.toInstant().toEpochMilli())
            is ZonedDateTime -> BsonDateTime(value.toInstant().toEpochMilli())
            is ByteArray -> BsonBinary(value)
            is List<*> -> BsonArray(value.map { convertValue(name, it!!) })
            is Array<*> -> BsonArray(value.map { convertValue(name, it!!) })

            else -> unknownType(value::class.simpleName!!, name)
        }
    }
}
