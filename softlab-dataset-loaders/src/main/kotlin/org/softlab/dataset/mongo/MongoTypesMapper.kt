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
import org.bson.BsonNull
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.BsonType
import org.bson.BsonValue
import org.bson.Document
import org.bson.types.Binary
import org.softlab.dataset.core.FieldDefinition
import java.sql.Timestamp
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Date
import java.util.UUID

/**
 * For the sake of simplicity
 * - I treat BSON's "timestamp" as "long" when it goes out of the Mongo context.
 * - From all the Postgres' date/time data types I stick to "TIMESTAMP WITH TIME ZONE" only.
 */
object MongoTypesMapper {
    private fun unknownType(type: String, field: String): Nothing =
        error("Unknown type '$type' of: $field")

    /**
     * Return a mapping of columns to their types as defined in the collection validator, if any
     */
    fun listValidatorTypes(collectionInfo: Document): Map<String, FieldDefinition>? {
        val jsonSchema = collectionInfo
            .get("options", Document::class.java)
            ?.get("validator", Document::class.java)
            ?.get("\$jsonSchema", Document::class.java)
        val required = jsonSchema
            ?.get("required", List::class.java)
            ?.mapNotNull { it.toString() }
            ?.toSet()
            ?: emptySet()
        return jsonSchema
            ?.get("properties", Document::class.java)
            ?.entries
            ?.associate { p ->
                p.key to when(val type = (p.value as Document)["bsonType"]!!) {
                    is String -> FieldDefinition(p.key, type, p.key !in required)
                    is List<*> -> {
                        check(type.size == 2) {
                            "I expect a list of two types at this point, one of them must be 'null'," +
                                " but got $type for field '${p.key}'"
                        }
                        val intendedType = type.filterNotNull().single { it != "null" }.toString()
                        FieldDefinition(p.key, intendedType, true)
                    }
                    else -> unknownType(type::class.simpleName!!, p.key)
                }
            }
    }

    fun listDocumentTypes(document: BsonDocument): Map<String, FieldDefinition> {
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
            key to FieldDefinition(key, type, false)
        }
    }

    /**
     * Convert a map of field-value pairs as it comes out of [ObjectMapper.readValue] to a [BsonDocument]
     */
    fun Map<String, Any>.asBsonDocument(
        typeHints: Map<String, FieldDefinition>,
        dateTimeFormatter: DateTimeFormatter
    ): BsonDocument {
        return this.entries.map { field ->
            val fieldMeta = typeHints[field.key]
                ?: error("Could not find field '${field.key}' among defined in the schema: ${typeHints.keys}")
            val value = convert2BsonValue(fieldMeta, field.value, dateTimeFormatter)
            BsonElement(field.key, value)
        }.let { BsonDocument(it) }
    }

    /**
     * See [BSON Types](https://www.mongodb.com/docs/manual/reference/bson-types/)
     */
    @Suppress("MagicNumber")
    private fun convert2BsonValue(
        fieldMeta: FieldDefinition,
        value: Any,
        dateTimeFormatter: DateTimeFormatter
    ): BsonValue {
        return when (fieldMeta.type) {
            "double" -> BsonDouble((value as Number).toDouble())
            "string" -> BsonString(value.toString())
            "binData" -> BsonBinary(
                org.dbunit.util.Base64.decode(
                    value.toString()
                )
            )
            "bool" -> BsonBoolean(value as Boolean)
            "date" -> {
                val dateTime = ZonedDateTime.parse(value.toString(), dateTimeFormatter)
                BsonDateTime(dateTime.toInstant().toEpochMilli())
            }
            "timestamp" -> BsonTimestamp((value as Number).toLong())
            "int" -> BsonInt32((value as Number).toInt())
            "long" -> BsonInt64((value as Number).toLong())

            else -> unknownType(fieldMeta.type, fieldMeta.name)
        }
    }

    /**
     * Convert vanilla data types to those accepted by Mongo
     */
    fun Map<String, Any?>.asBsonDocument(): BsonDocument {
        return this.entries.map { field ->
            val value = convert2BsonValue(field.key, field.value)
            BsonElement(field.key, value)
        }.let { BsonDocument(it) }
    }

    private fun convert2BsonValue(
        name: String,
        value: Any?
    ): BsonValue {
        return when (value) {
            null -> BsonNull()
            is Double, is Float -> BsonDouble(value.toDouble())
            is Int, is Short, is Byte -> BsonInt32(value.toInt())
            is Long -> BsonInt64(value )
            is String, is UUID -> BsonString(value.toString())
            is Boolean -> BsonBoolean(value)
            is Timestamp -> BsonDateTime(value.toInstant().toEpochMilli())
            is Date -> BsonDateTime(value.toInstant().toEpochMilli())
            is ZonedDateTime -> BsonDateTime(value.toInstant().toEpochMilli())
            is ByteArray -> BsonBinary(value)
            is List<*> -> BsonArray(value.map { convert2BsonValue(name, it!!) })
            is Array<*> -> BsonArray(value.map { convert2BsonValue(name, it!!) })

            else -> unknownType(value::class.qualifiedName!!, name)
        }
    }

    private fun convert2JdbcValue(name: String, value: Any?): Any? =
        when (value) {
            is Date -> Timestamp(value.toInstant().toEpochMilli())
            is Binary -> value.data
            is BsonTimestamp -> value.value // As this's an internal Mongo type, let's turn it into long
            is BsonValue -> unknownType(value::class.qualifiedName!!,  name) // Not all BSON types covered
            else -> value
        }

    /**
     * Convert data types returned by the Mongo driver to those accepted by /jdbc/ Postgres
     */
    fun Document.asNormalizedMap(): Map<String, Any?> =
        this.entries.associate {
            it.key to convert2JdbcValue(it.key, it.value)
        }
}
