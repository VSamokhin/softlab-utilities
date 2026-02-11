package org.softlab.dataset.mongo

import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.containsString
import org.hamcrest.collection.IsMapContaining.hasEntry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.mongo.MongoTypesMapper.asBsonDocument
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Date
import java.util.Locale


class MongoTypesMapperTest {
    @Test
    fun `listValidatorTypes() returns bson types from schema`() {
        val schema = Document(
            "options",
            Document(
                "validator",
                Document(
                    "\$jsonSchema",
                    Document(
                        "properties",
                        Document(
                            mapOf(
                                "name" to Document("bsonType", "string"),
                                "active" to Document("bsonType", "bool"),
                                "score" to Document("bsonType", arrayOf("double", "null"))
                            )
                        )
                    )
                )
            )
        )

        assertThat(MongoTypesMapper.listValidatorTypes(schema),
            allOf(
                hasEntry("name", "string"),
                hasEntry("active", "bool"),
                hasEntry("score", arrayOf("double", "null"))
            )
        )
    }

    @Test
    fun `asBsonDocument() converts values using schema types`() {
        val fieldTypes = mapOf(
            "double_field" to "double",
            "string_field" to "string",
            "bin_field" to "binData",
            "bool_field" to "bool",
            "date_field" to "date",
            "int_field" to "int",
            "long_field" to "long",
            "nullable_string" to listOf("string", "null")
        )
        val fieldValues = mapOf(
            "double_field" to 12.25,
            "string_field" to "hello",
            "bin_field" to "aGVsbG8=",
            "bool_field" to true,
            "date_field" to "2023-10-01T12:00:00Z",
            "int_field" to 7,
            "long_field" to 42L,
            "nullable_string" to "value"
        )
        val formatter = DateTimeFormatter.ISO_ZONED_DATE_TIME

        val actual = fieldValues.asBsonDocument(fieldTypes, formatter)

        assertEquals(BsonDouble(12.25), actual["double_field"])
        assertEquals(BsonString("hello"), actual["string_field"])
        assertTrue(actual["bin_field"] is BsonBinary)
        assertEquals(
            "hello",
            String((actual["bin_field"] as BsonBinary).data)
        )
        assertEquals(BsonBoolean(true), actual["bool_field"])
        assertEquals(
            BsonDateTime(
                ZonedDateTime.parse("2023-10-01T12:00:00Z", formatter).toInstant().toEpochMilli()),
            actual["date_field"]
        )
        assertEquals(BsonInt32(7), actual["int_field"])
        assertEquals(BsonInt64(42L), actual["long_field"])
        assertEquals(BsonString("value"), actual["nullable_string"])
    }

    @Test
    fun `asBsonDocument() throws when field is missing from schema`() {
        val fieldTypes = mapOf("known_filed" to "string")
        val fieldValues = mapOf("unknown_field" to "value")

        val exception = assertThrows<IllegalStateException> {
            fieldValues.asBsonDocument(fieldTypes, DateTimeFormatter.ISO_ZONED_DATE_TIME)
        }

        assertThat(exception.message, containsString("unknown_field"))
    }

    @Test
    fun `asBsonDocument() throws for unsupported type`() {
        val fieldTypes = mapOf("amount" to "decimal")
        val fieldValues = mapOf("amount" to "1.00")

        val exception = assertThrows<IllegalStateException> {
            fieldValues.asBsonDocument(fieldTypes, DateTimeFormatter.ISO_ZONED_DATE_TIME)
        }

        assertThat(exception.message, containsString("decimal"))
    }

    @Test
    fun `asBsonDocument() converts supported types`() {
        val date = Date(0)
        val formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH)
            .withZone(ZoneId.systemDefault())
        val values = mapOf(
            "double_field" to 1.5,
            "float_field" to 2.5f,
            "int_field" to 1,
            "long_field" to 2L,
            "string_field" to "text",
            "bool_field" to false,
            "date_field" to date,
            "binary_field" to byteArrayOf(1, 2, 3)
        )

        val actual = values.asBsonDocument(formatter)

        assertEquals(BsonDouble(1.5), actual["double_field"])
        assertEquals(BsonDouble(2.5), actual["float_field"])
        assertEquals(BsonInt32(1), actual["int_field"])
        assertEquals(BsonInt64(2L), actual["long_field"])
        assertEquals(BsonString("text"), actual["string_field"])
        assertEquals(BsonBoolean(false), actual["bool_field"])
        assertTrue(actual["date_field"] is BsonDateTime)
        assertEquals(BsonBinary(byteArrayOf(1, 2, 3)), actual["binary_field"])
    }

    @Test
    fun `asBsonDocument() throws for unsupported value`() {
        val value: Map<String, Any> = mapOf("unsupported_field" to listOf<Int>())

        val exception = assertThrows<IllegalStateException> {
            value.asBsonDocument(DateTimeFormatter.ISO_ZONED_DATE_TIME)
        }

        assertThat(exception.message, containsString("unsupported_field"))
    }
}
