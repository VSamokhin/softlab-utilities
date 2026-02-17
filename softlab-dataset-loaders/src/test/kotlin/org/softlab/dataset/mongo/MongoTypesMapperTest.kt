package org.softlab.dataset.mongo

import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDecimal128
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonNull
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.BsonSymbol
import org.bson.BsonTimestamp
import org.bson.BsonValue
import org.bson.Document
import org.bson.types.Decimal128
import org.dbunit.util.Base64
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.containsString
import org.hamcrest.collection.IsMapContaining.hasEntry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.softlab.dataset.mongo.MongoTypesMapper.asBsonDocument
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Date


class MongoTypesMapperTest {
    companion object {
        @JvmStatic
        fun bsonDocumentCases(): List<Arguments> {
            val zonedDateTime = ZonedDateTime.parse(
                "2023-10-01T12:00:00Z",
                DateTimeFormatter.ISO_ZONED_DATE_TIME
            )
            return listOf(
                Arguments.of("double_field", "double", 12.25, BsonDouble(12.25)),
                Arguments.of("double_field", "double", 12.25F, BsonDouble(12.25)),
                Arguments.of("double_field", "double", 12, BsonDouble(12.0)),
                Arguments.of("string_field", "string", "hello", BsonString("hello")),
                Arguments.of(
                    "bin_field",
                    "binData",
                    "aGVsbG8=",
                    BsonBinary(Base64.decode("aGVsbG8="))
                ),
                Arguments.of("bool_field", "bool", true, BsonBoolean(true)),
                Arguments.of(
                    "date_field", "date", zonedDateTime,
                    BsonDateTime(zonedDateTime.toInstant().toEpochMilli())
                ),
                Arguments.of(
                    "date_field", "date", "2023-10-01T12:00:00Z",
                    BsonDateTime(zonedDateTime.toInstant().toEpochMilli())
                ),
                Arguments.of("int_field", "int", 7, BsonInt32(7)),
                Arguments.of("int_field", "int", 7.0, BsonInt32(7)),
                Arguments.of("long_field", "long", 42L, BsonInt64(42L)),
                Arguments.of("long_field", "long", 42, BsonInt64(42L)),
                Arguments.of(
                    "nullable_string",
                    listOf("string", "null"),
                    "value",
                    BsonString("value")
                )
            )
        }
    }

    @Test
    fun `listValidatorTypes() infers types from validator`() {
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
    fun `listDocumentTypes() infers types from document values`() {
        val doc = Document(
            mapOf(
                "double_field" to BsonDouble(1.5),
                "string_field" to BsonString("Alice"),
                "symbol_field" to BsonSymbol("sym"),
                "id" to BsonObjectId(),
                "binData_field" to BsonBinary(byteArrayOf(1, 2)),
                "bool_field" to BsonBoolean(true),
                "date_field" to BsonDateTime(0),
                "int_field" to BsonInt32(30),
                "long_field" to BsonInt64(100L),
                "object_field" to Document(mapOf("k" to BsonString("v"))).toBsonDocument(),
                "array_field" to BsonArray(listOf(BsonString("a"), BsonString("b"))),
                "decimal_field" to BsonDecimal128(Decimal128(128)),
                "timestamp_field" to BsonTimestamp(1000)
            )
        ).toBsonDocument()

        val types = MongoTypesMapper.listDocumentTypes(doc)

        assertThat(
            types,
            allOf(
                hasEntry("double_field", "double"),
                hasEntry("string_field", "string"),
                hasEntry("symbol_field", "string"),
                hasEntry("id", "string"),
                hasEntry("binData_field", "binData"),
                hasEntry("bool_field", "bool"),
                hasEntry("date_field", "date"),
                hasEntry("int_field", "int"),
                hasEntry("long_field", "long"),
                hasEntry("object_field", "object"),
                hasEntry("array_field", "array"),
                hasEntry("decimal_field", "decimal"),
                hasEntry("timestamp_field", "timestamp")
            )
        )
    }

    @Test
    fun `listDocumentTypes() throws for unsupported value`() {
        val doc = Document(
            mapOf(
                "unknown_type" to BsonNull(),
            )
        ).toBsonDocument()

        val exception = assertThrows<IllegalStateException> {
            MongoTypesMapper.listDocumentTypes(doc)
        }

        assertThat(exception.message, containsString("unknown_type"))
    }

    @ParameterizedTest
    @MethodSource("bsonDocumentCases")
    fun `asBsonDocument(typeHints,dateTimeFormatter) converts values using schema types`(
        fieldName: String,
        fieldType: Any,
        fieldValue: Any,
        expected: BsonValue
    ) {
        val fieldTypes = mapOf(fieldName to fieldType)
        val fieldValues = mapOf(fieldName to fieldValue)

        val actual = fieldValues.asBsonDocument(fieldTypes, DateTimeFormatter.ISO_ZONED_DATE_TIME)

        assertEquals(expected, actual[fieldName])
    }

    @Test
    fun `asBsonDocument(typeHints,dateTimeFormatter) throws when field is missing from schema`() {
        val fieldTypes = mapOf("known_filed" to "string")
        val fieldValues = mapOf("unknown_field" to "value")

        val exception = assertThrows<IllegalStateException> {
            fieldValues.asBsonDocument(fieldTypes, DateTimeFormatter.ISO_ZONED_DATE_TIME)
        }

        assertThat(exception.message, containsString("unknown_field"))
    }

    @Test
    fun `asBsonDocument(typeHints,dateTimeFormatter) throws for unsupported type`() {
        val fieldTypes = mapOf("amount" to "decimal")
        val fieldValues = mapOf("amount" to "1.00")

        val exception = assertThrows<IllegalStateException> {
            fieldValues.asBsonDocument(fieldTypes, DateTimeFormatter.ISO_ZONED_DATE_TIME)
        }

        assertThat(exception.message, containsString("decimal"))
    }

    @Test
    fun `asBsonDocument() converts supported types`() {
        val values = mapOf(
            "double_field" to 1.5,
            "float_field" to 2.5f,
            "int_field" to 1,
            "short_field" to 2.toShort(),
            "byte_field" to 3.toByte(),
            "long_field" to 2L,
            "string_field" to "text",
            "bool_field" to false,
            "date_field" to Date(10),
            "date_zone_field" to ZonedDateTime.ofInstant(Date(100).toInstant(), ZoneId.of("Z")),
            "binary_field" to byteArrayOf(1, 2, 3),
            "list_field" to listOf(1, 2, 3),
            "array_field" to arrayOf("a", "b", "c")
        )

        val actual = values.asBsonDocument()

        assertEquals(BsonDouble(1.5), actual["double_field"])
        assertEquals(BsonDouble(2.5), actual["float_field"])
        assertEquals(BsonInt32(1), actual["int_field"])
        assertEquals(BsonInt32(2), actual["short_field"])
        assertEquals(BsonInt32(3), actual["byte_field"])
        assertEquals(BsonInt64(2L), actual["long_field"])
        assertEquals(BsonString("text"), actual["string_field"])
        assertEquals(BsonBoolean(false), actual["bool_field"])
        assertEquals(BsonDateTime(10), actual["date_field"])
        assertEquals(BsonDateTime(100), actual["date_zone_field"])
        assertEquals(BsonBinary(byteArrayOf(1, 2, 3)), actual["binary_field"])
        assertEquals(BsonArray(listOf(
            BsonInt32(1), BsonInt32(2), BsonInt32(3))),
            actual["list_field"]
        )
        assertEquals(BsonArray(listOf(
            BsonString("a"), BsonString("b"), BsonString("c"))),
            actual["array_field"]
        )
    }

    @Test
    fun `asBsonDocument() throws for unsupported value`() {
        val value: Map<String, Any> = mapOf("unsupported_field" to Any())

        val exception = assertThrows<IllegalStateException> {
            value.asBsonDocument()
        }

        assertThat(exception.message, containsString("unsupported_field"))
    }
}
