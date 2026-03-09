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
import org.bson.types.Binary
import org.bson.types.Decimal128
import org.dbunit.util.Base64
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.equalTo
import org.hamcrest.collection.IsMapContaining.hasEntry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.mongo.MongoTypesMapper.asBsonDocument
import org.softlab.dataset.mongo.MongoTypesMapper.asNormalizedMap
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Date
import java.util.UUID


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
                Arguments.of("timestamp_field", "timestamp", 1234L, BsonTimestamp(1234L)),
                Arguments.of("int_field", "int", 7, BsonInt32(7)),
                Arguments.of("int_field", "int", 7.0, BsonInt32(7)),
                Arguments.of("long_field", "long", 42L, BsonInt64(42L)),
                Arguments.of("long_field", "long", 42, BsonInt64(42L))
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
                                "score" to Document("bsonType", listOf("double", "null"))
                            )
                        )
                    )
                )
            )
        )

        assertThat(MongoTypesMapper.listValidatorTypes(schema),
            allOf(
                hasEntry(equalTo("name"), equalTo(FieldDefinition("name","string", true))),
                hasEntry(equalTo("active"), equalTo(FieldDefinition("active", "bool", true))),
                hasEntry(equalTo("score"), equalTo(FieldDefinition("score", "double", true)))
            )
        )
    }

    @Test
    fun `listValidatorTypes() infers types from validator with required fields`() {
        val schema = Document(
            "options",
            Document(
                "validator",
                Document(
                    "\$jsonSchema",
                    Document(mapOf(
                        "required" to listOf("name", "score"),
                        "properties" to Document(mapOf(
                            "name" to Document("bsonType", "double"),
                            "active" to Document("bsonType", "binData"),
                            "score" to Document("bsonType", listOf("long", "null"))
                        ))
                    ))
                )
            )
        )

        assertThat(MongoTypesMapper.listValidatorTypes(schema),
            allOf(
                hasEntry(equalTo("name"), equalTo(FieldDefinition("name","double", false))),
                hasEntry(equalTo("active"), equalTo(FieldDefinition("active", "binData", true))),
                hasEntry(equalTo("score"), equalTo(FieldDefinition("score", "long", true)))
            )
        )
    }

    @Test
    fun `listValidatorTypes() throws for unexpected bsonType's type`() {
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
                                "wrong_bson_type_field" to Document("bsonType", 123)
                            )
                        )
                    )
                )
            )
        )

        val exc = assertThrows<IllegalStateException> {
            MongoTypesMapper.listValidatorTypes(schema)
        }
        assertThat(exc.message, containsString("wrong_bson_type_field"))
    }

    @Test
    fun `listValidatorTypes() returns null when validator is missing`() {
        val schema = Document("options", Document())
        assertEquals(null, MongoTypesMapper.listValidatorTypes(schema))
    }

    @Test
    fun `listValidatorTypes() throws when bsonType list shape is invalid`() {
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
                                "wrong_list_field" to Document("bsonType", listOf("double", "long", "null"))
                            )
                        )
                    )
                )
            )
        )

        val exc = assertThrows<IllegalStateException> {
            MongoTypesMapper.listValidatorTypes(schema)
        }
        assertThat(exc.message, containsString("wrong_list_field"))
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
                hasEntry(equalTo("double_field"), equalTo(FieldDefinition("double_field", "double"))),
                hasEntry(equalTo("string_field"), equalTo(FieldDefinition("string_field", "string"))),
                hasEntry(equalTo("symbol_field"), equalTo(FieldDefinition("symbol_field", "string"))),
                hasEntry(equalTo("id"), equalTo(FieldDefinition("id", "string"))),
                hasEntry(equalTo("binData_field"), equalTo(FieldDefinition("binData_field", "binData"))),
                hasEntry(equalTo("bool_field"), equalTo(FieldDefinition("bool_field", "bool"))),
                hasEntry(equalTo("date_field"), equalTo(FieldDefinition("date_field", "date"))),
                hasEntry(equalTo("int_field"), equalTo(FieldDefinition("int_field", "int"))),
                hasEntry(equalTo("long_field"), equalTo(FieldDefinition("long_field", "long"))),
                hasEntry(equalTo("object_field"), equalTo(FieldDefinition("object_field", "object"))),
                hasEntry(equalTo("array_field"), equalTo(FieldDefinition("array_field", "array"))),
                hasEntry(equalTo("decimal_field"), equalTo(FieldDefinition("decimal_field", "decimal"))),
                hasEntry(equalTo("timestamp_field"), equalTo(FieldDefinition("timestamp_field", "timestamp")))
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
        fieldType: String,
        fieldValue: Any,
        expected: BsonValue
    ) {
        val fieldTypes = mapOf(fieldName to FieldDefinition(fieldName, fieldType))
        val fieldValues = mapOf(fieldName to fieldValue)

        val actual = fieldValues.asBsonDocument(fieldTypes, DateTimeFormatter.ISO_ZONED_DATE_TIME)

        assertEquals(expected, actual[fieldName])
    }

    @Test
    fun `asBsonDocument(typeHints,dateTimeFormatter) throws when field is missing from schema`() {
        val fieldTypes = mapOf("known_field" to FieldDefinition("known_field", "string"))
        val fieldValues = mapOf("unknown_field" to FieldDefinition("unknown_field", "value"))

        val exception = assertThrows<IllegalStateException> {
            fieldValues.asBsonDocument(fieldTypes, DateTimeFormatter.ISO_ZONED_DATE_TIME)
        }

        assertThat(exception.message, containsString("unknown_field"))
    }

    @Test
    fun `asBsonDocument(typeHints,dateTimeFormatter) throws for unsupported type`() {
        val fieldTypes = mapOf("amount" to FieldDefinition("amount", "decimal"))
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
            "array_field" to arrayOf("a", "b", "c"),
            "null_field" to null,
            "uuid_field" to UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
            "timestamp_field" to java.sql.Timestamp(1234)
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
        assertEquals(BsonNull(), actual["null_field"])
        assertEquals(BsonString("123e4567-e89b-12d3-a456-426614174000"), actual["uuid_field"])
        assertEquals(BsonDateTime(1234), actual["timestamp_field"])
    }

    @Test
    fun `asBsonDocument() throws for unsupported value`() {
        val value: Map<String, Any> = mapOf("unsupported_field" to Any())

        val exception = assertThrows<IllegalStateException> {
            value.asBsonDocument()
        }

        assertThat(exception.message, containsString("unsupported_field"))
    }

    @Test
    fun `asNormalizedMap() throws for unsupported BsonValue`() {
        val source = Document(mapOf("unsupported_bson_value" to BsonNull()))

        val exception = assertThrows<IllegalStateException> {
            source.asNormalizedMap()
        }

        assertThat(exception.message, containsString("unsupported_bson_value"))
    }

    @Test
    fun `asNormalizedMap() converts expected data types`() {
        val source = Document(
            mapOf(
                "binary_field" to Binary(byteArrayOf(1, 2, 3)),
                "ts_field" to BsonTimestamp(12345),
                "plain_field" to "ok",
                "date_field" to Date(54321L)
            )
        )

        val actual = source.asNormalizedMap()

        assertEquals(byteArrayOf(1, 2, 3).toList(), (actual["binary_field"] as ByteArray).toList())
        assertEquals(12345L, actual["ts_field"])
        assertEquals("ok", actual["plain_field"])
        assertEquals(java.sql.Timestamp(54321L), actual["date_field"])
    }
}
