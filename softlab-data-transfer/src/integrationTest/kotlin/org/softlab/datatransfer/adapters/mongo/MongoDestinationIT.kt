package org.softlab.datatransfer.adapters.mongo

import com.mongodb.MongoCommandException
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.bson.BsonDocument
import org.bson.Document
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.hasKey
import org.hamcrest.Matchers.not
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.core.FieldDefinition
import org.softlab.datataset.test.initiators.MongoInitiator
import org.softlab.datataset.test.initiators.createMongoContainer
import org.softlab.datatransfer.config.ConfigProvider
import org.softlab.datatransfer.core.CollectionMetadata
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue


@Testcontainers
class MongoDestinationIT {
    companion object {
        private const val DATABASE = "testdb"

        @Container
        @JvmStatic
        private val mongoContainer = createMongoContainer()

        private lateinit var mongoInitiator: MongoInitiator

        private val dataTypeMappings = ConfigProvider.config.getDataTypeMappings().destination(MongoDestination.BACKEND)

        @BeforeAll
        @JvmStatic
        fun setup() {
            mongoInitiator = MongoInitiator("${mongoContainer.connectionString}/$DATABASE")
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            mongoInitiator.close()
        }
    }

    @BeforeEach
    fun seedData() = runBlocking {
        mongoInitiator.mongoDb.listCollections()
            .map { it.getString("name") }
            .collect { collection ->
                mongoInitiator.mongoDb
                    .getCollection<Document>(collection)
                    .drop()
            }
    }

    @Test
    fun `createCollection() creates expected collection`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(
                CollectionMetadata("new_collection",
                    listOf(
                        FieldDefinition("id", "string"),
                        FieldDefinition("nickname", "string", nullable = true)
                    ))
            )
            destination.createCollection(
                CollectionMetadata("existing", emptyList())
            )
        }

        assertThat(
            mongoInitiator.mongoDb.listCollections().map { it["name"] }.toList(),
            containsInAnyOrder(
                "existing", "new_collection"
            )
        )

        val newValidator = getJsonSchemaValidator("new_collection")
        val newProperties = newValidator.get("properties", Document::class.java)

        assertEquals(listOf("id"), newValidator.getList("required", String::class.java))
        assertEquals("string", newProperties.get("id", Document::class.java).getString("bsonType"))
        assertEquals(
            listOf("string", "null"),
            newProperties.get("nickname", Document::class.java).getList("bsonType", String::class.java)
        )

        val existingCollection = mongoInitiator.mongoDb.listCollections()
            .toList()
            .first { it.getString("name") == "existing" }
        val existingOptions = existingCollection.get("options", Document::class.java)
        assertNotNull(existingOptions)
        assertTrue(existingOptions.isEmpty())
    }

        @Test
    fun `createCollection() forms expected validator descriptor`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(
                CollectionMetadata(
                    "validator_shape",
                    listOf(
                        FieldDefinition("user_id", "long"),
                        FieldDefinition("nickname", "string", nullable = true),
                        FieldDefinition("active", "bool")
                    )
                )
            )
        }

        val validator = getJsonSchemaValidator("validator_shape")
        val expected = Document(
            mapOf(
                "bsonType" to "object",
                "properties" to Document(
                    mapOf(
                        "user_id" to Document(mapOf("bsonType" to "long")),
                        "nickname" to Document(mapOf("bsonType" to listOf("string", "null"))),
                        "active" to Document(mapOf("bsonType" to "bool"))
                    )
                ),
                "required" to listOf("user_id", "active")
            )
        )
        assertEquals(expected, validator)
    }

    @Test
    fun `createCollection() omits required when all fields nullable`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(
                CollectionMetadata(
                    "validator_all_nullable",
                    listOf(
                        FieldDefinition("a", "string", nullable = true),
                        FieldDefinition("b", "double", nullable = true)
                    )
                )
            )
        }

        val validator = getJsonSchemaValidator("validator_all_nullable")
        assertThat(validator, not(hasKey("required")))
    }

    @Test
    fun `createCollection() maps aliases to bson types using config`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(
                CollectionMetadata(
                    "validator_with_aliases",
                    listOf(
                        FieldDefinition("age", "integer"),
                        FieldDefinition("memo", "text", nullable = true),
                        FieldDefinition("enabled", "boolean")
                    )
                )
            )
        }

        val validator = getJsonSchemaValidator("validator_with_aliases")
        val properties = validator.get("properties", Document::class.java)
        assertEquals("int", properties.get("age", Document::class.java).getString("bsonType"))
        assertEquals(
            listOf("string", "null"),
            properties.get("memo", Document::class.java).getList("bsonType", String::class.java)
        )
        assertEquals("bool", properties.get("enabled", Document::class.java).getString("bsonType"))
    }

    @Test
    fun `createCollection() causes mongo to throw for unknow field type`() {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            val col = CollectionMetadata("users",
                listOf(FieldDefinition("notes", "custom"))
            )

            val exc = assertThrows<MongoCommandException> {
                runBlocking { destination.createCollection(col) }
            }
            assertThat(exc.message, containsString("custom"))
        }
    }

    @Test
    fun `createCollection() throws when collection already exists`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            val metadata = CollectionMetadata("users", emptyList())

            destination.createCollection(metadata)

            val exc = assertThrows<IllegalStateException> {
                destination.createCollection(metadata)
            }
            assertThat(exc.message, containsString("users"))
        }
    }

    @Test
    fun `getBackendName() returns mongo`() {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            assertEquals("mongo", destination.getBackendName())
        }
    }

    @Test
    fun `dropCollection() drops collection with all data`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(CollectionMetadata("users", emptyList()))
            destination.insertDocuments(
                "users",
                flowOf(
                    mapOf("user_id" to "1", "name" to "Alice"),
                    mapOf("user_id" to "2", "name" to "Bob")
                )
            )
            destination.dropCollection("users")
        }

        val collectionNames = mongoInitiator.mongoDb.listCollections()
            .map { it.getString("name") }
            .toList()
        assertTrue("users" !in collectionNames)
    }

    @Test
    fun `dropCollection() doesn't fail if collection not exists`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            assertDoesNotThrow { destination.dropCollection("something") }
        }
    }

    @Test
    fun `insertDocuments() writes expected data`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(CollectionMetadata("users", emptyList()))
            destination.insertDocuments(
                "users",
                flowOf(
                    mapOf("user_id" to "1", "name" to "Alice"),
                    mapOf("user_id" to "2", "name" to "Bob")
                )
            )
        }

        val docs = mongoInitiator.mongoDb
            .getCollection<BsonDocument>("users")
            .find()
            .toList()

        assertThat(
            docs.map { it["user_id"]?.asString()?.value }.toList(),
            containsInAnyOrder("1", "2")
        )
        assertThat(
            docs.map { it["name"]?.asString()?.value }.toList(),
            containsInAnyOrder("Alice", "Bob")
        )
    }

    private suspend fun getJsonSchemaValidator(collectionName: String): Document {
        val collectionInfo = mongoInitiator.mongoDb.listCollections()
            .toList()
            .first { it.getString("name") == collectionName }
        return collectionInfo
            .get("options", Document::class.java)
            .get("validator", Document::class.java)
            .get("\$jsonSchema", Document::class.java)
    }
}
