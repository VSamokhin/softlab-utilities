package org.softlab.datatransfer.adapters.mongo

import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.bson.BsonDocument
import org.bson.Document
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.hamcrest.Matchers.hasKey
import org.hamcrest.Matchers.not
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.softlab.dataset.core.FieldDefinition
import org.softlab.datataset.test.initiators.MongoInitiator
import org.softlab.datatransfer.core.CollectionMetadata
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue


@Testcontainers
class MongoDestinationTest {
    companion object {
        private const val DATABASE = "testdb"

        @Container
        @JvmStatic
        private val mongoContainer = MongoDBContainer("mongo:latest")

        private lateinit var mongoInitiator: MongoInitiator

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
        MongoDestination(mongoInitiator.dbUrl).use { destination ->
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
    fun `insertDocuments() writes expected data`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl).use { destination ->
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

    @Test
    fun `createCollection() forms expected validator descriptor`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl).use { destination ->
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
        MongoDestination(mongoInitiator.dbUrl).use { destination ->
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
