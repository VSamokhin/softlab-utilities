package org.softlab.dataset.mongo.coroutine

import com.mongodb.kotlin.client.coroutine.MongoClient
import com.mongodb.kotlin.client.coroutine.MongoDatabase
import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers


@Testcontainers
class CoroutineMongoCollectionTest {
    companion object {
        @Container
        val mongoContainer = MongoDBContainer("mongo:latest")

        private lateinit var mongoClient: MongoClient
        private lateinit var mongoDb: MongoDatabase

        @BeforeAll
        @JvmStatic
        fun setup() {
            mongoContainer.start()
            mongoClient = MongoClient.create(mongoContainer.replicaSetUrl)
            mongoDb = runBlocking { mongoClient.getDatabase("testdb") }
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            mongoClient.close()
            mongoContainer.stop()
        }
    }

    @BeforeEach
    fun seedData() {
        runBlocking {
            mongoDb.getCollection<Document>("users").drop()
            mongoDb.getCollection<Document>("cities").drop()
        }
        runBlocking {
            mongoDb.getCollection<Document>("users").insertMany(listOf(
                Document(mapOf("_id" to 1, "name" to "Alice", "age" to 30)),
                Document(mapOf("_id" to 2, "name" to "Bob", "age" to 25))
            ))

            mongoDb.getCollection<Document>("cities").insertOne(
                Document(mapOf("_id" to 1, "city" to "Munich", "population" to 42))
            )
        }
    }

    @Test
    fun `name returns a correct name`() {
        val db = CoroutineMongoDatabase(mongoDb)

        assertEquals("users", db.getCollection("users").name)
        assertEquals("cities", db.getCollection("cities").name)
    }

    @Test
    fun `deleteAll() drops all documents in a collection`() {
        val cut = CoroutineMongoDatabase(mongoDb)

        val actualUsers = mongoDb.getCollection<Document>("users")
        assertEquals(2, runBlocking { actualUsers.countDocuments() })
        cut.getCollection("users").deleteAll()
        assertEquals(0, runBlocking { actualUsers.countDocuments() })

        val actualCities = mongoDb.getCollection<Document>("cities")
        assertEquals(1, runBlocking { actualCities.countDocuments() })
        cut.getCollection("cities").deleteAll()
        assertEquals(0, runBlocking { actualCities.countDocuments() })
    }

    @Test
    fun `insertMany() inserts several documents into collection`() {
        val cut = CoroutineMongoDatabase(mongoDb)

        val actualUsers = mongoDb.getCollection<Document>("users")
        assertEquals(2, runBlocking { actualUsers.countDocuments() })
        cut.getCollection("users").insertMany(
            listOf(Document(mapOf("_id" to 3, "name" to "Viktor", "age" to 42)).toBsonDocument())
        )
        assertEquals(3, runBlocking { actualUsers.countDocuments() })

        val actualCities = mongoDb.getCollection<Document>("cities")
        assertEquals(1, runBlocking { actualCities.countDocuments() })
        cut.getCollection("cities").insertMany(listOf(
            Document(mapOf("_id" to 2, "city" to "Kyiv", "population" to 100500)).toBsonDocument(),
            Document(mapOf("_id" to 3, "city" to "Berlin", "population" to 500100)).toBsonDocument()
        ))
        assertEquals(3, runBlocking { actualCities.countDocuments() })
    }
}
