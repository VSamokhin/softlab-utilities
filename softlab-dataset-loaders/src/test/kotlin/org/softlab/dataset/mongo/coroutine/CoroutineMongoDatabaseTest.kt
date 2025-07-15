package org.softlab.dataset.mongo.coroutine

import com.mongodb.kotlin.client.coroutine.MongoClient
import com.mongodb.kotlin.client.coroutine.MongoDatabase
import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertIterableEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers


@Testcontainers
class CoroutineMongoDatabaseTest {
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
            // Seed data to make the collection appear
            runBlocking {
                mongoDb.getCollection<Document>("users").insertOne(
                    Document(mapOf("_id" to 1, "name" to "Alice", "age" to 30))
                )
            }
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            mongoClient.close()
            mongoContainer.stop()
        }
    }

    @Test
    fun `getCollection() returns an initialized facade`() {
        val cut = CoroutineMongoDatabase(mongoDb)
        val users = cut.getCollection("users")

        assertEquals("users", users.name)
    }

    @Test
    fun `listCollections() returns all collections`() {
        val cut = CoroutineMongoDatabase(mongoDb)
        val collections = cut.listCollections()

        assertIterableEquals(listOf("users"), collections.map { it["name"] })
    }
}
