package org.softlab.datatransfer.adapters.mongo

import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.bson.BsonDocument
import org.bson.Document
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.softlab.datataset.test.initiators.MongoInitiator
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.dataset.core.FieldDefinition
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer


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
                    listOf(FieldDefinition("id", "string")))
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
}
