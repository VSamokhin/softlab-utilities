package org.softlab.datatransfer.adapters.mongo

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.softlab.datataset.test.initiators.MongoInitiator
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import kotlin.test.assertNotNull


@Testcontainers
class MongoDocumentCollectionTest {
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
            mongoInitiator.initSchema("liquibase/changelog-source-mongo.yaml")
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            mongoInitiator.close()
        }
    }

    @BeforeEach
    fun seedData() {
        mongoInitiator.seedData("datasets/users-products.yml")
    }

    @Test
    fun `readDocuments() returns expected documents`() = runBlocking {
        MongoSource(mongoInitiator.dbUrl).use { cut ->
            val collections = cut.listCollections().toList()

            val usersCollection = collections.firstOrNull { it.metadata.name == "schema1.users" }
            assertNotNull(usersCollection)
            val users = usersCollection.readDocuments().toList()
            assertThat(users.map { it["name"] }.toList(), containsInAnyOrder("Alice", "Bob"))
            assertThat(users.map { it["email"] }.toList(), containsInAnyOrder("alice@example.com", "bob@example.com"))

            val productsCollection = collections.firstOrNull { it.metadata.name == "schema2.products" }
            assertNotNull(productsCollection)
            val products = productsCollection.readDocuments().toList()
            assertThat(products.map { it["title"] }.toList(), containsInAnyOrder("Gizmo"))
        }
    }
}
