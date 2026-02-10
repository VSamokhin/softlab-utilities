package org.softlab.datatransfer.adapters.mongo

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.softlab.datataset.test.initiators.MongoInitiator
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import kotlin.test.assertContains
import kotlin.test.assertNotNull


@Testcontainers
class MongoSourceTest {
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
            mongoContainer.close()
        }
    }

    @BeforeEach
    fun seedData() {
        mongoInitiator.seedData("datasets/users-products.yml")
    }

    @Test
    fun `listCollections() returns expected collections`() = runBlocking {
        MongoSource(mongoInitiator.dbUrl). use { cut ->
            val collections = cut.listCollections().toList()

            assertThat(
                collections.map { it.metadata.name },
                containsInAnyOrder("schema1.users", "schema2.products",
                "DATABASECHANGELOG", "DATABASECHANGELOGLOCK")
            )

            val usersCollection = collections.first { it.metadata.name == "schema1.users" }
            assertThat(
                usersCollection.metadata.fields.map { it.name }.toList(),
                containsInAnyOrder("_id", "user_id", "name", "email")
            )

            val productsCollection = collections.first{ it.metadata.name == "schema2.products" }
            assertThat(
                productsCollection.metadata.fields.map { it.name }.toSet(),
                containsInAnyOrder("_id", "product_id", "title")
            )
        }
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

    @Test
    fun `constructor fails with wrong connection string`() {
        val exc = assertThrows<IllegalArgumentException> {
            MongoSource("invalid://connection:string")
        }
        assertContains(exc.message!!, "connection string is invalid")
    }
}
