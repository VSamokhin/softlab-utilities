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
import org.softlab.datataset.test.initiators.createMongoContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertEquals


@Testcontainers
class MongoSourceIT {
    companion object {
        private const val DATABASE = "testdb"

        @Container
        @JvmStatic
        private val mongoContainer = createMongoContainer()

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
    fun `countDocuments() returns correct count`() = runBlocking {
        MongoSource(mongoInitiator.dbUrl).use { cut ->
            assertEquals(2, cut.countDocuments("schema1.users"))
            assertEquals(1, cut.countDocuments("schema2.products"))
        }
    }

    @Test
    fun `getBackendName() returns mongo`() {
        MongoSource(mongoInitiator.dbUrl).use { cut ->
            assertEquals("mongo", cut.getBackendName())
        }
    }

    @Test
    fun `listCollections() returns expected collections`() = runBlocking {
        MongoSource(mongoInitiator.dbUrl).use { cut ->
            val collections = cut.listCollections().toList()

            assertThat(
                collections.map { it.fetchMetadata().name },
                containsInAnyOrder(
                    "schema1.users",
                    "schema2.products",
                    "DATABASECHANGELOG",
                    "DATABASECHANGELOGLOCK"
                )
            )

            val usersCollection = collections.first { it.fetchMetadata().name == "schema1.users" }
            assertThat(
                usersCollection.fetchMetadata().fields.map { it.name }.toList(),
                containsInAnyOrder("user_id", "name", "email")
            )

            val productsCollection = collections.first { it.fetchMetadata().name == "schema2.products" }
            assertThat(
                productsCollection.fetchMetadata().fields.map { it.name }.toSet(),
                containsInAnyOrder("product_id", "title")
            )
        }
    }
}
