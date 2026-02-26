package org.softlab.datatransfer.adapters.mongo

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.hamcrest.collection.IsIterableContainingInOrder.contains
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
import kotlin.test.assertTrue


@Testcontainers
class MongoDocumentCollectionTest {
    companion object {
        private const val DATABASE = "testdb"

        @Container
        @JvmStatic
        private val mongo = MongoDBContainer("mongo:latest")

        private lateinit var mongoInitiator: MongoInitiator

        @BeforeAll
        @JvmStatic
        fun setup() {
            mongoInitiator = MongoInitiator("${mongo.connectionString}/$DATABASE")
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            mongoInitiator.close()
        }
    }

    @BeforeEach
    fun seedData() {
        mongoInitiator.cleanup()
    }

    @Test
    fun `readDocuments() returns expected documents`() {
        mongoInitiator.initSchema("liquibase/changelog-source-mongo.yaml")
        mongoInitiator.seedData("datasets/users-products.yml")

        MongoSource(mongoInitiator.dbUrl).use { source ->
            val cutUsers = MongoDocumentCollection("schema1.users", source)
            val users = runBlocking { cutUsers.readDocuments().toList() }
            assertThat(users.map { it["name"] }, containsInAnyOrder("Alice", "Bob"))
            assertThat(users.map { it["email"] }, containsInAnyOrder("alice@example.com", "bob@example.com"))

            val cutProducts = MongoDocumentCollection("schema2.products", source)
            val products = runBlocking { cutProducts.readDocuments().toList() }
            assertThat(products.map { it["title"] }, containsInAnyOrder("Gizmo"))
        }
    }

    @Test
    fun `readDocuments() throws for absent table`() {
        MongoSource(mongoInitiator.dbUrl).use { source ->
            val cut = MongoDocumentCollection("schema1.non_existent", source)

            val exc = assertThrows<IllegalStateException> { runBlocking { cut.readDocuments().toList() } }
            assertContains(exc.message!!, "schema1.non_existent")
        }
    }

    @Test
    fun `fetchMetadata() retrieves metadata from validator when available`() = runBlocking {
        mongoInitiator.initSchema("liquibase/changelog-source-mongo.yaml")
        mongoInitiator.seedData("datasets/users-products.yml")

        MongoSource(mongoInitiator.dbUrl).use { source ->
            val cut = MongoDocumentCollection("schema1.users", source)

            assertThat(
                cut.fetchMetadata().fields.map { it.name },
                contains("user_id", "name", "email")
            )
            assertThat(
                cut.fetchMetadata().fields.map { it.type },
                contains("long", "string", "string")
            )
        }
    }

    @Test
    fun `fetchMetadata() retrieves metadata from a sample doc`() = runBlocking {
        mongoInitiator.mongoDb.createCollection("no_validator")
        mongoInitiator.mongoDb.getCollection<Document>("no_validator")
            .insertMany(listOf(
                Document(mapOf("some_id" to 1.0, "some_field" to "whatever1")),
                Document(mapOf("some_id" to 2.0, "some_field" to "whatever2", "some_another_field" to true))
            ))
        MongoSource(mongoInitiator.dbUrl).use { source ->
            val cut = MongoDocumentCollection("no_validator", source)

            assertThat(
                cut.fetchMetadata().fields.map { it.name },
                contains("_id", "some_id", "some_field") // Only the first document is sampled
            )
            assertThat(
                cut.fetchMetadata().fields.map { it.type },
                contains("string", "double", "string")
            )
        }
    }

    @Test
    fun `fetchMetadata() retrieves nothing when no validator and no docs`() = runBlocking {
        mongoInitiator.mongoDb.createCollection("no_validator")
        MongoSource(mongoInitiator.dbUrl).use { source ->
            val cut = MongoDocumentCollection("no_validator", source)

            assertTrue(cut.fetchMetadata().fields.isEmpty())
        }
    }

    @Test
    fun `fetchMetadata() throws when collection absent`()  {
        MongoSource(mongoInitiator.dbUrl).use { source ->
            val cut = MongoDocumentCollection("non_existent", source)

            assertThrows<NoSuchElementException> { runBlocking { cut.fetchMetadata() } }
        }
    }
}
