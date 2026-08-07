package org.softlab.datatransfer.adapters.mongo

import com.mongodb.client.model.IndexOptions
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import org.hamcrest.collection.IsIterableContainingInOrder.contains
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.softlab.datataset.test.initiators.MongoInitiator
import org.softlab.datataset.test.initiators.createMongoContainer
import org.softlab.datatransfer.core.IndexDefinition
import org.softlab.datatransfer.core.IndexFieldDefinition
import org.softlab.datatransfer.core.IndexSortOrder
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue


@Testcontainers
class MongoDocumentCollectionIT {
    companion object {
        private const val DATABASE = "testdb"

        @Container
        @JvmStatic
        private val mongo = createMongoContainer()

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

        val cutUsers = MongoDocumentCollection("schema1.users", mongoInitiator.getConnection())
        val users = runBlocking { cutUsers.readDocuments().toList() }
        assertThat(users.map { it["name"] }, contains("Alice", "Bob"))
        assertThat(users.map { it["email"] }, contains("alice@example.com", "bob@example.com"))

        val cutProducts = MongoDocumentCollection("schema2.products", mongoInitiator.getConnection())
        val products = runBlocking { cutProducts.readDocuments().toList() }
        assertThat(products.map { it["title"] }, contains("Gizmo"))
    }

    @Test
    fun `readDocuments() throws for absent table`() {
        val cut = MongoDocumentCollection("schema1.non_existent", mongoInitiator.getConnection())

        val exc = assertThrows<IllegalStateException> { runBlocking { cut.readDocuments().toList() } }
        assertThat(exc.message, containsString("schema1.non_existent"))
    }

    @Test
    fun `fetchMetadata() retrieves metadata from validator when available`() {
        mongoInitiator.initSchema("liquibase/changelog-source-mongo.yaml")
        mongoInitiator.seedData("datasets/users-products.yml")

        val cut = MongoDocumentCollection("schema1.users", mongoInitiator.getConnection())

        val metadata = runBlocking { cut.fetchMetadata() }
        assertThat(
            metadata.fields.keys,
            contains("user_id", "name", "email")
        )
        assertThat(
            metadata.fields.map { it.value.name },
            contains("user_id", "name", "email")
        )
        assertThat(
            metadata.fields.map { it.value.type },
            contains("long", "string", "string")
        )
        assertEquals(metadata, runBlocking { cut.fetchMetadata() })
    }

    @Test
    fun `fetchMetadata() retrieves metadata from a sample doc`() = runBlocking {
        mongoInitiator.getConnection().createCollection("no_validator")
        mongoInitiator.getConnection().getCollection<Document>("no_validator")
            .insertMany(
                listOf(
                    Document(mapOf("some_id" to 1.0, "some_field" to "whatever1")),
                    Document(mapOf("some_id" to 2.0, "some_field" to "whatever2", "some_another_field" to true))
                )
            )
        val cut = MongoDocumentCollection("no_validator", mongoInitiator.getConnection())

        assertThat(
            cut.fetchMetadata().fields.keys,
            contains("_id", "some_id", "some_field") // Only the first document is sampled
        )
        assertThat(
            cut.fetchMetadata().fields.map { it.value.type },
            contains("string", "double", "string")
        )
    }

    @Test
    fun `fetchMetadata() retrieves nothing when no validator and no docs`() = runBlocking {
        mongoInitiator.getConnection().createCollection("no_validator")
        val cut = MongoDocumentCollection("no_validator", mongoInitiator.getConnection())

        assertTrue(cut.fetchMetadata().fields.isEmpty())
    }

    @Test
    fun `fetchMetadata() includes supported secondary indices without id primary key`() = runBlocking {
        mongoInitiator.getConnection().createCollection("indexed_users")
        val collection = mongoInitiator.getConnection().getCollection<Document>("indexed_users")
        collection.createIndex(
            Document(mapOf("email" to 1)),
            IndexOptions().name("indexed_users_email_unique_idx").unique(true)
        )
        collection.createIndex(
            Document(mapOf("tenant_id" to 1, "name" to -1)),
            IndexOptions().name("indexed_users_tenant_name_idx")
        )

        val metadata = MongoDocumentCollection("indexed_users", mongoInitiator.getConnection()).fetchMetadata()

        assertNull(metadata.primaryKey)
        assertContentEquals(
            listOf(
                IndexDefinition(
                    name = "indexed_users_email_unique_idx",
                    fields = listOf(IndexFieldDefinition("email")),
                    unique = true
                ),
                IndexDefinition(
                    name = "indexed_users_tenant_name_idx",
                    fields = listOf(
                        IndexFieldDefinition("tenant_id"),
                        IndexFieldDefinition("name", IndexSortOrder.DESC)
                    )
                )
            ),
            metadata.indices
        )
    }

    @Test
    fun `fetchMetadata() skips unsupported index shapes and options`() = runBlocking {
        mongoInitiator.getConnection().createCollection("unsupported_indices")
        val collection = mongoInitiator.getConnection().getCollection<Document>("unsupported_indices")
        collection.createIndex(
            Document(mapOf("email" to 1)),
            IndexOptions().name("unsupported_indices_email_idx")
        )
        collection.createIndex(
            Document(mapOf("name" to "text")),
            IndexOptions().name("unsupported_indices_name_text_idx")
        )
        collection.createIndex(
            Document(mapOf("active" to 1)),
            IndexOptions().name("unsupported_indices_active_sparse_idx").sparse(true)
        )
        collection.createIndex(
            Document(mapOf("status" to 1)),
            IndexOptions()
                .name("unsupported_indices_status_partial_idx")
                .partialFilterExpression(Document(mapOf("status" to "active")))
        )

        val metadata = MongoDocumentCollection("unsupported_indices", mongoInitiator.getConnection()).fetchMetadata()

        assertContentEquals(
            listOf(IndexDefinition("unsupported_indices_email_idx", listOf(IndexFieldDefinition("email")))),
            metadata.indices
        )
    }

    @Test
    fun `fetchMetadata() throws when collection absent`() {
        val cut = MongoDocumentCollection("non_existent", mongoInitiator.getConnection())

        assertThrows<NoSuchElementException> { runBlocking { cut.fetchMetadata() } }
    }
}
