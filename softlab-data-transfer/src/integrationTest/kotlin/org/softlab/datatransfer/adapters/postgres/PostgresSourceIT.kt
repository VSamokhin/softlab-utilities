package org.softlab.datatransfer.adapters.postgres

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.contains
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.softlab.datataset.test.initiators.JdbcInitiator
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.softlab.datataset.test.initiators.getPool
import org.softlab.datataset.test.initiators.withTestPool
import org.softlab.datatransfer.core.PrimaryKeyDefinition
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertEquals
import kotlin.test.assertNotNull


@Testcontainers
class PostgresSourceIT {
    companion object {
        @Container
        @JvmStatic
        private val postgres = createPostgresContainer()

        private lateinit var postgresInitiator: JdbcInitiator

        @JvmStatic
        @BeforeAll
        fun setup() {
            postgresInitiator = JdbcInitiator(postgres.jdbcUrl, postgres.username, postgres.password)
            postgresInitiator.initSchema("liquibase/changelog-source-postgres.yaml")
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            postgresInitiator.close()
        }
    }

    @BeforeEach
    fun seedData() {
        postgresInitiator.seedData("datasets/users-products.yml")
    }

    @Test
    fun `class closes provider`() {
        postgresInitiator.withTestPool { provider ->
            PostgresSource(provider).use { cut ->
                runBlocking { cut.countDocuments("schema1.users") }
            }

            assertEquals(true, provider.closed)
        }
    }

    @Test
    fun `countDocuments() returns correct count`() {
        PostgresSource(getPool(postgresInitiator)).use { cut ->
            assertEquals(2, runBlocking { cut.countDocuments("schema1.users") })
            assertEquals(1, runBlocking { cut.countDocuments("schema2.products") })
        }
    }

    @Test
    fun `getBackendName() returns postgres`() {
        PostgresSource(getPool(postgresInitiator)).use { cut ->
            assertEquals("postgres", cut.getBackendName())
        }
    }

    @Test
    fun `listCollections() returns expected collections`() {
        PostgresSource(getPool(postgresInitiator)).use { cut ->
            val collections = runBlocking { cut.listCollections().toList() }

            assertThat(
                collections.map { runBlocking { it.fetchMetadata() }.name },
                containsInAnyOrder(
                    "schema1.users",
                    "schema2.products",
                    "metadata_cases.single_pk",
                    "metadata_cases.composite_pk",
                    "metadata_cases.no_key",
                    "metadata_cases.indexed_users",
                    "public.databasechangelog",
                    "public.databasechangeloglock"
                )
            )

            val usersCollection = collections.first { runBlocking { it.fetchMetadata() }.name == "schema1.users" }
            val usersMetadata = runBlocking { usersCollection.fetchMetadata() }
            assertThat(
                usersMetadata.fields.keys,
                contains("user_id", "name", "email")
            )
            assertThat(
                usersMetadata.fields.map { it.value.type },
                containsInAnyOrder("int4", "varchar", "varchar")
            )
            assertEquals(
                PrimaryKeyDefinition("users_pkey", listOf("user_id")),
                usersMetadata.primaryKey
            )

            val productsCollection = collections.first { runBlocking { it.fetchMetadata() }.name == "schema2.products" }
            val productsMetadata = runBlocking { productsCollection.fetchMetadata() }
            assertThat(
                productsMetadata.fields.keys,
                containsInAnyOrder("product_id", "title")
            )
            assertEquals(
                PrimaryKeyDefinition("products_pkey", listOf("product_id")),
                productsMetadata.primaryKey
            )
        }
    }

    @Test
    fun `readDocuments() returns expected documents`() {
        PostgresSource(getPool(postgresInitiator)).use { cut ->
            val collections = runBlocking { cut.listCollections().toList() }

            val usersCollection = collections.firstOrNull { runBlocking { it.fetchMetadata() }.name == "schema1.users" }
            assertNotNull(usersCollection)
            val users = runBlocking { usersCollection.readDocuments().toList() }
            assertThat(
                users.map { it["name"] }.toList(),
                containsInAnyOrder("Alice", "Bob")
            )
            assertThat(
                users.map { it["email"] }.toList(),
                containsInAnyOrder("alice@example.com", "bob@example.com")
            )

            val productsCollection = collections.firstOrNull {
                runBlocking { it.fetchMetadata() }.name == "schema2.products"
            }
            assertNotNull(productsCollection)
            val products = runBlocking { productsCollection.readDocuments().toList() }
            assertThat(products.map { it["title"] }.toList(), containsInAnyOrder("Gizmo"))
        }
    }
}
