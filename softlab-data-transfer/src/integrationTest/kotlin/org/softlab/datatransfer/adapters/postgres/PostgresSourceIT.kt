package org.softlab.datatransfer.adapters.postgres

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.softlab.datataset.test.initiators.JdbcInitiator
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.softlab.datataset.test.initiators.getProvider
import org.softlab.datataset.test.initiators.withProvider
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
        withProvider(postgresInitiator) { provider ->
            PostgresSource(provider).use { cut ->
                runBlocking { cut.countDocuments("schema1.users") }
            }

            assertEquals(true, provider.closed)
        }
    }

    @Test
    fun `countDocuments() returns correct count`() = runBlocking {
        PostgresSource(getProvider(postgresInitiator)).use { cut ->
            assertEquals(2, cut.countDocuments("schema1.users"))
            assertEquals(1, cut.countDocuments("schema2.products"))
        }
    }

    @Test
    fun `getBackendName() returns postgres`() {
        PostgresSource(getProvider(postgresInitiator)).use { cut ->
            assertEquals("postgres", cut.getBackendName())
        }
    }

    @Test
    fun `listCollections() returns expected collections`() = runBlocking {
        PostgresSource(getProvider(postgresInitiator)).use { cut ->
            val collections = cut.listCollections().toList()

            assertThat(
                collections.map { it.fetchMetadata().name },
                containsInAnyOrder(
                    "schema1.users",
                    "schema2.products",
                    "public.databasechangelog",
                    "public.databasechangeloglock"
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

    @Test
    fun `readDocuments() returns expected documents`() = runBlocking {
        PostgresSource(getProvider(postgresInitiator)).use { cut ->
            val collections = cut.listCollections().toList()

            val usersCollection = collections.firstOrNull { it.fetchMetadata().name == "schema1.users" }
            assertNotNull(usersCollection)
            val users = usersCollection.readDocuments().toList()
            assertThat(
                users.map { it["name"] }.toList(),
                containsInAnyOrder("Alice", "Bob")
            )
            assertThat(
                users.map { it["email"] }.toList(),
                containsInAnyOrder("alice@example.com", "bob@example.com")
            )

            val productsCollection = collections.firstOrNull { it.fetchMetadata().name == "schema2.products" }
            assertNotNull(productsCollection)
            val products = productsCollection.readDocuments().toList()
            assertThat(products.map { it["title"] }.toList(), containsInAnyOrder("Gizmo"))
        }
    }
}
