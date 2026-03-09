package org.softlab.datatransfer.adapters.postgres

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.softlab.datataset.test.initiators.JdbcInitiator
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.SQLException
import kotlin.test.assertEquals
import kotlin.test.assertNotNull


@Testcontainers
class PostgresSourceTest {
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

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `class closes connection when configured`(closeConnection: Boolean) = runBlocking {
        postgresInitiator.getConnection().use { connection ->
            PostgresSource(connection, closeConnection).use { cut ->
                cut.countDocuments("schema1.users")
            }
            assertEquals(closeConnection, connection.isClosed)
        }
    }

    @Test
    fun `countDocuments() returns correct count`() = runBlocking {
        PostgresSource(postgresInitiator.getConnection()).use { cut ->
            assertEquals(2, cut.countDocuments("schema1.users"))
            assertEquals(1, cut.countDocuments("schema2.products"))
        }
    }

    @Test
    fun `getBackendName() returns postgres`() {
        PostgresSource(postgresInitiator.getConnection()).use { cut ->
            assertEquals("postgres", cut.getBackendName())
        }
    }

    @Test
    fun `listCollections() returns expected collections`() = runBlocking {
        PostgresSource(postgresInitiator.getConnection()).use { cut ->
            val collections = cut.listCollections().toList()

            assertThat(
                collections.map { it.fetchMetadata().name },
                containsInAnyOrder("schema1.users", "schema2.products",
                "public.databasechangelog", "public.databasechangeloglock")
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
        PostgresSource(postgresInitiator.getConnection()).use { cut ->
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

    @Test
    fun `test invalid connection string throws exception`() {
        val exc = assertThrows<SQLException> {
            PostgresSource("invalid://connection:string", "user", "pass")
        }
        assertEquals("08001", exc.sqlState)
    }
}
