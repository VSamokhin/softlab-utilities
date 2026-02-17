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
import org.softlab.datataset.test.initiators.JdbcInitiator
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.lang.Thread.sleep
import java.sql.SQLException
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertTrue


@Testcontainers
class PostgresDocumentCollectionTest {
    companion object {
        @Container
        @JvmStatic
        private val postgres = PostgreSQLContainer(DockerImageName.parse("postgres:latest"))

        private lateinit var postgresInitiator: JdbcInitiator

        @BeforeAll
        @JvmStatic
        fun setup() {
            // Workaround for Rancher Desktop on Mac, somehow the container is not ready while the tests start
            val isMac = System.getProperty("os.name").contains("Mac", ignoreCase = true)
            if (isMac) sleep(3000) // Wait for the container to be ready

            postgresInitiator = JdbcInitiator(postgres.jdbcUrl, postgres.username, postgres.password)
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            postgresInitiator.close()
        }
    }

    @BeforeEach
    fun prepareDb() {
        postgresInitiator.cleanup { c ->
            c.createStatement().use { stmt ->
                stmt.execute("DROP SCHEMA IF EXISTS schema1 CASCADE;")
                stmt.execute("DROP SCHEMA IF EXISTS schema2 CASCADE;")
            }
        }
        postgresInitiator.initSchema("liquibase/changelog-source-postgres.yaml")
    }

    @Test
    fun `readDocuments() returns expected documents`() {
        postgresInitiator.seedData("datasets/users-products.yml")

        postgresInitiator.getConnection().use { connection ->
            val usersCollection = PostgresDocumentCollection("schema1", "users", connection)
            val users = runBlocking { usersCollection.readDocuments().toList() }
            assertThat(users.map { it["name"] }.toList(), containsInAnyOrder("Alice", "Bob"))
            assertThat(users.map { it["email"] }.toList(), containsInAnyOrder("alice@example.com", "bob@example.com"))

            val productsCollection = PostgresDocumentCollection("schema2", "products", connection)
            val products = runBlocking { productsCollection.readDocuments().toList() }
            assertThat(products.map { it["title"] }.toList(), containsInAnyOrder("Gizmo"))
        }
    }

    @Test
    fun `readDocuments() throws for absent table`() {
        postgresInitiator.getConnection().use { connection ->
            val cut = PostgresDocumentCollection("schema1", "non_existent", connection)

            assertThrows<SQLException> { runBlocking { cut.readDocuments().toList() } }
        }
    }

    @Test
    fun `fetchMetadata() retrieves expected metadata`() {
        postgresInitiator.seedData("datasets/users-products.yml")

        postgresInitiator.getConnection().use { connection ->
            val usersCollection = PostgresDocumentCollection("schema1", "users", connection)
            val usersMetadata = runBlocking { usersCollection.fetchMetadata() }
            assertEquals("schema1.users", usersMetadata.name)
            assertThat(
                usersMetadata.fields.map { it.name }.toList(),
                containsInAnyOrder("user_id", "name", "email")
            )

            val productsCollection = PostgresDocumentCollection("schema2", "products", connection)
            val productsMetadata = runBlocking { productsCollection.fetchMetadata() }
            assertEquals("schema2.products", productsMetadata.name)
            assertThat(
                productsMetadata.fields.map { it.name }.toSet(),
                containsInAnyOrder("product_id", "title")
            )
        }
    }

    @Test
    fun `fetchMetadata() retrieves nothing when no columns`() {
        postgresInitiator.getConnection().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE schema1.no_columns ();")
            }

            val cut = PostgresDocumentCollection("schema1", "no_columns", connection)

            assertTrue(runBlocking { cut.fetchMetadata().fields.isEmpty() })
        }
    }

    @Test
    fun `fetchMetadata() throws for absent table`()  {
        postgresInitiator.getConnection().use { connection ->
            val cut = PostgresDocumentCollection("schema1", "non_existent", connection)

            val exc = assertThrows<IllegalStateException> { runBlocking { cut.fetchMetadata() } }

            assertContains(exc.message!!, "schema1.non_existent")
        }
    }
}
