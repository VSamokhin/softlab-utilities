package org.softlab.datatransfer.adapters.postgres

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.softlab.datataset.test.initiators.JdbcInitiator
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.lang.Thread.sleep
import kotlin.test.assertFalse
import kotlin.test.assertTrue


@Testcontainers
class PostgresHelperTest {
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
    fun `tableExists() returns true when table exists`() {
        postgresInitiator.getConnection().use { connection ->
            assertTrue(PostgresHelper.tableExists("schema1", "users", connection))
            assertTrue(PostgresHelper.tableExists("schema2", "products", connection))
        }
    }

    @Test
    fun `tableExists() returns false when table does not exist`() {
        postgresInitiator.getConnection().use { connection ->
            assertFalse(PostgresHelper.tableExists("schema1", "non_existent", connection))
            assertFalse(PostgresHelper.tableExists("schema2", "users", connection))
        }
    }

    @Test
    fun `readColumns() returns expected fields for existing table`() {
        postgresInitiator.getConnection().use { connection ->
            val fields = PostgresHelper.readColumns("schema1", "users", connection)

            assertThat(fields.map { it.name }, containsInAnyOrder("user_id", "name", "email"))
            assertThat(
                fields.map { it.type.lowercase() },
                containsInAnyOrder("integer", "character varying", "character varying")
            )
        }
    }

    @Test
    fun `readColumns() returns empty list for absent table`() {
        postgresInitiator.getConnection().use { connection ->
            assertTrue(PostgresHelper.readColumns("schema1", "non_existent", connection).isEmpty())
        }
    }

    @Test
    fun `readColumns() returns empty list for table with no columns`() {
        postgresInitiator.getConnection().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE schema1.no_columns ();")
            }

            assertTrue(PostgresHelper.readColumns("schema1", "no_columns", connection).isEmpty())
        }
    }
}
