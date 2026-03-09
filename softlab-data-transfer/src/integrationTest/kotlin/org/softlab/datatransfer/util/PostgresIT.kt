package org.softlab.datatransfer.util

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.hamcrest.Matchers.hasItem
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.softlab.dataset.core.FieldDefinition
import org.softlab.datataset.test.initiators.JdbcInitiator
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertFalse
import kotlin.test.assertTrue


@Testcontainers
class PostgresIT {
    companion object {
        @Container
        @JvmStatic
        private val postgres = createPostgresContainer()

        private lateinit var postgresInitiator: JdbcInitiator

        @BeforeAll
        @JvmStatic
        fun setup() {
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
            assertTrue(Postgres.tableExists("schema1", "users", connection))
            assertTrue(Postgres.tableExists("schema2", "products", connection))
        }
    }

    @Test
    fun `tableExists() returns false when table does not exist`() {
        postgresInitiator.getConnection().use { connection ->
            assertFalse(Postgres.tableExists("schema1", "non_existent", connection))
            assertFalse(Postgres.tableExists("schema2", "users", connection))
        }
    }

    @Test
    fun `readColumns() returns expected fields for existing table`() {
        postgresInitiator.getConnection().use { connection ->
            val fields = Postgres.readColumns("schema1", "users", connection)

            assertThat(fields.map { it.name }, containsInAnyOrder("user_id", "name", "email"))
            assertThat(
                fields.map { it.type.lowercase() },
                containsInAnyOrder("integer", "character varying", "character varying")
            )
            assertThat(fields, hasItem(FieldDefinition("email", "character varying", true)))
        }
    }

    @Test
    fun `readColumns() returns empty list for absent table`() {
        postgresInitiator.getConnection().use { connection ->
            assertTrue(Postgres.readColumns("schema1", "non_existent", connection).isEmpty())
        }
    }

    @Test
    fun `readColumns() returns empty list for table with no columns`() {
        postgresInitiator.getConnection().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE schema1.no_columns ();")
            }

            assertTrue(Postgres.readColumns("schema1", "no_columns", connection).isEmpty())
        }
    }
}
