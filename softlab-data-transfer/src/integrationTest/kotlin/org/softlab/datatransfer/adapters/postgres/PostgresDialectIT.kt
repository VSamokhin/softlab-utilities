package org.softlab.datatransfer.adapters.postgres

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.contains
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.softlab.datataset.test.initiators.JdbcInitiator
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.softlab.datatransfer.adapters.jdbc.JdbcTableName
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertFalse
import kotlin.test.assertTrue


@Testcontainers
class PostgresDialectIT {
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
        postgresInitiator.cleanup { connection ->
            connection.createStatement().use { statement ->
                statement.execute("DROP SCHEMA IF EXISTS schema1 CASCADE;")
                statement.execute("DROP SCHEMA IF EXISTS schema2 CASCADE;")
            }
        }
        postgresInitiator.initSchema("liquibase/changelog-source-postgres.yaml")
    }

    @Test
    fun `tableExists() returns table presence`() {
        postgresInitiator.getConnection().use { connection ->
            val dialect = PostgresDialect()

            assertTrue(dialect.tableExists(
                JdbcTableName("schema1", "users"), connection)
            )
            assertFalse(dialect.tableExists(
                JdbcTableName("schema1", "missing"), connection)
            )
        }
    }

    @Test
    fun `listColumns() returns normalized Postgres column metadata`() {
        postgresInitiator.getConnection().use { connection ->
            val fields = PostgresDialect()
                .listColumns(JdbcTableName("schema1", "users"), connection)

            assertThat(
                fields.keys,
                contains("user_id", "name", "email")
            )
            assertThat(
                fields.map { it.value.type },
                contains("int4", "varchar", "varchar")
            )
        }
    }

    @Test
    fun `beforeCreateTable() creates missing schema`() {
        postgresInitiator.getConnection().use { connection ->
            val dialect = PostgresDialect()

            dialect.beforeCreateTable(JdbcTableName("custom", "users"), connection)

            connection.metaData.getSchemas(null, "custom").use { rs ->
                assertTrue(rs.next())
            }
        }
    }
}
