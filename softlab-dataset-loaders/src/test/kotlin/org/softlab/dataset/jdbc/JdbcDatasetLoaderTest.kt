package org.softlab.dataset.jdbc

import com.github.database.rider.core.exception.DataBaseSeedingException
import org.dbunit.assertion.DbComparisonFailure
import org.hamcrest.CoreMatchers.containsString
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.softlab.datataset.test.initiators.JdbcInitiator
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers


/**
 * These tests are relying on a PostgreSQL instance, but other JDBC/SQL databases should work as well
 */
@Testcontainers
class JdbcDatasetLoaderTest {
    companion object {
        @JvmStatic
        @Container
        private val postgres = createPostgresContainer()

        private lateinit var postgresInitiator: JdbcInitiator

        @BeforeAll
        @JvmStatic
        fun setup() {
            postgresInitiator = JdbcInitiator(postgres.jdbcUrl, postgres.username, postgres.password)
            postgresInitiator.initSchema("liquibase/changelog-postgres.yaml")
        }
    }

    @Test
    fun `load() loads dataset correctly`() {
        postgresInitiator.getConnection().use {
            val loader = JdbcDatasetLoader(it)
            loader.load("datasets/test-dataset.yml")
            loader.compare("datasets/test-dataset.yml")
        }
    }

    @Test
    fun `load() fails without clean on second try`() {
        postgresInitiator.getConnection().use {
            val loader = JdbcDatasetLoader(it)
            loader.load("datasets/test-dataset.yml", true)
            val exc = assertThrows<DataBaseSeedingException> {
                loader.load("datasets/test-dataset-incorrect-expected.yml", false)
            }
            assertThat(exc.cause?.cause?.message, containsString("duplicate key value"))
        }
    }

    @Test
    fun `compare() fails on wrong data`() {
        postgresInitiator.getConnection().use {
            val loader = JdbcDatasetLoader(it)
            loader.load("datasets/test-dataset.yml")
            val exc = assertThrows<DbComparisonFailure> {
                loader.compare("datasets/test-dataset-incorrect-expected.yml")
            }
            assertThat(exc.message, containsString("col=bool_column"))
        }
    }

    @Test
    fun `compare() ignores specified columns on wrong data`() {
        postgresInitiator.getConnection().use {
            val loader = JdbcDatasetLoader(it)
            loader.load("datasets/test-dataset.yml")
            loader.compare("datasets/test-dataset-incorrect-expected.yml", "bool_column", "long_column")
        }
    }
}

