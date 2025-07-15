package org.softlab.dataset.jdbc

import com.github.database.rider.core.exception.DataBaseSeedingException
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.database.core.PostgresDatabase
import liquibase.resource.ClassLoaderResourceAccessor
import org.dbunit.assertion.DbComparisonFailure
import org.hamcrest.CoreMatchers.containsString
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.lang.Thread.sleep
import java.sql.Connection
import java.sql.DriverManager


/**
 * These tests are relying on a PostgreSQL instance, but other JDBC/SQL databases should work as well
 */
class JdbcDatasetLoaderTest {
    companion object {
        private val postgres = PostgreSQLContainer(DockerImageName.parse("postgres:latest"))

        private fun getConnection(): Connection =
            DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)

        @BeforeAll
        @JvmStatic
        fun setup() {
            postgres.start()
            // Workaround for Rancher Desktop on Mac, somehow the container is not ready while the tests start
            val isMac = System.getProperty("os.name").contains("Mac", ignoreCase = true)
            if (isMac) sleep(3000) // Wait for the container to be fully ready

            val database = DatabaseFactory.getInstance()
                .openDatabase(
                    postgres.jdbcUrl,
                    postgres.username,
                    postgres.password,
                    null,
                    null
                ) as PostgresDatabase
            val liquibase = Liquibase(
                "liquibase/changelog-postgres.yaml",
                ClassLoaderResourceAccessor(),
                database
            )
            liquibase.update()
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            postgres.stop()
        }
    }

    @Test
    fun `load() loads dataset correctly`() {
        getConnection().use {
            val loader = JdbcDatasetLoader(it)
            loader.load("datasets/test-dataset.yml")
            loader.compare("datasets/test-dataset.yml")
        }
    }

    @Test
    fun `load() fails without clean on second try`() {
        getConnection().use {
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
        getConnection().use {
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
        getConnection().use {
            val loader = JdbcDatasetLoader(it)
            loader.load("datasets/test-dataset.yml")
            loader.compare("datasets/test-dataset-incorrect-expected.yml", "bool_column", "long_column")
        }
    }
}

