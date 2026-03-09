package org.softlab.datatransfer.adapters

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.softlab.datatransfer.adapters.mongo.MongoDestination
import org.softlab.datatransfer.adapters.mongo.MongoSource
import org.softlab.datatransfer.adapters.postgres.PostgresDestination
import org.softlab.datatransfer.adapters.postgres.PostgresSource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.lang.Thread.sleep
import kotlin.test.assertIs


@Testcontainers
class AdapterProviderTest {
    companion object {
        @Container
        @JvmStatic
        private val postgres = PostgreSQLContainer(DockerImageName.parse("postgres:latest"))

        @Container
        @JvmStatic
        private val mongo = MongoDBContainer("mongo:latest")

        private lateinit var postgresUri: String
        private lateinit var mongoUri: String

        @BeforeAll
        @JvmStatic
        fun setup() {
            // Workaround for Rancher Desktop on Mac, somehow postgres container is not ready while the tests start
            val isMac = System.getProperty("os.name").contains("Mac", ignoreCase = true)
            if (isMac) sleep(3000) // Wait for the container to be ready

            postgresUri = "${postgres.jdbcUrl}&user=${postgres.username}&password=${postgres.password}"
            mongoUri = "${mongo.connectionString}/testdb"
        }
    }

    @Test
    fun `sourceFor() returns postgres source for postgres URI`() {
        AdapterProvider.sourceFor(postgresUri).use { source ->
            assertIs<PostgresSource>(source)
        }
    }

    @Test
    fun `sourceFor() returns mongo source for mongo URI`() {
        AdapterProvider.sourceFor(mongoUri).use { source ->
            assertIs<MongoSource>(source)
        }
    }

    @Test
    fun `sourceFor() throws for unsupported URI`() {
        val exc = assertThrows<IllegalStateException> {
            AdapterProvider.sourceFor("unknown://db")
        }
        assertThat(exc.message, containsString("unknown://db"))
    }

    @Test
    fun `destinationFor() returns postgres destination for postgres URI`() {
        AdapterProvider.destinationFor(postgresUri).use { destination ->
            assertIs<PostgresDestination>(destination)
        }
    }

    @Test
    fun `destinationFor() returns mongo destination for mongo URI`() {
        AdapterProvider.destinationFor(mongoUri).use { destination ->
            assertIs<MongoDestination>(destination)
        }
    }

    @Test
    fun `destinationFor() throws for unsupported URI`() {
        val exc = assertThrows<IllegalStateException> {
            AdapterProvider.destinationFor("unknown://db")
        }
        assertThat(exc.message, containsString("unknown://db"))
    }
}
