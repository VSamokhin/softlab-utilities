package org.softlab.datatransfer.migration

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.softlab.dataset.core.FieldDefinition
import org.softlab.datatransfer.adapters.mongo.MongoDestination
import org.softlab.datatransfer.adapters.mongo.MongoSource
import org.softlab.datatransfer.adapters.postgres.PostgresDestination
import org.softlab.datatransfer.adapters.postgres.PostgresSource
import org.softlab.datatransfer.config.ConfigProvider
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.core.DocumentCollection
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.lang.Thread.sleep


@Testcontainers
class MigratorTest {
    companion object {
        private const val DATABASE = "testdb"

        private const val NUM_RECORDS = 10_000

        @Container
        @JvmStatic
        private val mongo = MongoDBContainer("mongo:latest")

        @Container
        @JvmStatic
        private val postgres = PostgreSQLContainer(DockerImageName.parse("postgres:latest"))

        private val mongoTypes = ConfigProvider.config
            .getDataTypeMappings()
            .destination(MongoDestination.BACKEND)
        private val postgresTypes = ConfigProvider.config
            .getDataTypeMappings()
            .destination(PostgresDestination.BACKEND)

        @BeforeAll
        @JvmStatic
        fun setup() {
            // Workaround for Rancher Desktop on Mac, somehow the container is not ready while the tests start
            val isMac = System.getProperty("os.name").contains("Mac", ignoreCase = true)
            if (isMac) sleep(3000) // Wait for the container to be ready
        }

        @JvmStatic
        fun getTargetDbs(): List<Arguments> {
            return listOf(
                Arguments.of(
                    MongoDestination(mongo.connectionString, mongoTypes, DATABASE),
                    MongoSource(mongo.connectionString, databaseName = DATABASE)
                ),
                Arguments.of(
                    PostgresDestination(
                        "${postgres.jdbcUrl}/${DATABASE}",
                        postgres.username,
                        postgres.password,
                        postgresTypes
                    ),
                    PostgresSource("${postgres.jdbcUrl}/${DATABASE}", postgres.username, postgres.password)
                )
            )
        }
    }

    @Test
    fun `migrate() copies data from source to destination`() {
        val mockSource = mockk<DatabaseSource>()
        val mockDest = mockk<DatabaseDestination>()
        val mockCollection = mockk<DocumentCollection>()

        val testMetadata = CollectionMetadata(
            name = "users",
            fields = listOf(
                FieldDefinition("id", "string"),
                FieldDefinition("name", "string")
            )
        )

        val testDocuments = flowOf(
            mapOf("id" to "1", "name" to "Test User 1"),
            mapOf("id" to "2", "name" to "Test User 2")
        )

        coEvery { mockCollection.fetchMetadata() } returns testMetadata
        every { mockCollection.readDocuments() } returns testDocuments
        every { mockSource.listCollections() } returns flowOf(mockCollection)
        coEvery { mockDest.createCollection(eq(testMetadata)) } returns Unit
        coEvery { mockDest.insertDocuments(eq("users"), eq(testDocuments)) } returns Unit

        runBlocking { Migrator().migrate(mockSource, mockDest) }

        verify(exactly = 1) { mockSource.listCollections() }
        coVerify(exactly = 1) { mockCollection.fetchMetadata() }
        verify(exactly = 1) { mockCollection.readDocuments() }
        coVerify(exactly = 1) { mockDest.createCollection(testMetadata) }
        coVerify(exactly = 1) { mockDest.insertDocuments(testMetadata.name, any()) }
    }

    @Test
    fun `migrate() works correctly with empty source`() {
        val mockSource = mockk<DatabaseSource>()
        val mockDest = mockk<DatabaseDestination>()

        every { mockSource.listCollections() } returns emptyFlow()

        runBlocking { Migrator().migrate(mockSource, mockDest) }

        verify(exactly = 1) { mockSource.listCollections() }
        coVerify(exactly = 0) { mockDest.createCollection(any()) }
        coVerify(exactly = 0) { mockDest.insertDocuments(any(), any()) }
    }

    @ParameterizedTest
    @MethodSource("getTargetDbs")
    fun `migrate() handles large datasets efficiently`(
        destination: DatabaseDestination,
        destinationAsSource: DatabaseSource
    ) = runBlocking {

        val mockSource = mockk<DatabaseSource>()
        val mockCollection = mockk<DocumentCollection>()

        val testMetadata = CollectionMetadata(
            name = "large_table",
            fields = listOf(FieldDefinition("id", "string"))
        )

        // Generate a large number of test documents
        var recordsCount = 0
        val testDocuments = flow {
            while (recordsCount < NUM_RECORDS) {
                emit(mapOf("id" to recordsCount.toString()))
                recordsCount++
            }
        }

        coEvery { mockCollection.fetchMetadata() } returns testMetadata
        every { mockCollection.readDocuments() } returns testDocuments
        every { mockSource.listCollections() } returns flowOf(mockCollection)

        destination.use { Migrator().migrate(mockSource, it) }

        assertEquals(NUM_RECORDS, recordsCount)
        assertEquals(
            NUM_RECORDS.toLong(),
            destinationAsSource.use { it.countDocuments("large_table") }
        )
    }
}
