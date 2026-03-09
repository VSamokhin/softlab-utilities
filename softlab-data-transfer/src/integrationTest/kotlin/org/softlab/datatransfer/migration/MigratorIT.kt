package org.softlab.datatransfer.migration

import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.softlab.dataset.core.FieldDefinition
import org.softlab.datataset.test.initiators.createMongoContainer
import org.softlab.datataset.test.initiators.createPostgresContainer
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

@Testcontainers
class MigratorIT {
    companion object {
        private const val DATABASE = "testdb"

        private const val NUM_RECORDS = 10_000

        @Container
        @JvmStatic
        private val mongo = createMongoContainer()

        @Container
        @JvmStatic
        private val postgres = createPostgresContainer()

        private val mongoTypes = ConfigProvider.config
            .getDataTypeMappings()
            .destination(MongoDestination.Companion.BACKEND)
        private val postgresTypes = ConfigProvider.config
            .getDataTypeMappings()
            .destination(PostgresDestination.Companion.BACKEND)

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

    @ParameterizedTest
    @MethodSource("getTargetDbs")
    fun `migrate() handles large datasets efficiently`(
        destination: DatabaseDestination,
        destinationAsSource: DatabaseSource
    ) = runBlocking {

        val mockSource = mockk<DatabaseSource>()
        val mockCollection = mockk<DocumentCollection>()

        val testMetadata = CollectionMetadata(
            name = "public.large_table",
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

        Assertions.assertEquals(NUM_RECORDS, recordsCount)
        Assertions.assertEquals(
            NUM_RECORDS.toLong(),
            destinationAsSource.use { it.countDocuments("public.large_table") }
        )
    }
}