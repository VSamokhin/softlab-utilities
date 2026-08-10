package org.softlab.datatransfer

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.hasItem
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.softlab.datataset.test.initiators.MongoInitiator
import org.softlab.datataset.test.initiators.RedisInitiator
import org.softlab.datatransfer.adapters.AdapterProvider
import kotlin.test.assertEquals


/**
 * Here I'm not into checking each and every aspect of the data transfer process,
 * just a quick proof whether all transfer combinations are functional
 */
class MainIT : BackendTestSupport() {
    data class AdapterEndpoints(
        val name: String,
        val source: SourceEndpoint,
        val destination: DestinationEndpoint
    )

    data class SourceEndpoint(
        val options: Map<String, String> = emptyMap(),
        val collectionName: String = COLLECTION_NAME,
        val prepare: () -> String
    )

    data class DestinationEndpoint(
        val options: Map<String, String> = emptyMap(),
        val prepare: () -> String
    )

    companion object {
        private const val EXPECTED_RECORDS = 100L
        private const val COLLECTION_NAME = "data.various_datatypes"
        private const val SQLITE_COLLECTION_NAME = "various_datatypes"

        private const val POSTGRES_SOURCE_DB = "main_source_db"
        private const val POSTGRES_TARGET_DB = "main_target_db"

        private const val POSTGRES_CHANGELOG = "liquibase/changelog-various_datatypes-postgres.yaml"
        private const val MONGO_CHANGELOG = "liquibase/changelog-various_datatypes-mongo.yaml"
        private const val SQLITE_CHANGELOG = "liquibase/changelog-various_datatypes-sqlite.yaml"
        private const val DATASET = "datasets/various_datatypes.yml"
        private const val REDIS_MAPPING = "mappings/MainIT-various_datatypes.yml"
        private val JOBS_OPTION = mapOf(AdapterProvider.JOBS_HINT to "1")

        @JvmStatic
        @BeforeAll
        fun setup() {
            createPostgresDatabases(POSTGRES_SOURCE_DB, POSTGRES_TARGET_DB)
        }

        @JvmStatic
        fun databaseAdapterCombinations(): List<Arguments> {
            val adapters = adapterEndpoints()

            return adapters.flatMap { source ->
                adapters.map { destination ->
                    Arguments.of(
                        "${source.name} to ${destination.name}",
                        source.source,
                        destination.destination
                    )
                }
            }
        }

        private fun adapterEndpoints(): List<AdapterEndpoints> = listOf(
            postgresEndpoints(),
            mongoEndpoints(),
            redisEndpoints(),
            sqliteEndpoints()
        )

        private fun postgresEndpoints(): AdapterEndpoints {
            val sourceDbUrl = postgresDbUrl(POSTGRES_SOURCE_DB)
            val destinationDbUrl = postgresDbUrl(POSTGRES_TARGET_DB)

            return AdapterEndpoints(
                name = "postgres",
                source = SourceEndpoint(
                    options = JOBS_OPTION,
                    prepare = {
                        withPostgresInitiator(sourceDbUrl) {
                            cleanup { connection ->
                                connection.createStatement().use { stmt ->
                                    stmt.execute("DROP SCHEMA IF EXISTS data CASCADE;")
                                }
                            }
                            initSchema(POSTGRES_CHANGELOG)
                            seedData(DATASET)
                        }
                        postgresAdapterUri(sourceDbUrl)
                    }
                ),
                destination = DestinationEndpoint(
                    options = JOBS_OPTION,
                    prepare = {
                        withPostgresInitiator(destinationDbUrl) {
                            cleanup { connection ->
                                connection.createStatement().use { stmt ->
                                    stmt.execute("DROP SCHEMA IF EXISTS data CASCADE;")
                                }
                            }
                        }
                        postgresAdapterUri(destinationDbUrl)
                    }
                )
            )
        }

        private fun mongoEndpoints(): AdapterEndpoints {
            return AdapterEndpoints(
                name = "mongo",
                source = SourceEndpoint(
                    prepare = {
                        val sourceUri = mongoUri("main_source_db")
                        MongoInitiator(sourceUri).use {
                            it.cleanup()
                            it.initSchema(MONGO_CHANGELOG)
                            it.seedData(DATASET)
                        }
                        sourceUri
                    }
                ),
                destination = DestinationEndpoint(
                    prepare = {
                        val destinationUri = mongoUri("main_target_db")
                        MongoInitiator(destinationUri).use { it.cleanup() }
                        destinationUri
                    }
                )
            )
        }

        private fun redisEndpoints(): AdapterEndpoints {
            val options = mapOf(AdapterProvider.REDIS_SOURCE_MAPPING to REDIS_MAPPING)

            return AdapterEndpoints(
                name = "redis",
                source = SourceEndpoint(
                    options = options,
                    prepare = {
                        val sourceUri = redisDbUri(0)
                        RedisInitiator(sourceUri, REDIS_MAPPING).use {
                            it.cleanup()
                            it.initSchema("ignored")
                            it.seedData(DATASET)
                        }
                        sourceUri
                    }
                ),
                destination = DestinationEndpoint(
                    options = options,
                    prepare = {
                        val destinationUri = redisDbUri(1)
                        RedisInitiator(destinationUri, REDIS_MAPPING).use { it.cleanup() }
                        destinationUri
                    }
                )
            )
        }

        private fun sqliteEndpoints(): AdapterEndpoints {

            return AdapterEndpoints(
                name = "sqlite",
                source = SourceEndpoint(
                    options = JOBS_OPTION,
                    collectionName = SQLITE_COLLECTION_NAME,
                    prepare = {
                        val source = sqliteDbPath("data-transfer-main-source")
                        val sourceUri = sqliteUrl(source)
                        withSqliteInitiator(source) {
                            cleanup()
                            initSchema(SQLITE_CHANGELOG)
                            seedData(DATASET)
                        }
                        sourceUri
                    }
                ),
                destination = DestinationEndpoint(
                    options = JOBS_OPTION,
                    prepare = {
                        val destination = sqliteDbPath("data-transfer-main-target")
                        val destinationUri = sqliteUrl(destination)
                        withSqliteInitiator(destination) {
                            cleanup()
                        }
                        destinationUri
                    }
                )
            )
        }

        private fun Map<String, String>.toCliOptions(): String =
            entries.joinToString(",") { (key, value) -> "$key=$value" }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("databaseAdapterCombinations")
    fun `main() migrates expected count of documents`(
        testName: String,
        source: SourceEndpoint,
        destination: DestinationEndpoint
    ) {
        val sourceUri = source.prepare()
        val destinationUri = destination.prepare()

        assertDoesNotThrow(testName) {
            val args = mutableListOf(sourceUri, destinationUri, "--source-filter", source.collectionName)
            if (source.options.isNotEmpty()) {
                args += listOf("--source-options", source.options.toCliOptions())
            }
            if (destination.options.isNotEmpty()) {
                args += listOf("--dest-options", destination.options.toCliOptions())
            }
            main(args.toTypedArray())
        }

        AdapterProvider.sourceFor(destinationUri, destination.options).use { destinationSource ->
            val targetCollections = runBlocking {
                destinationSource.listCollections().toList()
                    .map { it.fetchMetadata().name }
            }
            val expectedCollection = source.collectionName.substringAfter(".")
            assertThat(targetCollections, hasItem(containsString(expectedCollection)))

            val targetCount = runBlocking { destinationSource.countDocuments(source.collectionName) }
            assertEquals(EXPECTED_RECORDS, targetCount)
        }
    }
}
