package org.softlab.datatransfer

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.dbunit.database.DatabaseConfig
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.softlab.dataset.jdbc.sqlite.SqliteDataTypeFactory
import org.softlab.dataset.jdbc.sqlite.SqliteMetadataHandler
import org.softlab.dataset.jdbc.sqlite.sqliteAttach
import org.softlab.datataset.test.initiators.JdbcInitiator
import org.softlab.datataset.test.initiators.MongoInitiator
import org.softlab.datataset.test.initiators.RedisInitiator
import org.softlab.datataset.test.initiators.createMongoContainer
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.softlab.datataset.test.initiators.createRedisContainer
import org.softlab.datatransfer.adapters.AdapterProvider
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertEquals


/**
 * Here I'm not into checking each and every aspect of the data transfer process,
 * just a quick proof whether all transfer combinations are functional
 */
@Testcontainers
class MainIT {
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
        val collectionName: String = COLLECTION_NAME,
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

        @Container
        @JvmStatic
        private val postgres = createPostgresContainer()

        @Container
        @JvmStatic
        private val mongo = createMongoContainer()

        @Container
        @JvmStatic
        private val redis = createRedisContainer()

        @JvmStatic
        @BeforeAll
        fun setup() {
            DriverManager.getConnection(
                postgresDbUrl("postgres"),
                postgres.username,
                postgres.password
            ).use { connection ->
                connection.createStatement().use { stmt ->
                    stmt.execute("CREATE DATABASE $POSTGRES_SOURCE_DB;")
                    stmt.execute("CREATE DATABASE $POSTGRES_TARGET_DB;")
                }
            }
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
                    prepare = {
                        withJdbcInitiator(sourceDbUrl) {
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
                    prepare = {
                        withJdbcInitiator(destinationDbUrl) {
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
                        val sourceUri = "${mongo.connectionString}/main_source_db"
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
                        val destinationUri = "${mongo.connectionString}/main_target_db"
                        MongoInitiator(destinationUri).use { it.cleanup() }
                        destinationUri
                    }
                )
            )
        }

        private fun redisEndpoints(): AdapterEndpoints {
            val options = mapOf("mapping" to REDIS_MAPPING)

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

            val destination = Files.createTempFile("data-transfer-main-target", ".db")
            destination.toFile().deleteOnExit()

            return AdapterEndpoints(
                name = "sqlite",
                source = SourceEndpoint(
                    collectionName = SQLITE_COLLECTION_NAME,
                    prepare = {
                        val source = Files
                            .createTempFile("data-transfer-main-source", ".db")
                            .apply { toFile().deleteOnExit() }
                        val sourceUri = "jdbc:sqlite:$source"
                        withSqliteInitiator(source) {
                            cleanup()
                            initSchema(SQLITE_CHANGELOG)
                            seedData(DATASET)
                        }
                        sourceUri
                    }
                ),
                destination = DestinationEndpoint(
                    collectionName = SQLITE_COLLECTION_NAME,
                    prepare = {
                        val destination = Files
                            .createTempFile("data-transfer-main-target", ".db")
                            .apply { toFile().deleteOnExit() }
                        val destinationUri = "jdbc:sqlite:$destination"
                        withSqliteInitiator(destination) {
                            cleanup()
                        }
                        destinationUri
                    }
                )
            )
        }

        private fun postgresDbUrl(dbName: String): String =
            "jdbc:postgresql://${postgres.host}:${postgres.firstMappedPort}/$dbName?loggerLevel=OFF"

        private fun postgresAdapterUri(dbUrl: String): String =
            "$dbUrl&user=${postgres.username}&password=${postgres.password}"

        private inline fun withJdbcInitiator(dbUrl: String, block: JdbcInitiator.() -> Unit) {
            JdbcInitiator(dbUrl, postgres.username, postgres.password).use(block)
        }

        private inline fun withSqliteInitiator(dbFile: Path, block: JdbcInitiator.() -> Unit) {
            JdbcInitiator(dbUrl = "jdbc:sqlite:$dbFile", dbUnitProperties = mapOf(
                DatabaseConfig.PROPERTY_DATATYPE_FACTORY to SqliteDataTypeFactory(),
                DatabaseConfig.PROPERTY_METADATA_HANDLER to SqliteMetadataHandler("data"),
            )) { url ->
                DriverManager
                    .getConnection(url)
                    .sqliteAttach(dbFile.toString(), "data")
            }.use(block)
        }

        private fun redisDbUri(dbIndex: Int): String {
            val uri = URI(redis.redisURI.toString())
            return URI(uri.scheme, uri.rawAuthority, "/$dbIndex", uri.rawQuery, uri.rawFragment).toString()
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
            assertThat(targetCollections, hasItem(destination.collectionName))

            val targetCount = runBlocking { destinationSource.countDocuments(source.collectionName) }
            assertEquals(EXPECTED_RECORDS, targetCount)
        }
    }
}
