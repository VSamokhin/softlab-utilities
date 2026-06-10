package org.softlab.datatransfer.adapters

import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.softlab.datataset.test.initiators.createMongoContainer
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.softlab.datataset.test.initiators.createRedisContainer
import org.softlab.datatransfer.adapters.mongo.MongoDestination
import org.softlab.datatransfer.adapters.mongo.MongoSource
import org.softlab.datatransfer.adapters.postgres.PostgresDestination
import org.softlab.datatransfer.adapters.postgres.PostgresSource
import org.softlab.datatransfer.adapters.redis.RedisDestination
import org.softlab.datatransfer.adapters.redis.RedisSource
import org.softlab.datatransfer.adapters.sqlite.SqliteDestination
import org.softlab.datatransfer.adapters.sqlite.SqliteSource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.nio.file.Files


@Testcontainers
class AdapterProviderIT {
    companion object {
        @Container
        @JvmStatic
        private val postgres = createPostgresContainer()

        @Container
        @JvmStatic
        private val mongo = createMongoContainer()

        @Container
        @JvmStatic
        private val redis = createRedisContainer()

        private lateinit var postgresUri: String
        private lateinit var mongoUri: String
        private lateinit var redisUri: String
        private val sqliteUri: String = Files.createTempFile("data-transfer-sqlite", ".db")
            .apply { toFile().deleteOnExit() }.let { "jdbc:sqlite:$it" }

        private val REDIS_OPTIONS = mapOf(
            AdapterProvider.REDIS_SOURCE_MAPPING to "mappings/MainIT-AdapterProviderIT-various_datatypes.yml"
        )
        private val JOBS_OPTION = mapOf(AdapterProvider.JOBS_HINT to "1")

        @BeforeAll
        @JvmStatic
        fun setup() {
            postgresUri = "${postgres.jdbcUrl}&user=${postgres.username}&password=${postgres.password}"
            mongoUri = "${mongo.connectionString}/testdb"
            redisUri = redis.redisURI
        }

        @JvmStatic
        fun getSources(): List<Arguments> {
            return listOf(
                Arguments.of(postgresUri, JOBS_OPTION, PostgresSource::class.java),
                Arguments.of(mongoUri, emptyMap<String, String>(), MongoSource::class.java),
                Arguments.of(sqliteUri, JOBS_OPTION, SqliteSource::class.java),
                Arguments.of(redisUri, REDIS_OPTIONS, RedisSource::class.java)
            )
        }

        @JvmStatic
        fun getDestinations(): List<Arguments> {
            return listOf(
                Arguments.of(postgresUri, JOBS_OPTION, PostgresDestination::class.java),
                Arguments.of(mongoUri, emptyMap<String, String>(), MongoDestination::class.java),
                Arguments.of(sqliteUri, emptyMap<String, String>(), SqliteDestination::class.java),
                Arguments.of(redisUri, REDIS_OPTIONS, RedisDestination::class.java)
            )
        }
    }

    @ParameterizedTest
    @MethodSource("getSources")
    fun `sourceFor() returns correct source for given URI`(
        uri: String,
        options:  Map<String, String>,
        type: Class<*>
    ) {
        AdapterProvider.sourceFor(uri, options).use { source ->
            assertInstanceOf(type, source)
        }
    }

    @ParameterizedTest
    @MethodSource("getDestinations")
    fun `destinationFor() returns correct destination for given URI`(
        uri: String,
        options:  Map<String, String>,
        type: Class<*>
    ) {
        AdapterProvider.destinationFor(uri, options).use { destination ->
            assertInstanceOf(type, destination)
        }
    }
}
