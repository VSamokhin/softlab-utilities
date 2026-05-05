package org.softlab.datatransfer.adapters

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.softlab.datataset.test.initiators.createMongoContainer
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.softlab.datataset.test.initiators.createRedisContainer
import org.softlab.datatransfer.adapters.mongo.MongoDestination
import org.softlab.datatransfer.adapters.mongo.MongoSource
import org.softlab.datatransfer.adapters.postgres.PostgresDestination
import org.softlab.datatransfer.adapters.postgres.PostgresSource
import org.softlab.datatransfer.adapters.redis.RedisDestination
import org.softlab.datatransfer.adapters.redis.RedisSource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertIs


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

        private const val REDIS_MAPPING = "mappings/various_datatypes-redis.yml"

        @BeforeAll
        @JvmStatic
        fun setup() {
            postgresUri = "${postgres.jdbcUrl}&user=${postgres.username}&password=${postgres.password}"
            mongoUri = "${mongo.connectionString}/testdb"
            redisUri = redis.redisURI
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
    fun `sourceFor() returns redis source for redis URI with mapping`() {
        AdapterProvider.sourceFor(redisUri, mapOf("mapping" to REDIS_MAPPING)).use { source ->
            assertIs<RedisSource>(source)
        }
    }

    @Test
    fun `destinationFor() returns redis destination for redis URI with mapping`() {
        AdapterProvider.destinationFor(redisUri, mapOf("mapping" to REDIS_MAPPING)).use { destination ->
            assertIs<RedisDestination>(destination)
        }
    }
}
