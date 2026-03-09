package org.softlab.datatransfer

import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.collection.IsIterableContainingInOrder.contains
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.softlab.datataset.test.initiators.JdbcInitiator
import org.softlab.datataset.test.initiators.MongoInitiator
import org.softlab.datataset.test.initiators.createMongoContainer
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.softlab.datatransfer.adapters.AdapterProvider
import org.softlab.datatransfer.core.DatabaseSource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.DriverManager
import kotlin.test.assertEquals


/**
 * Here I'm not into checking each and every aspect of the data transfer process,
 * just a quick proof whether all transfer combinations are functional
 */
@Testcontainers
class MainTest {
    companion object {
        private const val EXPECTED_RECORDS = 100L
        private const val COLLECTION_NAME = "data.various_datatypes"
        private const val POSTGRES_TARGET_DB = "main_target_db"

        private const val POSTGRES_CHANGELOG = "liquibase/changelog-various_datatypes-postgres.yaml"
        private const val MONGO_CHANGELOG = "liquibase/changelog-various_datatypes-mongo.yaml"
        private const val DATASET = "datasets/various_datatypes.yml"

        @Container
        @JvmStatic
        private val postgres = createPostgresContainer()

        @Container
        @JvmStatic
        private val mongo = createMongoContainer()

        private lateinit var postgresSource: JdbcInitiator
        private lateinit var postgresDest: JdbcInitiator

        private lateinit var postgresSourceUri: String
        private lateinit var postgresDestUri: String

        private lateinit var mongoSource: MongoInitiator
        private lateinit var mongoDest: MongoInitiator

        @BeforeAll
        @JvmStatic
        fun setup() {
            //postgres.waitingFor()
            postgresSource = JdbcInitiator(postgres.jdbcUrl, postgres.username, postgres.password)
            postgresSourceUri =
                "${postgresSource.dbUrl}&user=${postgres.username}&password=${postgres.password}"
            createPostgresTargetDatabase()
            postgresDest = JdbcInitiator("jdbc:postgresql://${postgres.host}:${postgres.firstMappedPort}" +
                "/$POSTGRES_TARGET_DB?loggerLevel=OFF", postgres.username, postgres.password)
            postgresDestUri =
                "${postgresDest.dbUrl}&user=${postgres.username}&password=${postgres.password}"

            mongoSource = MongoInitiator("${mongo.connectionString}/main_source_db")
            mongoDest = MongoInitiator("${mongo.connectionString}/main_target_db")
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            postgresSource.close()
            postgresDest.close()

            mongoSource.close()
            mongoDest.close()
        }

        private fun createPostgresTargetDatabase() {
            val adminUrl = "jdbc:postgresql://${postgres.host}:${postgres.firstMappedPort}/postgres"
            DriverManager.getConnection(adminUrl, postgres.username, postgres.password).use { conn ->
                conn.autoCommit = true
                conn.createStatement().use { stmt ->
                    stmt.execute("DROP DATABASE IF EXISTS $POSTGRES_TARGET_DB WITH (FORCE);")
                    stmt.execute("CREATE DATABASE $POSTGRES_TARGET_DB;")
                }
            }
        }

        @JvmStatic
        fun getDbCombinations(): List<Arguments> = listOf(
            Arguments.of(
                postgresSourceUri,
                mongoDest.dbUrl,
                AdapterProvider.sourceFor(mongoDest.dbUrl)
            ),
            Arguments.of(
                mongoSource.dbUrl,
                mongoDest.dbUrl,
                AdapterProvider.sourceFor(mongoDest.dbUrl)
            ),
            Arguments.of(
                mongoSource.dbUrl,
                postgresDestUri,
                AdapterProvider.sourceFor(postgresDestUri)
            ),
            Arguments.of(
                postgresSourceUri,
                postgresDestUri,
                AdapterProvider.sourceFor(postgresDestUri)
            )
        )
    }

    @BeforeEach
    fun resetDatabases() {
        postgresSource.cleanup { c ->
            c.createStatement().use { stmt ->
                stmt.execute("DROP SCHEMA IF EXISTS data CASCADE;")
            }
        }
        postgresDest.cleanup { c ->
            c.createStatement().use { stmt ->
                stmt.execute("DROP SCHEMA IF EXISTS data CASCADE;")
            }
        }
        mongoSource.cleanup()
        mongoDest.cleanup()

        postgresSource.initSchema(POSTGRES_CHANGELOG)
        postgresSource.seedData(DATASET)

        mongoSource.initSchema(MONGO_CHANGELOG)
        mongoSource.seedData(DATASET)
    }

    @ParameterizedTest
    @MethodSource("getDbCombinations")
    fun `main() migrates expected count of documents`(
        sourceUri: String,
        destUri: String,
        destDb: DatabaseSource
    ) = runBlocking {

        assertDoesNotThrow {
            main(arrayOf(sourceUri, destUri, "--source-filter", COLLECTION_NAME))
        }

        val targetCollections = destDb.listCollections()
            .map { it.fetchMetadata().name }
            .toList()
        assertThat(targetCollections, contains(COLLECTION_NAME))

        val targetCount = destDb.countDocuments(COLLECTION_NAME)
        assertEquals(EXPECTED_RECORDS, targetCount)
    }
}
