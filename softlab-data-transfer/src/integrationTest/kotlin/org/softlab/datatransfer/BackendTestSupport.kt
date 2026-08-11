package org.softlab.datatransfer

import org.dbunit.database.DatabaseConfig
import org.softlab.dataset.jdbc.sqlite.SqliteDataTypeFactory
import org.softlab.dataset.jdbc.sqlite.SqliteMetadataHandler
import org.softlab.dataset.jdbc.sqlite.sqliteAttach
import org.softlab.datataset.test.initiators.JdbcInitiator
import org.softlab.datataset.test.initiators.createMongoContainer
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.softlab.datataset.test.initiators.createRedisContainer
import org.softlab.datatransfer.config.ConfigProvider
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager


@Testcontainers
abstract class BackendTestSupport {
    companion object {
        @Container
        val postgres = createPostgresContainer()

        @Container
        val mongo = createMongoContainer()

        @Container
        val redis = createRedisContainer()

        val dataTypeMappings = ConfigProvider.config.getDataTypeMappings()

        fun createPostgresDatabases(vararg dbNames: String) {
            DriverManager.getConnection(
                postgresDbUrl("postgres"),
                postgres.username,
                postgres.password
            ).use { connection ->
                connection.createStatement().use { stmt ->
                    dbNames.forEach { dbName ->
                        stmt.execute("CREATE DATABASE $dbName;")
                    }
                }
            }
        }

        fun postgresDbUrl(dbName: String): String =
            "jdbc:postgresql://${postgres.host}:${postgres.firstMappedPort}/$dbName?loggerLevel=OFF"

        fun postgresAdapterUri(dbUrl: String): String =
            "$dbUrl&user=${postgres.username}&password=${postgres.password}"

        fun mongoUri(dbName: String): String =
            "${mongo.connectionString}/$dbName"

        fun redisDbUri(dbIndex: Int): String {
            val uri = URI(redis.redisURI.toString())
            return URI(uri.scheme, uri.rawAuthority, "/$dbIndex", uri.rawQuery, uri.rawFragment).toString()
        }

        fun sqliteDb(prefix: String): String =
            sqliteDbPath(prefix).toString()

        fun sqliteDbPath(prefix: String): Path =
            Files.createTempFile(prefix, ".db")
                .apply { toFile().deleteOnExit() }

        fun sqliteUrl(dbFile: String): String = "jdbc:sqlite:$dbFile"

        fun sqliteUrl(dbFile: Path): String = "jdbc:sqlite:$dbFile"

        fun withPostgresInitiator(dbUrl: String, block: JdbcInitiator.() -> Unit) {
            JdbcInitiator(dbUrl, postgres.username, postgres.password).use(block)
        }

        fun withSqliteInitiator(dbFile: Path, block: JdbcInitiator.() -> Unit) {
            JdbcInitiator(
                dbUrl = sqliteUrl(dbFile),
                dbUnitProperties = mapOf(
                    DatabaseConfig.PROPERTY_DATATYPE_FACTORY to SqliteDataTypeFactory(),
                    DatabaseConfig.PROPERTY_METADATA_HANDLER to SqliteMetadataHandler("data")
                )
            ) { url ->
                DriverManager
                    .getConnection(url)
                    .sqliteAttach(dbFile.toString(), "data")
            }.use(block)
        }
    }
}
