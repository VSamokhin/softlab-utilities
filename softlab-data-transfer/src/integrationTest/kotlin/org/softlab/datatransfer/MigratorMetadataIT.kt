package org.softlab.datatransfer

import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.softlab.dataset.core.FieldDefinition
import org.softlab.datataset.test.initiators.MongoInitiator
import org.softlab.datataset.test.initiators.RedisInitiator
import org.softlab.datatransfer.adapters.mongo.MongoDestination
import org.softlab.datatransfer.adapters.mongo.MongoSource
import org.softlab.datatransfer.adapters.postgres.PostgresConnectionPool
import org.softlab.datatransfer.adapters.postgres.PostgresDestination
import org.softlab.datatransfer.adapters.postgres.PostgresSource
import org.softlab.datatransfer.adapters.redis.RedisDestination
import org.softlab.datatransfer.adapters.redis.RedisSource
import org.softlab.datatransfer.adapters.sqlite.SqliteConnectionPool
import org.softlab.datatransfer.adapters.sqlite.SqliteDestination
import org.softlab.datatransfer.adapters.sqlite.SqliteSource
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.core.IndexDefinition
import org.softlab.datatransfer.core.IndexFieldDefinition
import org.softlab.datatransfer.core.IndexSortOrder
import org.softlab.datatransfer.core.PrimaryKeyDefinition
import org.softlab.datatransfer.migration.Migrator
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull


class MigratorMetadataIT : BackendTestSupport() {
    companion object {
        private const val POSTGRES_SOURCE_DB = "metadata_source_db"
        private const val POSTGRES_TARGET_DB = "metadata_target_db"
        private const val MONGO_SOURCE_DB = "metadata_source_db"
        private const val MONGO_TARGET_DB = "metadata_target_db"
        private const val REDIS_MAPPING = "mappings/MigratorMetadataIT-users.yml"

        @JvmStatic
        @BeforeAll
        fun setupPostgresDatabases() {
            createPostgresDatabases(POSTGRES_SOURCE_DB, POSTGRES_TARGET_DB)
        }
    }

    @BeforeEach
    fun cleanup() {
        cleanupPostgres(postgresDbUrl(POSTGRES_SOURCE_DB))
        cleanupPostgres(postgresDbUrl(POSTGRES_TARGET_DB))
        MongoInitiator(mongoUri(MONGO_SOURCE_DB)).use { it.cleanup() }
        MongoInitiator(mongoUri(MONGO_TARGET_DB)).use { it.cleanup() }
        RedisInitiator(redisDbUri(0), REDIS_MAPPING).use { it.cleanup() }
        RedisInitiator(redisDbUri(1), REDIS_MAPPING).use { it.cleanup() }
    }

    @Test
    fun `postgres to postgres preserves primary key and indices`() = runBlocking {
        createPostgresSource()

        migrate(
            source = postgresSource(POSTGRES_SOURCE_DB),
            destination = postgresDestination(POSTGRES_TARGET_DB)
        )

        val metadata = postgresSource(POSTGRES_TARGET_DB).use {
            it.fetchMetadata("public.users")
        }
        assertEquals(PrimaryKeyDefinition("users_pkey", listOf("id")), metadata.primaryKey)
        assertContentEquals(expectedIndices(), metadata.indices)
    }

    @Test
    fun `sqlite to sqlite preserves primary key and supported indices`() = runBlocking {
        val sourceDb = newSqliteDb()
        val targetDb = newSqliteDb()
        createSqliteSource(sourceDb)

        migrate(
            source = SqliteSource(SqliteConnectionPool(sqliteUrl(sourceDb))),
            destination = SqliteDestination(
                SqliteConnectionPool(sqliteUrl(targetDb)),
                dataTypeMappings
            )
        )

        val metadata = SqliteSource(SqliteConnectionPool(sqliteUrl(targetDb))).use {
            it.fetchMetadata("users")
        }
        assertEquals(PrimaryKeyDefinition(null, listOf("id")), metadata.primaryKey)
        assertContentEquals(expectedIndices(), metadata.indices)
    }

    @Test
    fun `postgres to sqlite preserves compatible primary key and supported indices`() = runBlocking {
        val targetDb = newSqliteDb()
        createPostgresSource()

        migrate(
            source = postgresSource(POSTGRES_SOURCE_DB),
            destination = SqliteDestination(
                SqliteConnectionPool(sqliteUrl(targetDb)),
                dataTypeMappings
            )
        )

        val metadata = SqliteSource(SqliteConnectionPool(sqliteUrl(targetDb))).use {
            it.fetchMetadata("users")
        }
        assertEquals(listOf("id"), metadata.primaryKey?.fields)
        assertContentEquals(expectedIndices(), metadata.indices)
    }

    @Test
    fun `sqlite to postgres preserves compatible primary key and supported indices`() = runBlocking {
        val sourceDb = newSqliteDb()
        createSqliteSource(sourceDb)

        migrate(
            source = SqliteSource(SqliteConnectionPool(sqliteUrl(sourceDb))),
            destination = postgresDestination(POSTGRES_TARGET_DB)
        )

        val metadata = postgresSource(POSTGRES_TARGET_DB).use {
            it.fetchMetadata("public.users")
        }
        assertEquals(PrimaryKeyDefinition("users_pkey", listOf("id")), metadata.primaryKey)
        assertContentEquals(expectedIndices(), metadata.indices)
    }

    @Test
    fun `mongo to mongo preserves supported indices`() = runBlocking {
        createMongoSource()

        migrate(
            source = MongoSource(mongoUri(MONGO_SOURCE_DB)),
            destination = MongoDestination(
                mongoUri(MONGO_TARGET_DB),
                dataTypeMappings
            )
        )

        val metadata = MongoSource(mongoUri(MONGO_TARGET_DB)).use {
            it.fetchMetadata("users")
        }
        assertNull(metadata.primaryKey)
        assertContentEquals(expectedIndices(), metadata.indices)
    }

    @Test
    fun `postgres to mongo maps primary key to unique index and supported secondary indices`() = runBlocking {
        createPostgresSource()

        migrate(
            source = postgresSource(POSTGRES_SOURCE_DB),
            destination = MongoDestination(
                mongoUri(MONGO_TARGET_DB),
                dataTypeMappings
            )
        )

        val metadata = MongoSource(mongoUri(MONGO_TARGET_DB)).use {
            it.fetchMetadata("public.users")
        }
        assertNull(metadata.primaryKey)
        assertContentEquals(expectedIndicesWithPrimaryKey("users_pkey"), metadata.indices)
    }

    @Test
    fun `mongo to postgres maps supported indices without primary key`() = runBlocking {
        createMongoSource()

        migrate(
            source = MongoSource(mongoUri(MONGO_SOURCE_DB)),
            destination = postgresDestination(POSTGRES_TARGET_DB)
        )

        val metadata = postgresSource(POSTGRES_TARGET_DB).use {
            it.fetchMetadata("public.users")
        }
        assertNull(metadata.primaryKey)
        assertContentEquals(expectedIndices(), metadata.indices)
    }

    @Test
    fun `sqlite to mongo maps primary key to unique index and supported secondary indices`() = runBlocking {
        val sourceDb = newSqliteDb()
        createSqliteSource(sourceDb)

        migrate(
            source = SqliteSource(SqliteConnectionPool(sqliteUrl(sourceDb))),
            destination = MongoDestination(
                mongoUri(MONGO_TARGET_DB),
                dataTypeMappings
            )
        )

        val metadata = MongoSource(mongoUri(MONGO_TARGET_DB)).use {
            it.fetchMetadata("users")
        }
        assertNull(metadata.primaryKey)
        assertContentEquals(expectedIndicesWithPrimaryKey("users_pkey"), metadata.indices)
    }

    @Test
    fun `mongo to sqlite maps supported indices without primary key`() = runBlocking {
        val targetDb = newSqliteDb()
        createMongoSource()

        migrate(
            source = MongoSource(mongoUri(MONGO_SOURCE_DB)),
            destination = SqliteDestination(
                SqliteConnectionPool(sqliteUrl(targetDb)),
                dataTypeMappings
            )
        )

        val metadata = SqliteSource(SqliteConnectionPool(sqliteUrl(targetDb))).use {
            it.fetchMetadata("users")
        }
        assertNull(metadata.primaryKey)
        assertContentEquals(expectedIndices(), metadata.indices)
    }

    @Test
    fun `postgres to redis does not expose primary key or indices`() = runBlocking {
        createPostgresSource()

        migrate(
            source = postgresSource(POSTGRES_SOURCE_DB),
            destination = RedisDestination(redisDbUri(1), REDIS_MAPPING)
        )

        val metadata = RedisSource(
            redisDbUri(1),
            REDIS_MAPPING,
            dataTypeMappings
        ).use {
            it.fetchMetadata("public.users")
        }
        assertNull(metadata.primaryKey)
        assertContentEquals(emptyList(), metadata.indices)
    }

    private fun cleanupPostgres(dbUrl: String) {
        PostgresConnectionPool(
            dbUrl,
            postgres.username,
            postgres.password
        ).use { pool ->
            pool.openConnection().use { connection ->
                connection.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE IF EXISTS public.users;")
                }
            }
        }
    }

    private suspend fun createPostgresSource() {
        PostgresConnectionPool(
            postgresDbUrl(POSTGRES_SOURCE_DB),
            postgres.username,
            postgres.password
        ).use { pool ->
            PostgresDestination(
                pool,
                dataTypeMappings,
                closePool = false
            ).use { destination ->
                createIndexedSource(destination, sqlMetadata("users"))
            }
        }
    }

    private suspend fun createSqliteSource(dbFile: String) {
        SqliteConnectionPool(sqliteUrl(dbFile)).use { pool ->
            SqliteDestination(
                pool,
                dataTypeMappings,
                closePool = false
            ).use { destination ->
                createIndexedSource(destination, sqlMetadata("users"))
            }
        }
    }

    private suspend fun createMongoSource() {
        MongoDestination(
            mongoUri(MONGO_SOURCE_DB),
            dataTypeMappings
        ).use { destination ->
            createIndexedSource(destination, mongoMetadata("users"))
        }
    }

    private suspend fun createIndexedSource(destination: DatabaseDestination, metadata: CollectionMetadata) {
        destination.createCollection(metadata)
        destination.insertDocuments(
            metadata.name,
            flowOf(
                mapOf("id" to 1, "tenant_id" to 10, "name" to "Alice", "email" to "alice@example.com"),
                mapOf("id" to 2, "tenant_id" to 10, "name" to "Bob", "email" to "bob@example.com")
            )
        )
        destination.createIndexesAndConstraints(metadata)
    }

    private suspend fun migrate(source: DatabaseSource, destination: DatabaseDestination) {
        source.use { src ->
            destination.use { dest ->
                Migrator(workerThreads = 1).migrate(src, dest)
            }
        }
    }

    private suspend fun DatabaseSource.fetchMetadata(collectionName: String): CollectionMetadata =
        listCollections()
            .toList()
            .map { it.fetchMetadata() }
            .single { it.name == collectionName }

    private fun postgresSource(dbName: String): PostgresSource =
        PostgresSource(
            PostgresConnectionPool(
                postgresDbUrl(dbName),
                postgres.username,
                postgres.password
            )
        )

    private fun postgresDestination(dbName: String): PostgresDestination =
        PostgresDestination(
            PostgresConnectionPool(
                postgresDbUrl(dbName),
                postgres.username,
                postgres.password
            ),
            dataTypeMappings
        )

    private fun sqlMetadata(name: String): CollectionMetadata =
        CollectionMetadata(
            name = name,
            fields = fields(),
            primaryKey = PrimaryKeyDefinition(null, listOf("id")),
            indices = expectedIndices()
        )

    private fun mongoMetadata(name: String): CollectionMetadata =
        CollectionMetadata(
            name = name,
            fields = fields(),
            indices = expectedIndices()
        )

    private fun fields(): Map<String, FieldDefinition> =
        mapOf(
            "id" to FieldDefinition("id", "int"),
            "tenant_id" to FieldDefinition("tenant_id", "int"),
            "name" to FieldDefinition("name", "string"),
            "email" to FieldDefinition("email", "string")
        )

    private fun expectedIndices(): List<IndexDefinition> =
        listOf(
            IndexDefinition(
                name = "users_email_unique_idx",
                fields = listOf(IndexFieldDefinition("email")),
                unique = true
            ),
            IndexDefinition(
                name = "users_tenant_name_idx",
                fields = listOf(
                    IndexFieldDefinition("tenant_id"),
                    IndexFieldDefinition("name", IndexSortOrder.DESC)
                )
            )
        )

    private fun expectedIndicesWithPrimaryKey(primaryKeyIndexName: String): List<IndexDefinition> =
        (
            expectedIndices() + IndexDefinition(
                name = primaryKeyIndexName,
                fields = listOf(IndexFieldDefinition("id")),
                unique = true
            )
        ).sortedBy { it.name }

    private fun newSqliteDb(): String = sqliteDb("data-transfer-metadata")
}
