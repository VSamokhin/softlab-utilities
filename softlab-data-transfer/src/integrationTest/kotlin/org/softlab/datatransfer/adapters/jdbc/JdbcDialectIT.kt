package org.softlab.datatransfer.adapters.jdbc

import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.contains
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.softlab.datataset.test.initiators.JdbcInitiator
import org.softlab.datataset.test.initiators.TestJdbcPool
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.softlab.datatransfer.adapters.postgres.PostgresDialect
import org.softlab.datatransfer.core.IndexDefinition
import org.softlab.datatransfer.core.IndexFieldDefinition
import org.softlab.datatransfer.core.IndexSortOrder
import org.softlab.datatransfer.core.PrimaryKeyDefinition
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.Connection
import kotlin.test.assertContentEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue


@Testcontainers
class JdbcDialectIT {
    companion object {
        private const val CHANGELOG = "liquibase/changelog-jdbc-dialect.yaml"

        @Container
        @JvmStatic
        private val postgres = createPostgresContainer()

        private lateinit var postgresInitiator: JdbcInitiator
        private val sqlitePool = TestJdbcPool.create()

        @BeforeAll
        @JvmStatic
        fun setup() {
            postgresInitiator = JdbcInitiator(postgres.jdbcUrl, postgres.username, postgres.password)
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            postgresInitiator.close()
        }

        @JvmStatic
        fun provideDialects(): List<Arguments> {
            return listOf(
                Arguments.of(PostgresDialect(), postgresInitiator.getConnection()),
                //Arguments.of(SqliteDialect(), sqlitePool.openConnection())
            )
        }
    }

    @BeforeEach
    fun prepareDb() {
        postgresInitiator.cleanup()
        postgresInitiator.initSchema(CHANGELOG)
    }

    @ParameterizedTest
    @MethodSource("provideDialects")
    fun `listTables() returns JDBC tables`(dialect: JdbcDialect, connection: Connection) {
        connection.use {
            val tables = dialect.listTables(connection).map { it.table }.toList()

            assertTrue(tables.containsAll(listOf(
                "users",
                "products",
                "single_pk",
                "composite_pk",
                "no_key",
                "indexed_users"
            )))
            assertFalse(tables.contains("active_users"))
        }
    }

    @ParameterizedTest
    @MethodSource("provideDialects")
    fun `listColumns() returns column definitions from JDBC metadata`(dialect: JdbcDialect, connection: Connection) {
        connection.use {
            val columns = dialect.listColumns(JdbcTableName(null, "users"), connection)

            assertThat(columns.keys, contains("id", "name", "email"))
            assertThat(columns.map { it.value.name }, contains("id", "name", "email"))
            assertContentEquals(listOf("int4", "varchar", "varchar"), columns.map { it.value.type })
            assertContentEquals(listOf(false, true, true), columns.map { it.value.nullable })
            assertTrue(dialect.listColumns(JdbcTableName(null, "missing"), connection).isEmpty())
        }
    }

    @ParameterizedTest
    @MethodSource("provideDialects")
    fun `tableExists() returns table presence`(dialect: JdbcDialect, connection: Connection) {
        connection.use { connection ->
            assertTrue(dialect.tableExists(JdbcTableName(null, "users"), connection))
            assertTrue(dialect.tableExists(JdbcTableName(null, "products"), connection))
            assertFalse(dialect.tableExists(JdbcTableName(null, "missing"), connection))
        }
    }

    @ParameterizedTest
    @MethodSource("provideDialects")
    fun `readPrimaryKey() returns primary key metadata`(dialect: JdbcDialect, connection: Connection) {
        connection.use { connection ->
            assertThat(
                dialect.readPrimaryKey(JdbcTableName(null, "single_pk"), connection),
                equalTo(PrimaryKeyDefinition("single_pk_pkey", listOf("id")))
            )
            assertThat(
                dialect.readPrimaryKey(JdbcTableName(null, "composite_pk"), connection),
                equalTo(PrimaryKeyDefinition("composite_pk_pkey", listOf("tenant_id", "user_id")))
            )
            assertNull(dialect.readPrimaryKey(JdbcTableName(null, "no_key"), connection))
        }
    }

    @ParameterizedTest
    @MethodSource("provideDialects")
    fun `readIndices() returns secondary index metadata`(dialect: JdbcDialect, connection: Connection) {
        connection.use { connection ->
            assertContentEquals(
                listOf(
                    IndexDefinition(
                        name = "indexed_users_email_unique_idx",
                        fields = listOf(IndexFieldDefinition("email")),
                        unique = true
                    ),
                    IndexDefinition(
                        name = "indexed_users_name_desc_idx",
                        fields = listOf(IndexFieldDefinition("name", IndexSortOrder.DESC))
                    ),
                    IndexDefinition(
                        name = "indexed_users_tenant_name_idx",
                        fields = listOf(
                            IndexFieldDefinition("tenant_id"),
                            IndexFieldDefinition("name")
                        )
                    )
                ),
                dialect.readIndices(JdbcTableName(null, "indexed_users"), connection)
            )
            assertContentEquals(emptyList(), dialect.readIndices(JdbcTableName(null, "no_key"), connection))
        }
    }
}
