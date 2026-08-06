package org.softlab.datatransfer.adapters.postgres

import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.contains
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.postgresql.util.PSQLException
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.jdbc.withConnection
import org.softlab.dataset.jdbc.withStatement
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.softlab.datatransfer.adapters.jdbc.JdbcTableName
import org.softlab.datatransfer.config.ConfigProvider
import org.softlab.datatransfer.config.DataTypeMappings
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.IndexDefinition
import org.softlab.datatransfer.core.IndexFieldDefinition
import org.softlab.datatransfer.core.IndexSortOrder
import org.softlab.datatransfer.core.PrimaryKeyDefinition
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.SQLException
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue


@Testcontainers
class PostgresDestinationIT {
    companion object {
        @Container
        @JvmStatic
        private val postgres = createPostgresContainer()

        private val dataTypeMappings = ConfigProvider.config.getDataTypeMappings()

        private fun getPool(): PostgresConnectionPool =
            PostgresConnectionPool(
                postgres.jdbcUrl,
                postgres.username,
                postgres.password
            )

        private inline fun <T> withPool(block: (PostgresConnectionPool) -> T): T =
            getPool().use(block)
    }

    @BeforeEach
    fun resetSchema() {
        getPool().withStatement { stmt ->
            stmt.execute("DROP TABLE IF EXISTS public.users;")
            stmt.execute("DROP TABLE IF EXISTS custom.users;")
            stmt.execute("DROP SCHEMA IF EXISTS custom CASCADE;")
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `class closes connection when configured`(closeConnection: Boolean) {
        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings, closeConnection).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "public.users",
                            fields = mapOf("id" to FieldDefinition("id", "int"))
                        )
                    )
                }
            }
            assertEquals(closeConnection, pool.closed)
        }
    }

    @Test
    fun `createCollection() creates table with mapped column types`()  {
        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "public.users",
                            fields = mapOf(
                                "id" to FieldDefinition("id", "int", false),
                                "name" to FieldDefinition("name", "string"),
                                "active" to FieldDefinition("active", "boolean", false)
                            )
                        )
                    )
                }

                val columns = pool.withConnection {
                    PostgresDialect().listColumns(JdbcTableName("public", "users"), it)
                }

                assertThat(columns.values, contains(
                        FieldDefinition("id", "int4", false),
                        FieldDefinition("name", "text"),
                        FieldDefinition("active", "bool", false)
                    )
                )
            }
        }
    }

    @Test
    fun `createCollection() throws when table already exists`() {
        PostgresDestination(getPool(), dataTypeMappings).use { destination ->
            val metadata = CollectionMetadata(
                name = "public.users",
                fields = mapOf("id" to FieldDefinition("id", "int"))
            )

            runBlocking {
                destination.createCollection(metadata)

                val exc = assertThrows<SQLException> {
                    destination.createCollection(metadata)
                }
                assertThat(exc.message, containsString("users"))
            }
        }
    }

    @Test
    fun `createCollection() uses provided data type mappings`()  {
        withPool { pool ->
            PostgresDestination(
                pool,
                PostgresDialect(),
                DataTypeMappings(mapOf("custom_type" to "TEXT")),
                false
            ).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "public.users",
                            fields = mapOf("notes" to FieldDefinition("notes", "custom_type"))
                        )
                    )
                }
            }

            val columns = pool.withConnection {
                PostgresDialect().listColumns(JdbcTableName("public", "users"), it)
            }
            assertThat(columns.values, contains(FieldDefinition("notes", "text")))
        }
    }

    @Test
    fun `createCollection() causes postgres to throw for unknow field type`() {
        PostgresDestination(getPool(), dataTypeMappings).use { destination ->
            val col = CollectionMetadata("public.users",
                mapOf("notes" to FieldDefinition("notes", "custom"))
            )

            val exc = assertThrows<PSQLException> {
                runBlocking { destination.createCollection(col) }
            }
            assertThat(exc.message, containsString("custom"))
        }
    }

    @Test
    fun `createCollection() works when no columns`() {
        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata("public.no_columns", emptyMap())
                    )
                }
            }

            val dialect = PostgresDialect()
            pool.withConnection { conn ->
                assertTrue(dialect.tableExists(
                    JdbcTableName("public", "no_columns"), conn
                ))
                assertTrue(dialect.listColumns(
                    JdbcTableName("public", "no_columns"), conn
                ).isEmpty())
            }
        }
    }

    @Test
    fun `createCollection() creates missing schema automatically`() {
        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "custom.users",
                            fields = mapOf("id" to FieldDefinition("id", "int"))
                        )
                    )
                }
            }

            pool.withConnection { conn ->
                assertTrue(PostgresDialect().tableExists(
                    JdbcTableName("custom", "users"), conn)
                )
            }
        }
    }

    @Test
    fun `createIndexesAndConstraints() creates primary key and indices`() {
        val metadata = usersMetadataWithPrimaryKeyAndIndices()

        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(metadata)
                    destination.insertDocuments(
                        metadata.name,
                        flowOf(
                            mapOf("id" to 1, "tenant_id" to 1, "name" to "Alice", "email" to "alice@example.com"),
                            mapOf("id" to 2, "tenant_id" to 1, "name" to "Bob", "email" to "bob@example.com")
                        )
                    )
                    destination.createIndexesAndConstraints(metadata)
                }
            }

            pool.withConnection { conn ->
                val dialect = PostgresDialect()
                val tableName = JdbcTableName("public", "users")

                assertEquals(metadata.primaryKey, dialect.readPrimaryKey(tableName, conn))
                assertContentEquals(metadata.indices, dialect.readIndices(tableName, conn))
            }
        }
    }

    @Test
    fun `createIndexesAndConstraints() does nothing when metadata has no definitions`() {
        val metadata = CollectionMetadata(
            name = "public.users",
            fields = mapOf("id" to FieldDefinition("id", "int"))
        )

        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(metadata)
                    destination.createIndexesAndConstraints(metadata)
                }
            }

            pool.withConnection { conn ->
                val tableName = JdbcTableName("public", "users")

                assertNull(PostgresDialect().readPrimaryKey(tableName, conn))
                assertContentEquals(emptyList(), PostgresDialect().readIndices(tableName, conn))
            }
        }
    }

    @Test
    fun `createIndexesAndConstraints() uses fallback names when metadata names are absent`() {
        val metadata = CollectionMetadata(
            name = "public.users",
            fields = mapOf(
                "id" to FieldDefinition("id", "int"),
                "email" to FieldDefinition("email", "string")
            ),
            primaryKey = PrimaryKeyDefinition(null, listOf("id")),
            indices = listOf(
                IndexDefinition(
                    name = null,
                    fields = listOf(IndexFieldDefinition("email"))
                )
            )
        )

        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(metadata)
                    destination.createIndexesAndConstraints(metadata)
                }
            }

            pool.withConnection { conn ->
                val tableName = JdbcTableName("public", "users")

                assertEquals(
                    PrimaryKeyDefinition("users_pkey", listOf("id")),
                    PostgresDialect().readPrimaryKey(tableName, conn)
                )
                assertContentEquals(
                    listOf(
                        IndexDefinition(
                            name = "users_email_idx",
                            fields = listOf(IndexFieldDefinition("email"))
                        )
                    ),
                    PostgresDialect().readIndices(tableName, conn)
                )
            }
        }
    }

    @Test
    fun `createIndexesAndConstraints() fails when primary key data has duplicates`() {
        val metadata = CollectionMetadata(
            name = "public.users",
            fields = mapOf(
                "id" to FieldDefinition("id", "int"),
                "name" to FieldDefinition("name", "string")
            ),
            primaryKey = PrimaryKeyDefinition("users_pkey", listOf("id"))
        )

        PostgresDestination(getPool(), dataTypeMappings).use { destination ->
            runBlocking {
                destination.createCollection(metadata)
                destination.insertDocuments(
                    metadata.name,
                    flowOf(
                        mapOf("id" to 1, "name" to "Alice"),
                        mapOf("id" to 1, "name" to "Bob")
                    )
                )

                assertThrows<PSQLException> {
                    destination.createIndexesAndConstraints(metadata)
                }
            }
        }
    }

    @Test
    fun `createIndexesAndConstraints() fails when unique index data has duplicates`() {
        val metadata = CollectionMetadata(
            name = "public.users",
            fields = mapOf(
                "id" to FieldDefinition("id", "int"),
                "email" to FieldDefinition("email", "string")
            ),
            indices = listOf(
                IndexDefinition(
                    name = "users_email_unique_idx",
                    fields = listOf(IndexFieldDefinition("email")),
                    unique = true
                )
            )
        )

        PostgresDestination(getPool(), dataTypeMappings).use { destination ->
            runBlocking {
                destination.createCollection(metadata)
                destination.insertDocuments(
                    metadata.name,
                    flowOf(
                        mapOf("id" to 1, "email" to "duplicate@example.com"),
                        mapOf("id" to 2, "email" to "duplicate@example.com")
                    )
                )

                assertThrows<PSQLException> {
                    destination.createIndexesAndConstraints(metadata)
                }
            }
        }
    }

    @Test
    fun `getBackendName() returns postgres`() {
        PostgresDestination(getPool(), dataTypeMappings, false).use { destination ->
            assertEquals("postgres", destination.getBackendName())
        }
    }

    @Test
    fun `dropCollection() drops table with all data`() {
        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "public.users",
                            fields = mapOf(
                                "id" to FieldDefinition("id", "int"),
                                "name" to FieldDefinition("name", "string")
                            )
                        )
                    )
                    destination.insertDocuments(
                        "public.users",
                        flowOf(
                            mapOf("id" to 1, "name" to "Alice"),
                            mapOf("id" to 2, "name" to "Bob")
                        )
                    )

                    destination.dropCollection("public.users")
                }

                pool.withConnection { conn ->
                    assertFalse(PostgresDialect().tableExists(JdbcTableName("public", "users"), conn))
                }
            }
        }
    }

    @Test
    fun `dropCollection() doesn't fail if table not exists`() {
        PostgresDestination(getPool(), dataTypeMappings).use { destination ->
            assertDoesNotThrow { runBlocking { destination.dropCollection("public.something") } }
        }
    }

    @Test
    fun `insertDocuments() writes expected data`() {
        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "public.users",
                            fields = mapOf(
                                "id" to FieldDefinition("id", "int"),
                                "name" to FieldDefinition("name", "string"),
                                "active" to FieldDefinition("active", "boolean")
                            )
                        )
                    )

                    destination.insertDocuments(
                        "public.users",
                        flowOf(
                            mapOf("id" to 1, "name" to "Alice", "active" to true),
                            mapOf("id" to 2, "name" to "Bob", "active" to false)
                        )
                    )
                }
            }

            pool.withStatement { stmt ->
                stmt.executeQuery("SELECT id, name, active FROM public.users ORDER BY id;").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(1, rs.getInt("id"))
                    assertEquals("Alice", rs.getString("name"))
                    assertEquals(true, rs.getBoolean("active"))

                    assertTrue(rs.next())
                    assertEquals(2, rs.getInt("id"))
                    assertEquals("Bob", rs.getString("name"))
                    assertEquals(false, rs.getBoolean("active"))

                    assertFalse(rs.next())
                }
            }
        }
    }

    @Test
    fun `insertDocuments() casts integer values to booleans for boolean columns`() {
        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "public.users",
                            fields = mapOf(
                                "id" to FieldDefinition("id", "int"),
                                "active" to FieldDefinition("active", "boolean")
                            )
                        )
                    )

                    destination.insertDocuments(
                        "public.users",
                        flowOf(
                            mapOf("id" to 1, "active" to 1),
                            mapOf("id" to 2, "active" to 0)
                        )
                    )
                }
            }

            pool.withStatement { stmt ->
                stmt.executeQuery("SELECT id, active FROM public.users ORDER BY id;").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(1, rs.getInt("id"))
                    assertEquals(true, rs.getBoolean("active"))

                    assertTrue(rs.next())
                    assertEquals(2, rs.getInt("id"))
                    assertEquals(false, rs.getBoolean("active"))

                    assertFalse(rs.next())
                }
            }
        }
    }

    @Test
    fun `insertDocuments() writes null for missing nullable fields`() {
        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings, closePool = false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "public.users",
                            fields = mapOf(
                                "id" to FieldDefinition("id", "int", false),
                                "name" to FieldDefinition("name", "string", true),
                                "active" to FieldDefinition("active", "boolean", true)
                            )
                        )
                    )

                    destination.insertDocuments(
                        "public.users",
                        flowOf(
                            mapOf("id" to 1)
                        )
                    )
                }
            }

            pool.withStatement { stmt ->
                stmt.executeQuery("SELECT id, name, active FROM public.users;").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(1, rs.getInt("id"))
                    assertEquals(null, rs.getString("name"))
                    assertEquals(false, rs.getBoolean("active"))
                    assertTrue(rs.wasNull())
                    assertFalse(rs.next())
                }
            }
        }
    }


    @Test
    fun `insertDocuments() throws when document has more fields than table`() {
        PostgresDestination(getPool(), dataTypeMappings).use { destination ->
            runBlocking {
                destination.createCollection(
                    CollectionMetadata(
                        name = "public.users",
                        fields = mapOf("id" to FieldDefinition("id", "int"))
                    )
                )

                val exc = assertThrows<IllegalStateException> {
                    destination.insertDocuments(
                        "public.users",
                        flowOf(
                            mapOf("id" to 1, "extra" to "unexpected")
                        )
                    )
                }
                assertThat(exc.message, allOf(
                    containsString("public.users"),
                    containsString("table=id"),
                    containsString("row=id, extra")
                ))
            }
        }
    }

    private fun usersMetadataWithPrimaryKeyAndIndices(): CollectionMetadata =
        CollectionMetadata(
            name = "public.users",
            fields = mapOf(
                "id" to FieldDefinition("id", "int"),
                "tenant_id" to FieldDefinition("tenant_id", "int"),
                "name" to FieldDefinition("name", "string"),
                "email" to FieldDefinition("email", "string")
            ),
            primaryKey = PrimaryKeyDefinition("users_pkey", listOf("id")),
            indices = listOf(
                IndexDefinition(
                    name = "users_email_unique_idx",
                    fields = listOf(IndexFieldDefinition("email")),
                    unique = true
                ),
                IndexDefinition(
                    name = "users_name_desc_idx",
                    fields = listOf(IndexFieldDefinition("name", IndexSortOrder.DESC))
                ),
                IndexDefinition(
                    name = "users_tenant_name_idx",
                    fields = listOf(
                        IndexFieldDefinition("tenant_id"),
                        IndexFieldDefinition("name")
                    )
                )
            )
        )
}
