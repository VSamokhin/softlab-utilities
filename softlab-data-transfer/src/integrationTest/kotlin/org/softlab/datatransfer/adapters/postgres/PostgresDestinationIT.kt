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
import org.softlab.dataset.jdbc.JdbcConnectionProvider
import org.softlab.dataset.jdbc.withConnection
import org.softlab.dataset.jdbc.withStatement
import org.softlab.datataset.test.initiators.createPostgresContainer
import org.softlab.datatransfer.config.ConfigProvider
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.util.Postgres
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.SQLException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


@Testcontainers
class PostgresDestinationIT {
    companion object {
        @Container
        @JvmStatic
        private val postgres = createPostgresContainer()

        private val dataTypeMappings = ConfigProvider.config
            .getDataTypeMappings()
            .destination(PostgresDestination.BACKEND)

        private fun getPool(): JdbcConnectionProvider =
            ConnectionPool(
                postgres.jdbcUrl,
                postgres.username,
                postgres.password
            )

        private inline fun <T> withPool(block: (JdbcConnectionProvider) -> T): T =
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
                            fields = listOf(FieldDefinition("id", "int"))
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
                            fields = listOf(
                                FieldDefinition("id", "int", false),
                                FieldDefinition("name", "string"),
                                FieldDefinition("active", "boolean", false)
                            )
                        )
                    )
                }

                val columns = pool.withConnection {
                    Postgres.readColumns("public", "users", it)
                }

                assertThat(
                    columns, contains(
                        FieldDefinition("id", "integer", false),
                        FieldDefinition("name", "text"),
                        FieldDefinition("active", "boolean", false)
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
                fields = listOf(FieldDefinition("id", "int"))
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
            PostgresDestination(pool, mapOf("custom_type" to "TEXT"), false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "public.users",
                            fields = listOf(FieldDefinition("notes", "custom_type"))
                        )
                    )
                }
            }

            val columns = pool.withConnection {
                Postgres.readColumns("public", "users", it)
            }
            assertThat(columns, contains(FieldDefinition("notes", "text")))
        }
    }

    @Test
    fun `createCollection() causes postgres to throw for unknow field type`() {
        PostgresDestination(getPool(), dataTypeMappings).use { destination ->
            val col = CollectionMetadata("public.users",
                listOf(FieldDefinition("notes", "custom"))
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
            PostgresDestination(pool, dataTypeMappings, closePool = false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata("public.no_columns", emptyList())
                    )
                }
            }

            pool.withConnection { conn ->
                assertTrue(Postgres.tableExists("public", "no_columns", conn))
                assertTrue(Postgres.readColumns("public", "no_columns", conn).isEmpty())
            }
        }
    }

    @Test
    fun `createCollection() creates missing schema automatically`() {
        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings, closePool = false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "custom.users",
                            fields = listOf(FieldDefinition("id", "int"))
                        )
                    )
                }
            }

            pool.withConnection { conn ->
                assertTrue(Postgres.tableExists("custom", "users", conn))
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
                            fields = listOf(
                                FieldDefinition("id", "int"),
                                FieldDefinition("name", "string")
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
                    assertFalse(Postgres.tableExists("public", "users", conn))
                }
            }
        }
    }

    @Test
    fun `dropCollection() doesn't fail if table not exists`() = runBlocking {
        PostgresDestination(getPool(), dataTypeMappings).use { destination ->
            assertDoesNotThrow {  destination.dropCollection("public.something") }
        }
    }

    @Test
    fun `insertDocuments() writes expected data`() {
        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings, closePool = false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "public.users",
                            fields = listOf(
                                FieldDefinition("id", "int"),
                                FieldDefinition("name", "string"),
                                FieldDefinition("active", "boolean")
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
    fun `insertDocuments() writes null for missing nullable fields`() {
        withPool { pool ->
            PostgresDestination(pool, dataTypeMappings, closePool = false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "public.users",
                            fields = listOf(
                                FieldDefinition("id", "int", false),
                                FieldDefinition("name", "string", true),
                                FieldDefinition("active", "boolean", true)
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
                        fields = listOf(FieldDefinition("id", "int"))
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
}
