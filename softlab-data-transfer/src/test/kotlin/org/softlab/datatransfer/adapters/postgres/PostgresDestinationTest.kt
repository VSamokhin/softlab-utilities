package org.softlab.datatransfer.adapters.postgres

import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.contains
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.softlab.dataset.core.FieldDefinition
import org.softlab.datatransfer.config.ConfigProvider
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.util.Postgres
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.lang.Thread.sleep
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


@Testcontainers
class PostgresDestinationTest {
    companion object {
        @Container
        @JvmStatic
        private val postgres = PostgreSQLContainer(DockerImageName.parse("postgres:latest"))

        private val dataTypeMappings = ConfigProvider.config
            .getDataTypeMappings()
            .destination(PostgresDestination.BACKEND)

        private fun getConnection(): Connection =
            DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)

        @BeforeAll
        @JvmStatic
        fun setup() {
            // Workaround for Rancher Desktop on Mac, somehow the container is not ready while the tests start
            val isMac = System.getProperty("os.name").contains("Mac", ignoreCase = true)
            if (isMac) sleep(3000) // Wait for the container to be ready
        }
    }

    @BeforeEach
    fun resetSchema() {
        getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("DROP TABLE IF EXISTS public.users;")
                stmt.execute("DROP TABLE IF EXISTS custom.users;")
                stmt.execute("DROP SCHEMA IF EXISTS custom CASCADE;")
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `class closes connection when configured`(closeConnection: Boolean) = runBlocking {
        getConnection().use { connection ->
            PostgresDestination(connection, dataTypeMappings, closeConnection).use { destination ->
                destination.createCollection(
                    CollectionMetadata(
                        name = "public.users",
                        fields = listOf(FieldDefinition("id", "int"))
                    )
                )
            }
            assertEquals(closeConnection, connection.isClosed)
        }
    }

    @Test
    fun `createCollection() creates table with mapped column types`() = runBlocking {
            PostgresDestination(getConnection(), dataTypeMappings, false).use { destination ->
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

            val columns = getConnection().use {
                Postgres.readColumns("public", "users", it)
            }

            assertThat(columns, contains(
                FieldDefinition("id", "integer", false),
                FieldDefinition("name", "text"),
                FieldDefinition("active", "boolean", false)
            ))
    }

    @Test
    fun `createCollection() throws when table already exists`() = runBlocking {
        PostgresDestination(getConnection(), dataTypeMappings, false).use { destination ->
            val metadata = CollectionMetadata(
                name = "public.users",
                fields = listOf(FieldDefinition("id", "int"))
            )

            destination.createCollection(metadata)

            val exc = assertThrows<SQLException> {
                destination.createCollection(metadata)
            }
            assertThat(exc.message, containsString("users"))
        }
    }

    @Test
    fun `getBackendName() returns postgres`() {
        PostgresDestination(getConnection(), dataTypeMappings, false).use { destination ->
            assertEquals("postgres", destination.getBackendName())
        }
    }

    @Test
    fun `createCollection() uses provided data type mappings`() = runBlocking {
        PostgresDestination(
            connection = getConnection(),
            closeConnection = false,
            dataTypeMappings = mapOf("custom_type" to "TEXT")
        ).use { destination ->
            destination.createCollection(
                CollectionMetadata(
                    name = "public.users",
                    fields = listOf(FieldDefinition("notes", "custom_type"))
                )
            )
        }

        val columns = getConnection().use {
            Postgres.readColumns("public", "users", it)
        }
        assertThat(columns, contains(FieldDefinition("notes", "text")))
    }

        @Test
        fun `createCollection() throws for unknow field type`() {
            PostgresDestination(getConnection(), dataTypeMappings, false).use { destination ->
                val col = CollectionMetadata("public.users",
                    listOf(FieldDefinition("notes", "custom")))

                val exc = assertThrows<IllegalStateException> {
                    runBlocking { destination.createCollection(col) }
                }
                assertContains(exc.message!!, "custom")
            }
        }

    @Test
    fun `createCollection() works when no columns`() = runBlocking {
        PostgresDestination(
            postgres.jdbcUrl,
            postgres.username,
            postgres.password,
            dataTypeMappings
        ).use { destination ->
             destination.createCollection(CollectionMetadata("public.no_columns", emptyList()))
        }

        getConnection().use { conn ->
            assertTrue(Postgres.tableExists("public", "no_columns", conn))
            assertTrue(Postgres.readColumns("public", "no_columns", conn).isEmpty())
        }
    }

    @Test
    fun `createCollection() creates missing schema automatically`() = runBlocking {
        PostgresDestination(getConnection(), dataTypeMappings).use { destination ->
            destination.createCollection(
                CollectionMetadata(
                    name = "custom.users",
                    fields = listOf(FieldDefinition("id", "int"))
                )
            )
        }

        getConnection().use { conn ->
            assertTrue(Postgres.tableExists("custom", "users", conn))
        }
    }

    @Test
    fun `dropCollection() drops table with all data`() = runBlocking {
        PostgresDestination(getConnection(), dataTypeMappings).use { destination ->
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

        getConnection().use { conn ->
            assertFalse(Postgres.tableExists("public", "users", conn))
        }
    }

    @Test
    fun `dropCollection() doesn't fail if table not exists`() = runBlocking {
        PostgresDestination(getConnection(), dataTypeMappings).use { destination ->
            assertDoesNotThrow {  destination.dropCollection("public.something") }
        }
    }

    @Test
    fun `insertDocuments() writes expected data`() = runBlocking {
        PostgresDestination(getConnection(), dataTypeMappings).use { destination ->
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

        getConnection().use { conn ->
            conn.createStatement().use { stmt ->
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
    fun `insertDocuments() writes null for missing nullable fields`() = runBlocking {
        PostgresDestination(getConnection(), dataTypeMappings).use { destination ->
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

        getConnection().use { conn ->
            conn.createStatement().use { stmt ->
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
    fun `insertDocuments() throws when document has more fields than table`() = runBlocking {
        PostgresDestination(getConnection(), dataTypeMappings).use { destination ->
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
