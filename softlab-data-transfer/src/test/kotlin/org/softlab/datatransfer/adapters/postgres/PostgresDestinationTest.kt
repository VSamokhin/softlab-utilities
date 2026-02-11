package org.softlab.datatransfer.adapters.postgres

import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.FieldMetadata
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.lang.Thread.sleep
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


@Testcontainers
class PostgresDestinationTest {
    companion object {
        @Container
        @JvmStatic
        private val postgres = PostgreSQLContainer(DockerImageName.parse("postgres:latest"))

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
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `class closes connection when configured`(closeConnection: Boolean) = runBlocking {
        getConnection().use { connection ->
            PostgresDestination(connection, closeConnection).use { destination ->
                destination.createCollection(
                    CollectionMetadata(
                        name = "public.users",
                        fields = listOf(FieldMetadata("id", "int"))
                    )
                )
            }
            assertEquals(closeConnection, connection.isClosed)
        }
    }

    @Test
    fun `createCollection() creates table with mapped column types`() = runBlocking {
            PostgresDestination(getConnection(), false).use { destination ->
                destination.createCollection(
                    CollectionMetadata(
                        name = "public.users",
                        fields = listOf(
                            FieldMetadata("id", "int"),
                            FieldMetadata("name", "string"),
                            FieldMetadata("active", "boolean"),
                            FieldMetadata("notes", "custom")
                        )
                    )
                )
            }

            val columnTypes = getConnection().use { conn->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery(
                        """
                            SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_schema = 'public' AND table_name = 'users';
                            """.trimIndent()
                    ).use { rs ->
                        val result = mutableMapOf<String, String>()
                        while (rs.next()) {
                            result[rs.getString("column_name")] = rs.getString("data_type")
                        }
                        result
                    }
                }
            }

            assertEquals("integer", columnTypes["id"])
            assertEquals("text", columnTypes["name"])
            assertEquals("boolean", columnTypes["active"])
            assertEquals("text", columnTypes["notes"])
    }

    @Test
    fun `insertDocuments() writes expected data`() = runBlocking {
        PostgresDestination(getConnection()).use { destination ->
            destination.createCollection(
                CollectionMetadata(
                    name = "public.users",
                    fields = listOf(
                        FieldMetadata("id", "int"),
                        FieldMetadata("name", "string"),
                        FieldMetadata("active", "boolean")
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
}
