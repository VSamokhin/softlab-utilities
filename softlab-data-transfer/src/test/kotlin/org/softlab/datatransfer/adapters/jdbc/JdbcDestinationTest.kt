package org.softlab.datatransfer.adapters.jdbc

import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.jdbc.withConnection
import org.softlab.dataset.jdbc.withStatement
import org.softlab.datatransfer.core.CollectionMetadata
import java.sql.SQLException
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class JdbcDestinationTest {
    @Test
    fun `getBackendName() returns dialect backend name`() {
        TestJdbcProvider.create().use { provider ->
            val destination = JdbcDestination(provider, TestJdbcDialect("test-backend"), emptyMap(), false)

            assertEquals("test-backend", destination.getBackendName())
        }
    }

    @Test
    fun `createCollection() creates table with mapped field types and nullability`() {
        TestJdbcProvider.create().use { provider ->
            val dialect = TestJdbcDialect()

            runBlocking {
                JdbcDestination(provider, dialect, mapOf("string" to "TEXT"), false).createCollection(
                    CollectionMetadata(
                        "users",
                        listOf(
                            FieldDefinition("id", "int", false),
                            FieldDefinition("name", "string", true)
                        )
                    )
                )
            }

            provider.withConnection { connection ->
                assertTrue(dialect.tableExists(JdbcTableName(null, "users"), connection))
                val columns = dialect.listColumns(JdbcTableName(null, "users"), connection)
                assertContentEquals(listOf("int", "text"), columns.map { it.type })
                assertFalse(columns.first { it.name == "id" }.nullable)
                assertTrue(columns.first { it.name == "name" }.nullable)
            }
            assertContentEquals(listOf(JdbcTableName(null, "users")), dialect.beforeCreateTableCalls)
        }
    }

    @Test
    fun `createCollection() throws when table exists`() {
        TestJdbcProvider.create().use { provider ->
            val destination = JdbcDestination(provider, TestJdbcDialect(), emptyMap(), false)
            val metadata = CollectionMetadata("users", listOf(FieldDefinition("id", "int")))

            runBlocking { destination.createCollection(metadata) }

            val exc = assertThrows<SQLException> {
                runBlocking { destination.createCollection(metadata) }
            }
            assertThat(exc.message, containsString("users"))
        }
    }

    @Test
    fun `dropCollection() drops table and ignores absent table`() {
        TestJdbcProvider.create().use { provider ->
            val destination = JdbcDestination(provider, TestJdbcDialect(), emptyMap(), false)
            runBlocking {
                destination.createCollection(CollectionMetadata("users", listOf(FieldDefinition("id", "int"))))
                destination.dropCollection("users")
            }

            provider.withConnection { connection ->
                assertFalse(TestJdbcDialect().tableExists(JdbcTableName(null, "users"), connection))
            }
            assertDoesNotThrow { runBlocking { destination.dropCollection("users") } }
        }
    }

    @Test
    fun `insertDocuments() writes data using table column order`() {
        TestJdbcProvider.create().use { provider ->
            val destination = JdbcDestination(provider, TestJdbcDialect(), mapOf("string" to "TEXT"), false)
            runBlocking {
                destination.createCollection(
                    CollectionMetadata(
                        "users",
                        listOf(FieldDefinition("id", "int"), FieldDefinition("name", "string", true))
                    )
                )

                destination.insertDocuments(
                    "users",
                    flowOf(
                        mapOf("name" to "Alice", "id" to 1),
                        mapOf("ID" to 2, "NAME" to "Bob")
                    )
                )
            }

            provider.withStatement { statement ->
                statement.executeQuery("SELECT id, name FROM users ORDER BY id;").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(1, rs.getInt("id"))
                    assertEquals("Alice", rs.getString("name"))
                    assertTrue(rs.next())
                    assertEquals(2, rs.getInt("id"))
                    assertEquals("Bob", rs.getString("name"))
                    assertFalse(rs.next())
                }
            }
        }
    }

    @Test
    fun `insertDocuments() writes null for missing fields`() {
        TestJdbcProvider.create().use { provider ->
            val destination = JdbcDestination(provider, TestJdbcDialect(), mapOf("string" to "TEXT"), false)
            runBlocking {
                destination.createCollection(
                    CollectionMetadata(
                        "users",
                        listOf(FieldDefinition("id", "int"), FieldDefinition("name", "string", true))
                    )
                )

                destination.insertDocuments("users", flowOf(mapOf("id" to 1)))
            }

            provider.withStatement { statement ->
                statement.executeQuery("SELECT id, name FROM users;").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(1, rs.getInt("id"))
                    assertEquals(null, rs.getString("name"))
                }
            }
        }
    }

    @Test
    fun `insertDocuments() throws when document has more fields than table`() {
        TestJdbcProvider.create().use { provider ->
            val destination = JdbcDestination(
                provider,
                TestJdbcDialect(),
                emptyMap(),
                false
            )
            runBlocking {
                destination.createCollection(
                    CollectionMetadata(
                        "users",
                        listOf(FieldDefinition("id", "int"))
                    )
                )
            }

            val exc = assertThrows<IllegalStateException> {
                runBlocking {
                    destination.insertDocuments(
                        "users",
                        flowOf(mapOf("id" to 1, "name" to "Alice"))
                    )
                }
            }

            assertThat(exc.message, allOf(
                containsString("users"),
                containsString("table=id"),
                containsString("row=id, name")
            ))
        }
    }

    @Test
    fun `insertDocuments() writes non-concurrent batches when dialect disallows concurrent inserts`() {
        TestJdbcProvider.create().use { provider ->
            val destination = JdbcDestination(
                provider,
                TestJdbcDialect(supportsConcurrentInserts = false),
                emptyMap(),
                false
            )
            runBlocking {
                destination.createCollection(
                    CollectionMetadata(
                        "users",
                        listOf(FieldDefinition("id", "int"))
                    )
                )

                destination.insertDocuments(
                    "users",
                    flow {
                        repeat(2) { index -> emit(mapOf("id" to index)) }
                    }
                )
                val count = JdbcSource(
                    provider,
                    TestJdbcDialect(),
                    false
                ).countDocuments("users")
                assertEquals(2, count)
            }
        }
    }

    @Test
    fun `close() closes provider only when configured`() {
        TestJdbcProvider.create().use { provider ->
            JdbcDestination(provider, TestJdbcDialect(), emptyMap(), false).close()
            assertFalse(provider.closed)
            JdbcDestination(provider, TestJdbcDialect(), emptyMap(), true).close()
            assertTrue(provider.closed)
        }
    }
}
