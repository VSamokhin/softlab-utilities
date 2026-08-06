package org.softlab.datatransfer.adapters.jdbc

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
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
import org.softlab.dataset.jdbc.JdbcConnectionPool
import org.softlab.dataset.jdbc.withConnection
import org.softlab.dataset.jdbc.withStatement
import org.softlab.datataset.test.initiators.TestJdbcPool
import org.softlab.datataset.test.initiators.withTestPool
import org.softlab.datatransfer.config.DataTypeMappings
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.SqlTableName
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class JdbcDestinationTest {
    @Test
    fun `getBackendName() returns dialect backend name`() {
        withTestPool { pool ->
            val destination = JdbcDestination(pool, TestJdbcDialect("test-backend"), DataTypeMappings(), false)

            assertEquals("test-backend", destination.getBackendName())
        }
    }

    @Test
    fun `createCollection() creates table with mapped field types and nullability`() {
        withTestPool { pool ->
            val dialect = TestJdbcDialect()

            runBlocking {
                JdbcDestination(pool, dialect, DataTypeMappings(mapOf("string" to "TEXT")), false).createCollection(
                    CollectionMetadata(
                        "users",
                        mapOf(
                            "id" to FieldDefinition("id", "int", false),
                            "name" to FieldDefinition("name", "string", true)
                        )
                    )
                )
            }

            pool.withConnection { connection ->
                assertTrue(dialect.tableExists(SqlTableName(null, "users"), connection))
                val columns = dialect.listColumns(SqlTableName(null, "users"), connection)
                assertContentEquals(listOf("INT", "TEXT"), columns.map { it.value.type })
                assertContentEquals(listOf(false, true), columns.map { it.value.nullable })
            }
            assertContentEquals(
                listOf(SqlTableName(null, "users")), dialect.beforeCreateTableCalls
            )
        }
    }

    @Test
    fun `createCollection() throws when table exists`() {
        withTestPool { pool ->
            val destination = JdbcDestination(pool, TestJdbcDialect(), DataTypeMappings(), false)
            val metadata = CollectionMetadata(
                "users",
                mapOf("id" to FieldDefinition("id", "int"))
            )

            runBlocking { destination.createCollection(metadata) }

            val exc = assertThrows<SQLException> {
                runBlocking { destination.createCollection(metadata) }
            }
            assertThat(exc.message, containsString("users"))
        }
    }

    @Test
    fun `dropCollection() drops table and ignores absent table`() {
        withTestPool { pool ->
            val destination = JdbcDestination(pool, TestJdbcDialect(), DataTypeMappings(), false)
            runBlocking {
                destination.createCollection(
                    CollectionMetadata(
                        "users",
                        mapOf("id" to FieldDefinition("id", "int")))
                )
                destination.dropCollection("users")
            }

            pool.withConnection { connection ->
                assertFalse(
                    TestJdbcDialect().tableExists(
                        SqlTableName(null, "users"), connection
                    )
                )
            }
            assertDoesNotThrow { runBlocking { destination.dropCollection("users") } }
        }
    }

    @Test
    fun `insertDocuments() writes data using table column order`() {
        withTestPool { pool ->
            val destination = JdbcDestination(
                pool,
                TestJdbcDialect(),
                DataTypeMappings(mapOf("string" to "TEXT")),
                false
            )
            runBlocking {
                destination.createCollection(
                    CollectionMetadata(
                        "users",
                        mapOf(
                            "id" to FieldDefinition("id", "int"),
                            "name" to FieldDefinition("name", "string", true)
                        )
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

            pool.withStatement { statement ->
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
        withTestPool { pool ->
            val destination = JdbcDestination(
                pool,
                TestJdbcDialect(),
                DataTypeMappings(mapOf("string" to "TEXT")),
                false
            )
            runBlocking {
                destination.createCollection(
                    CollectionMetadata(
                        "users",
                        mapOf(
                            "id" to FieldDefinition("id", "int"),
                            "name" to FieldDefinition("name", "string", true)
                        )
                    )
                )

                destination.insertDocuments("users", flowOf(mapOf("id" to 1)))
            }

            pool.withStatement { statement ->
                statement.executeQuery("SELECT id, name FROM users;").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(1, rs.getInt("id"))
                    assertEquals(null, rs.getString("name"))
                }
            }
        }
    }

    @Test
    fun `insertDocuments() casts integer values to booleans for boolean target columns`() {
        val connection = mockk<Connection>(relaxed = true)
        val statement = mockk<PreparedStatement>(relaxed = true)
        val pool = object : JdbcConnectionPool {
            override fun openConnection(): Connection = connection
            override val closed: Boolean get() = false
            override fun close() = Unit
        }
        val dialect = object : JdbcDialect("jdbc-test", supportsConcurrentInserts = false) {
            override fun parseTableName(tableName: String): SqlTableName = SqlTableName(null, tableName)

            override fun listColumns(tableName: SqlTableName, connection: Connection): Map<String, FieldDefinition> =
                mapOf(
                    "id" to FieldDefinition("id", "integer"),
                    "active" to FieldDefinition("active", "boolean")
                )
        }
        every { connection.prepareStatement(any()) } returns statement
        every { statement.executeBatch() } returns intArrayOf(1, 1)

        val destination = object : JdbcDestination(pool, dialect, DataTypeMappings(), false) {
            override fun castValue(type: String, value: Any?): Any? =
                if (type == "boolean" && value is Int) value != 0 else value
        }

        runBlocking {
            destination.insertDocuments(
                "users",
                flowOf(
                    mapOf("id" to 1, "active" to 1),
                    mapOf("id" to 2, "active" to 0)
                )
            )
        }

        verify { statement.setObject(2, true) }
        verify { statement.setObject(2, false) }
    }

    @Test
    fun `insertDocuments() throws when document has more fields than table`() {
        TestJdbcPool.create().use { pool ->
            val destination = JdbcDestination(
                pool,
                TestJdbcDialect(),
                DataTypeMappings(),
                false
            )
            runBlocking {
                destination.createCollection(
                    CollectionMetadata(
                        "users",
                        mapOf("id" to FieldDefinition("id", "int"))
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
        TestJdbcPool.create().use { pool ->
            val destination = JdbcDestination(
                pool,
                TestJdbcDialect(supportsConcurrentInserts = false),
                DataTypeMappings(),
                false
            )
            runBlocking {
                destination.createCollection(
                    CollectionMetadata(
                        "users",
                        mapOf("id" to FieldDefinition("id", "int"))
                    )
                )

                destination.insertDocuments(
                    "users",
                    flow {
                        repeat(2) { index -> emit(mapOf("id" to index)) }
                    }
                )
                val count = JdbcSource(
                    pool,
                    TestJdbcDialect(),
                    false
                ).countDocuments("users")
                assertEquals(2, count)
            }
        }
    }

    @Test
    fun `close() closes pool only when configured`() {
        TestJdbcPool.create().use { pool ->
            JdbcDestination(pool, TestJdbcDialect(), DataTypeMappings(), false).close()
            assertFalse(pool.closed)
            JdbcDestination(pool, TestJdbcDialect(), DataTypeMappings(), true).close()
            assertTrue(pool.closed)
        }
    }
}
