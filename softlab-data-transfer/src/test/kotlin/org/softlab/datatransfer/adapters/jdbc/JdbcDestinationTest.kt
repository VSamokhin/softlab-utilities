package org.softlab.datatransfer.adapters.jdbc

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.jdbc.JdbcConnectionPool
import org.softlab.datatransfer.config.DataTypeMappings
import org.softlab.datatransfer.core.BATCH_SIZE
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.SqlTableName
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Statement
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class JdbcDestinationTest {
    @Test
    fun `getBackendName() returns dialect backend name`() {
        val connection = mockk<Connection>(relaxed = true)
        val destination = JdbcDestination(
            MockJdbcPool(connection),
            TestJdbcDialect("test-backend"),
            DataTypeMappings(),
            false
        )

        assertEquals("test-backend", destination.getBackendName())
    }

    @Test
    fun `createCollection() creates table with mapped field types and nullability`() {
        val statement = mockk<Statement>(relaxed = true) {
            every { execute(any()) } returns false
        }
        val connection = mockk<Connection>(relaxed = true) {
            every { createStatement() } returns statement
        }
        val pool = MockJdbcPool(connection)
        val dialect = RecordingJdbcDialect()

        runBlocking {
            JdbcDestination(pool, dialect, DataTypeMappings(mapOf("string" to "TEXT")), false).createCollection(
                CollectionMetadata(
                    "users",
                    mapOf(
                        "id" to FieldDefinition("id", "int", false),
                        "full name" to FieldDefinition("full name", "string", true)
                    )
                )
            )
        }

        assertContentEquals(listOf(SqlTableName(null, "users")), dialect.beforeCreateTableCalls)
        verify(exactly = 1) {
            statement.execute("CREATE TABLE \"users\" (\"id\" int NOT NULL, \"full name\" TEXT);")
        }
    }

    @Test
    fun `createCollection() propagates SQL exceptions from table creation`() {
        val statement = mockk<Statement>(relaxed = true) {
            every { execute(any()) } throws SQLException("table users already exists")
        }
        val connection = mockk<Connection>(relaxed = true) {
            every { createStatement() } returns statement
        }
        val destination = JdbcDestination(
            MockJdbcPool(connection),
            RecordingJdbcDialect(),
            DataTypeMappings(),
            false
        )

        val exc = assertThrows<SQLException> {
            runBlocking {
                destination.createCollection(
                    CollectionMetadata(
                        "users",
                        mapOf("id" to FieldDefinition("id", "int"))
                    )
                )
            }
        }

        assertThat(exc.message, containsString("users"))
    }

    @Test
    fun `dropCollection() drops table if it exists`() {
        val statement = mockk<Statement>(relaxed = true) {
            every { execute(any()) } returns false
        }
        val connection = mockk<Connection>(relaxed = true) {
            every { createStatement() } returns statement
        }
        val destination = JdbcDestination(
            MockJdbcPool(connection),
            RecordingJdbcDialect(),
            DataTypeMappings(),
            false
        )

        runBlocking { destination.dropCollection("users") }

        verify(exactly = 1) { statement.execute("DROP TABLE IF EXISTS \"users\";") }
    }

    @Test
    fun `insertDocuments() writes data using table column order`() {
        val statement = mockk<PreparedStatement>(relaxed = true) {
            every { executeBatch() } returns intArrayOf(1)
        }
        val connection = mockk<Connection>(relaxed = true) {
            every { prepareStatement(any()) } returns statement
        }
        val destination = JdbcDestination(
            MockJdbcPool(connection),
            RecordingJdbcDialect(
                columns = mapOf(
                    "id" to FieldDefinition("id", "int"),
                    "full name" to FieldDefinition("full name", "string", true)
                )
            ),
            DataTypeMappings(),
            false
        )

        runBlocking {
            destination.insertDocuments(
                "users",
                flowOf(
                    mapOf("full name" to "Alice", "id" to 1),
                    mapOf("ID" to 2, "FULL NAME" to "Bob")
                )
            )
        }

        verifyOrder {
            statement.setObject(1, 1)
            statement.setObject(2, "Alice")
            statement.addBatch()
            statement.setObject(1, 2)
            statement.setObject(2, "Bob")
            statement.addBatch()
            statement.executeBatch()
        }
    }

    @Test
    fun `insertDocuments() writes null for missing fields`() {
        val statement = mockk<PreparedStatement>(relaxed = true) {
            every { executeBatch() } returns intArrayOf(1)
        }
        val connection = mockk<Connection>(relaxed = true) {
            every { prepareStatement(any()) } returns statement
        }
        val destination = JdbcDestination(
            MockJdbcPool(connection),
            RecordingJdbcDialect(
                columns = mapOf(
                    "id" to FieldDefinition("id", "int"),
                    "full name" to FieldDefinition("full name", "string", true)
                )
            ),
            DataTypeMappings(),
            false
        )

        runBlocking {
            destination.insertDocuments("users", flowOf(mapOf("id" to 1)))
        }

        verifyOrder {
            statement.setObject(1, 1)
            statement.setObject(2, null)
            statement.addBatch()
            statement.executeBatch()
        }
    }

    @Test
    fun `insertDocuments() casts integer values to booleans for boolean target columns`() {
        val statement = mockk<PreparedStatement>(relaxed = true) {
            every { executeBatch() } returns intArrayOf(1)
        }
        val connection = mockk<Connection>(relaxed = true) {
            every { prepareStatement(any()) } returns statement
        }
        val destination = object : JdbcDestination(
            MockJdbcPool(connection),
            RecordingJdbcDialect(
                columns = mapOf(
                    "id" to FieldDefinition("id", "integer"),
                    "active" to FieldDefinition("active", "boolean")
                )
            ),
            DataTypeMappings(),
            false
        ) {
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
        val statement = mockk<PreparedStatement>(relaxed = true) {
            every { executeBatch() } returns intArrayOf(1)
        }
        val connection = mockk<Connection>(relaxed = true) {
            every { prepareStatement(any()) } returns statement
        }
        val destination = JdbcDestination(
            MockJdbcPool(connection),
            RecordingJdbcDialect(columns = mapOf("id" to FieldDefinition("id", "int"))),
            DataTypeMappings(),
            false
        )

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

    @Test
    fun `insertDocuments() writes non-concurrent batches when dialect disallows concurrent inserts`() {
        val statement = mockk<PreparedStatement>(relaxed = true) {
            every { executeBatch() } returns intArrayOf(1)
        }
        val connection = mockk<Connection>(relaxed = true) {
            every { prepareStatement(any()) } returns statement
        }
        val destination = JdbcDestination(
            MockJdbcPool(connection),
            RecordingJdbcDialect(
                columns = mapOf("id" to FieldDefinition("id", "int")),
                supportsConcurrentInserts = false
            ),
            DataTypeMappings(),
            false
        )

        runBlocking {
            destination.insertDocuments(
                "users",
                flow {
                    repeat(BATCH_SIZE + 1) { index -> emit(mapOf("id" to index)) }
                }
            )
        }

        verify(exactly = 2) { statement.executeBatch() }
    }

    @Test
    fun `close() closes pool only when configured`() {
        val connection = mockk<Connection>(relaxed = true)
        val pool = MockJdbcPool(connection)

        JdbcDestination(pool, TestJdbcDialect(), DataTypeMappings(), false).close()
        assertFalse(pool.closed)
        JdbcDestination(pool, TestJdbcDialect(), DataTypeMappings(), true).close()
        assertTrue(pool.closed)
    }

    private class MockJdbcPool(
        private val connection: Connection
    ) : JdbcConnectionPool {
        override val closed: Boolean get() = closeCalls > 0

        private var closeCalls = 0

        override fun openConnection(): Connection = connection

        override fun close() {
            closeCalls++
        }
    }

    private class RecordingJdbcDialect(
        backendName: String = "jdbc-test",
        supportsConcurrentInserts: Boolean = false,
        private val columns: Map<String, FieldDefinition> = emptyMap()
    ) : JdbcDialect(
        backendName = backendName,
        supportsConcurrentInserts = supportsConcurrentInserts
    ) {
        var beforeCreateTableCalls: List<SqlTableName> = emptyList()

        override fun parseTableName(tableName: String): SqlTableName =
            SqlTableName.of(tableName).copy(schema = null)

        override fun beforeCreateTable(tableName: SqlTableName, connection: Connection) {
            beforeCreateTableCalls += tableName
        }

        override fun listColumns(tableName: SqlTableName, connection: Connection): Map<String, FieldDefinition> =
            columns
    }
}
