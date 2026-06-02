package org.softlab.dataset.jdbc.sqlite

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class SqliteMetadataHandlerTest {
    @Test
    fun `getColumns() maps schema alias to unqualified SQLite table`() {
        val resultSet = mockk<ResultSet>()
        val databaseMetaData = mockk<DatabaseMetaData> {
            every {
                getColumns(null, null, "various_datatypes", "%")
            } returns resultSet
        }

        val cut = SqliteMetadataHandler("data")

        assertEquals(resultSet, cut.getColumns(databaseMetaData, "data", "data.various_datatypes"))

        verify { databaseMetaData.getColumns(null, null, "various_datatypes", "%") }
    }

    @Test
    fun `getTables() returns tables from requested SQLite schema alias`() {
        val tableListResultSet = mockk<ResultSet>(relaxed = true) {
            every { next() } returnsMany listOf(true, false)
            every { getString("tbl_name") } returns "various_datatypes"
        }
        val sql = """
            SELECT tbl_name
            FROM "data".sqlite_schema
            WHERE type = 'table'
            ORDER BY tbl_name
            """.trimIndent()
        val tableListStatement = mockk<Statement>(relaxed = true) {
            every { executeQuery(sql) } returns tableListResultSet
        }
        val dbListStatement = mockk<Statement>(relaxed = true) {
            every { executeQuery("PRAGMA database_list;") } returns mockk<ResultSet>(relaxed = true) {
                every { next() } returnsMany listOf(true, true, false)
                every { getString("name") } returnsMany listOf("main", "data")
                every { getString("file") } returnsMany listOf("source.db", "source.db")
            }
        }
        val conn = mockk<Connection>()
        val databaseMetaData = mockk<DatabaseMetaData> {
            every { connection } returns conn
            every { driverName } returns "SQLite JDBC"
        }
        every { conn.metaData } returns databaseMetaData
        every { conn.createStatement() } returns tableListStatement andThen dbListStatement

        val cut = SqliteMetadataHandler("data")
        cut.getTables(databaseMetaData, "data", arrayOf("TABLE")).use { resultSet ->
            assertTrue(resultSet.next())
            assertEquals(null, resultSet.getString("TABLE_CAT"))
            assertEquals("data", resultSet.getString("TABLE_SCHEM"))
            assertEquals("various_datatypes", resultSet.getString("TABLE_NAME"))
            assertEquals("TABLE", resultSet.getString("TABLE_TYPE"))
            assertFalse(resultSet.next())
        }

        verify { dbListStatement.executeQuery("PRAGMA database_list;") }
        verify { tableListStatement.executeQuery(sql) }
    }

    @Test
    fun `getTables() returns main and attached SQLite tables when schema is not restricted`() {
        withSqliteConnection { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE main_users (id INTEGER);")
                statement.execute("ATTACH DATABASE ':memory:' AS data;")
                statement.execute("CREATE TABLE data.data_users (id INTEGER);")
            }

            val cut = SqliteMetadataHandler("data")
            val tables = cut.getTables(connection.metaData, null, arrayOf("TABLE")).use { resultSet ->
                buildList {
                    while (resultSet.next()) {
                        add(resultSet.getString("TABLE_SCHEM") to resultSet.getString("TABLE_NAME"))
                    }
                }
            }

            assertEquals(
                setOf(null to "main_users", "data" to "data_users"),
                tables.toSet()
            )
        }
    }

    @Test
    fun `getTables() filters out tables when requested table type does not match`() {
        withSqliteConnection { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE users (id INTEGER);")
            }

            val cut = SqliteMetadataHandler("data")
            cut.getTables(connection.metaData, null, arrayOf("VIEW")).use { resultSet ->
                assertFalse(resultSet.next())
            }
        }
    }

    @Test
    fun `tableExists() finds existing table and rejects missing table`() {
        withSqliteConnection { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE users (id INTEGER);")
            }

            val cut = SqliteMetadataHandler("data")

            assertTrue(cut.tableExists(connection.metaData, null, "users"))
            assertFalse(cut.tableExists(connection.metaData, null, "roles"))
        }
    }

    @Test
    fun `getPrimaryKeys() maps schema alias to unqualified SQLite table`() {
        val resultSet = mockk<ResultSet>()
        val databaseMetaData = mockk<DatabaseMetaData> {
            every { getPrimaryKeys(null, null, "various_datatypes") } returns resultSet
        }

        val cut = SqliteMetadataHandler("data")

        assertEquals(resultSet, cut.getPrimaryKeys(databaseMetaData, "data", "data.various_datatypes"))

        verify { databaseMetaData.getPrimaryKeys(null, null, "various_datatypes") }
    }

    @Test
    fun `getSchema() exposes configured alias`() {
        val resultSet = mockk<ResultSet>()
        val cut = SqliteMetadataHandler("data")

        assertEquals("data", cut.getSchema(resultSet))
    }

    private fun withSqliteConnection(block: (Connection) -> Unit) {
        DriverManager.getConnection("jdbc:sqlite::memory:").use(block)
    }
}
