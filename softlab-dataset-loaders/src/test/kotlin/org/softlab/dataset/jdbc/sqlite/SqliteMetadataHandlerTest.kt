package org.softlab.dataset.jdbc.sqlite

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.Statement
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class SqliteMetadataHandlerTest {
    @Test
    fun `getColumns() maps schema alias to unqualified SQLite table`() {
        val databaseMetaData = mockk<DatabaseMetaData>()
        val resultSet = mockk<ResultSet>()
        val handler = SqliteMetadataHandler("data")

        every { databaseMetaData.getColumns(null, null, "various_datatypes", "%") } returns resultSet

        assertEquals(resultSet, handler.getColumns(databaseMetaData, "data", "data.various_datatypes"))

        verify { databaseMetaData.getColumns(null, null, "various_datatypes", "%") }
    }

    @Test
    fun `getTables() returns tables from requested SQLite schema alias`() {
        val databaseMetaData = mockk<DatabaseMetaData>()
        val connection = mockk<Connection>()
        val dbListStatement = mockk<Statement>(relaxed = true)
        val tableListStatement = mockk<Statement>(relaxed = true)
        val dbListResultSet = mockk<ResultSet>(relaxed = true)
        val tableListResultSet = mockk<ResultSet>(relaxed = true)
        val handler = SqliteMetadataHandler("data")

        val tableTypes = arrayOf("TABLE")
        every { databaseMetaData.connection } returns connection
        every { connection.metaData } returns databaseMetaData
        every { databaseMetaData.driverName } returns "SQLite JDBC"
        every { connection.createStatement() } returns tableListStatement andThen dbListStatement
        every { dbListStatement.executeQuery("PRAGMA database_list;") } returns dbListResultSet
        every { dbListResultSet.next() } returnsMany listOf(true, true, false)
        every { dbListResultSet.getString("name") } returnsMany listOf("main", "data")
        every { dbListResultSet.getString("file") } returnsMany listOf("source.db", "source.db")
        every {
            tableListStatement.executeQuery(
                """
                SELECT tbl_name
                FROM "data".sqlite_schema
                WHERE type = 'table'
                ORDER BY tbl_name
                """.trimIndent()
            )
        } returns tableListResultSet
        every { tableListResultSet.next() } returnsMany listOf(true, false)
        every { tableListResultSet.getString("tbl_name") } returns "various_datatypes"

        handler.getTables(databaseMetaData, "data", tableTypes).use { resultSet ->
            assertTrue(resultSet.next())
            assertEquals(null, resultSet.getString("TABLE_CAT"))
            assertEquals("data", resultSet.getString("TABLE_SCHEM"))
            assertEquals("various_datatypes", resultSet.getString("TABLE_NAME"))
            assertEquals("TABLE", resultSet.getString("TABLE_TYPE"))
            assertFalse(resultSet.next())
        }

        verify { dbListStatement.executeQuery("PRAGMA database_list;") }
        verify {
            tableListStatement.executeQuery(
                """
                SELECT tbl_name
                FROM "data".sqlite_schema
                WHERE type = 'table'
                ORDER BY tbl_name
                """.trimIndent()
            )
        }
    }

    @Test
    fun `getSchema() exposes configured alias`() {
        val resultSet = mockk<ResultSet>()
        val handler = SqliteMetadataHandler("data")

        assertEquals("data", handler.getSchema(resultSet))
    }
}
