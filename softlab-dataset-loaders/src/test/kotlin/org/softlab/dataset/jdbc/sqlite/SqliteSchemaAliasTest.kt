package org.softlab.dataset.jdbc.sqlite

import io.mockk.every
import io.mockk.mockk
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.contains
import org.hamcrest.Matchers.containsString
import java.nio.file.Files
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith


class SqliteSchemaAliasTest {
    @Test
    fun `sqliteAttach() rejects non-SQLite connections`() {
        val connection = mockk<Connection> {
            every { metaData } returns mockk<DatabaseMetaData> {
                every { driverName } returns "PostgreSQL JDBC Driver"
            }
        }

        val exc = assertFailsWith<IllegalArgumentException> {
            connection.sqliteAttach("data.db", "data")
        }

        assertThat(exc.message, containsString("PostgreSQL JDBC Driver"))
    }

    @Test
    fun `sqliteAttach() attaches database and sqliteListDbs() lists available aliases`() {
        val dbFile = Files
            .createTempFile("sqlite-schema-alias-test", ".db")
            .apply { toFile().deleteOnExit() }

        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.sqliteAttach(dbFile.toString(), "data")
            val dbs = connection.sqliteListDbs()

            assertThat(dbs.keys,contains("main", "data"))
            assertEquals(dbFile.toRealPath().toString(), dbs["data"])
            assertEquals("", dbs["main"])
        }
    }

    @Test
    fun `sqliteListDbs() returns empty map for non-SQLite connection`() {
        val connection = mockk<Connection> {
            every { metaData } returns mockk<DatabaseMetaData> {
                every { driverName } returns "PostgreSQL JDBC Driver"
            }
        }

        val exc = assertFailsWith<IllegalArgumentException> {
            connection.sqliteListDbs()
        }

        assertThat(exc.message, containsString("PostgreSQL JDBC Driver"))
    }
}
