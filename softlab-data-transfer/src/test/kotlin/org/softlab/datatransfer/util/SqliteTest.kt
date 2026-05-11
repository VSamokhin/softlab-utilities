package org.softlab.datatransfer.util

import org.junit.jupiter.api.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class SqliteTest {
    @Test
    fun `isSqliteUri() returns true for SQLite JDBC URI`() {
        assertTrue(Sqlite.isSqliteUri("jdbc:sqlite:/tmp/test.db"))
        assertTrue(Sqlite.isSqliteUri("jdbc:sqlite::memory:"))
    }

    @Test
    fun `isSqliteUri() returns false for non-SQLite URIs`() {
        assertFalse(Sqlite.isSqliteUri("jdbc:postgresql://localhost/db"))
        assertFalse(Sqlite.isSqliteUri("mongodb://localhost/db"))
    }
}
