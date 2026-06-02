package org.softlab.datatransfer.adapters.jdbc

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.contains
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.jdbc.withConnection
import org.softlab.datataset.test.initiators.withTestPool
import java.sql.SQLException
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue


class JdbcDocumentCollectionTest {
    @Test
    fun `fetchMetadata() returns metadata when table exists`() {
        withTestPool { pool ->
            pool.withConnection { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE users (id INTEGER NOT NULL, name TEXT);")
                }
            }

            val metadata = runBlocking {
                JdbcDocumentCollection(
                    JdbcTableName(null, "users"),
                    pool,
                    TestJdbcDialect()
                ).fetchMetadata()
            }

            assertEquals("users", metadata.name)
            assertThat(metadata.fields.map { it.name }, contains("id", "name"))
        }
    }

    @Test
    fun `fetchMetadata() throws when table does not exist`() {
        withTestPool { pool ->
            val exc = assertThrows<IllegalStateException> {
                runBlocking {
                    JdbcDocumentCollection(
                        JdbcTableName(null, "users"),
                        pool,
                        TestJdbcDialect()
                    ).fetchMetadata()
                }
            }
            assertThat(exc.message, containsString("users"))
        }
    }

    @Test
    fun `readDocuments() returns rows mapped by column names`() {
        withTestPool { pool ->
            pool.withConnection { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE users (id INTEGER, name TEXT);")
                    statement.execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');")
                }
            }

            val documents = runBlocking {
                JdbcDocumentCollection(
                    JdbcTableName(null, "users"),
                    pool,
                    TestJdbcDialect()
                ).readDocuments().toList()
            }

            assertContentEquals(listOf(1, 2), documents.map { it["id"] })
            assertContentEquals(listOf("Alice", "Bob"), documents.map { it["name"] })
        }
    }

    @Test
    fun `readDocuments() uses transaction only when dialect requires cursor transaction`() {
        withTestPool { pool ->
            pool.withConnection { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE users (id INTEGER);")
                    statement.execute("INSERT INTO users (id) VALUES (1);")
                }
                assertTrue(connection.autoCommit)
            }

            val documents = runBlocking {
                JdbcDocumentCollection(
                    JdbcTableName(null, "users"),
                    pool,
                    TestJdbcDialect()
                ).readDocuments().toList()
            }

            assertContentEquals(listOf(1), documents.map { it["id"] })
            pool.withConnection { connection -> assertTrue(connection.autoCommit) }
        }
    }

    @Test
    fun `readDocuments() throws for absent table`() {
        withTestPool { pool ->
            val exc = assertThrows<SQLException> {
                runBlocking {
                    JdbcDocumentCollection(
                        JdbcTableName(null, "missing_table"),
                        pool,
                        TestJdbcDialect()
                    ).readDocuments().toList()
                }
            }
            assertThat(exc.message, containsString("missing_table"))
        }
    }
}
