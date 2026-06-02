package org.softlab.datatransfer.adapters.jdbc

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.jdbc.withConnection
import org.softlab.datataset.test.initiators.withTestPool
import java.sql.SQLException
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue


class JdbcSourceTest {
    @Test
    fun `getBackendName() returns dialect backend name`() {
        withTestPool { pool ->
            val source = JdbcSource(
                pool,
                TestJdbcDialect(backendName = "test-backend"),
                closePool = false
            )

            assertEquals("test-backend", source.getBackendName())
        }
    }

    @Test
    fun `listCollections() returns collections from dialect tables`() {
        withTestPool { pool ->
            pool.withConnection { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE users (id INTEGER);")
                    statement.execute("CREATE TABLE products (id INTEGER);")
                }
            }

            val collections = JdbcSource(pool, TestJdbcDialect(), closePool = false)
                .listCollections()
                .let { flow -> runBlocking { flow.toList() } }

            assertContentEquals(
                listOf("products", "users"),
                collections.map { runBlocking { it.fetchMetadata() }.name }.sorted()
            )
        }
    }

    @Test
    fun `listCollections() uses provided collection factory`() {
        withTestPool { pool ->
            pool.withConnection { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE users (id INTEGER);")
                }
            }
            val tableNames = mutableListOf<JdbcTableName>()
            val source = JdbcSource(
                pool,
                TestJdbcDialect(),
                closePool = false
            ) { tableName, pool, dialect ->
                tableNames += tableName
                JdbcDocumentCollection(tableName, pool, dialect)
            }

            runBlocking { source.listCollections().toList() }

            assertContentEquals(listOf(JdbcTableName(null, "users")), tableNames)
        }
    }

    @Test
    fun `countDocuments() returns table row count`() {
        withTestPool { pool ->
            pool.withConnection { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE users (id INTEGER);")
                    statement.execute("INSERT INTO users (id) VALUES (1), (2);")
                }
            }

            val count = runBlocking {
                JdbcSource(
                    pool,
                    TestJdbcDialect(),
                    closePool = false
                ).countDocuments("users")
            }

            assertEquals(2, count)
        }
    }

    @Test
    fun `close() closes pool when configured`() {
        withTestPool { pool ->
            JdbcSource(pool, TestJdbcDialect(), closePool = true).close()

            assertTrue(pool.closed)
        }
    }

    @Test
    fun `close() leaves pool open when configured`() {
        withTestPool { pool ->
            JdbcSource(pool, TestJdbcDialect(), closePool = false).close()

            assertEquals(false, pool.closed)
        }
    }

    @Test
    fun `countDocuments() throws for absent table`() {
        withTestPool { pool ->
            val exc = assertThrows<SQLException> {
                runBlocking {
                    JdbcSource(
                        pool,
                        TestJdbcDialect(),
                        false
                    ).countDocuments("missing_table")
                }
            }
            assertThat(exc.message, containsString("missing_table"))
        }
    }
}
