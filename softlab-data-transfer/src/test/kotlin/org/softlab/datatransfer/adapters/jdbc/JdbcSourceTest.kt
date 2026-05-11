package org.softlab.datatransfer.adapters.jdbc

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.jdbc.withConnection
import java.sql.SQLException
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue


class JdbcSourceTest {
    @Test
    fun `getBackendName() returns dialect backend name`() {
        TestJdbcProvider.create().use { provider ->
            val source = JdbcSource(
                provider,
                TestJdbcDialect(backendName = "test-backend"),
                closePool = false
            )

            assertEquals("test-backend", source.getBackendName())
        }
    }

    @Test
    fun `listCollections() returns collections from dialect tables`() {
        TestJdbcProvider.create().use { provider ->
            provider.withConnection { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE users (id INTEGER);")
                    statement.execute("CREATE TABLE products (id INTEGER);")
                }
            }

            val collections = JdbcSource(provider, TestJdbcDialect(), closePool = false)
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
        TestJdbcProvider.create().use { provider ->
            provider.withConnection { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE users (id INTEGER);")
                }
            }
            val tableNames = mutableListOf<JdbcTableName>()
            val source = JdbcSource(
                provider,
                TestJdbcDialect(),
                closePool = false
            ) { tableName ->
                tableNames += tableName
                JdbcDocumentCollection(tableName, provider, TestJdbcDialect())
            }

            runBlocking { source.listCollections().toList() }

            assertContentEquals(listOf(JdbcTableName(null, "users")), tableNames)
        }
    }

    @Test
    fun `countDocuments() returns table row count`() {
        TestJdbcProvider.create().use { provider ->
            provider.withConnection { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE users (id INTEGER);")
                    statement.execute("INSERT INTO users (id) VALUES (1), (2);")
                }
            }

            val count = runBlocking {
                JdbcSource(
                    provider,
                    TestJdbcDialect(),
                    closePool = false
                ).countDocuments("users")
            }

            assertEquals(2, count)
        }
    }

    @Test
    fun `close() closes provider when configured`() {
        TestJdbcProvider.create().use { provider ->
            JdbcSource(provider, TestJdbcDialect(), closePool = true).close()

            assertTrue(provider.closed)
        }
    }

    @Test
    fun `close() leaves provider open when configured`() {
        TestJdbcProvider.create().use { provider ->
            JdbcSource(provider, TestJdbcDialect(), closePool = false).close()

            assertEquals(false, provider.closed)
        }
    }

    @Test
    fun `countDocuments() throws for absent table`() {
        TestJdbcProvider.create().use { provider ->
            val exc = assertThrows<SQLException> {
                runBlocking {
                    JdbcSource(
                        provider,
                        TestJdbcDialect(),
                         false
                    ).countDocuments("missing_table")
                }
            }
            assertThat(exc.message, containsString("missing_table"))
        }
    }
}
