package org.softlab.datatransfer.adapters.sqlite

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.contains
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.jupiter.api.Test
import org.softlab.dataset.jdbc.withStatement
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertNotNull


class SqliteSourceIT {
    private inline fun <T> withSource(block: (SqliteSource) -> T): T {
        val dbFile = Files.createTempFile("data-transfer-sqlite", ".db")
        return try {
            SqliteConnectionPool("jdbc:sqlite:$dbFile").use { pool ->
                pool.withStatement { stmt ->
                    stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT);")
                    stmt.execute("CREATE TABLE products (id INTEGER NOT NULL, title TEXT);")
                    stmt.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');")
                    stmt.execute("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com');")
                    stmt.execute("INSERT INTO products (id, title) VALUES (1, 'Gizmo');")
                }
                SqliteSource(pool, closePool = false).use(block)
            }
        } finally {
            Files.deleteIfExists(dbFile)
        }
    }

    @Test
    fun `countDocuments() returns correct count`() {
        withSource { cut ->
            assertEquals(2, runBlocking { cut.countDocuments("users") })
            assertEquals(1, runBlocking { cut.countDocuments("products") })
        }
    }

    @Test
    fun `getBackendName() returns sqlite`() {
        withSource { cut ->
            assertEquals("sqlite", cut.getBackendName())
        }
    }

    @Test
    fun `listCollections() returns expected collections`() {
        withSource { cut ->
            val collections = runBlocking { cut.listCollections().toList() }

            assertThat(
                collections.map { runBlocking { it.fetchMetadata() }.name },
                containsInAnyOrder("users", "products")
            )

            val usersCollection = collections.first { runBlocking { it.fetchMetadata() }.name == "users" }
            assertThat(
                runBlocking { usersCollection.fetchMetadata() }.fields.map { it.name }.toList(),
                contains("id", "name", "email")
            )
        }
    }

    @Test
    fun `readDocuments() returns expected documents`() {
        withSource { cut ->
            val collections = runBlocking { cut.listCollections().toList() }
            val usersCollection = collections.firstOrNull { runBlocking { it.fetchMetadata() }.name == "users" }
            assertNotNull(usersCollection)

            val users = runBlocking { usersCollection.readDocuments().toList() }

            assertThat(users.map { it["name"] }, containsInAnyOrder("Alice", "Bob"))
            assertThat(users.map { it["email"] }, containsInAnyOrder("alice@example.com", "bob@example.com"))
        }
    }
}
