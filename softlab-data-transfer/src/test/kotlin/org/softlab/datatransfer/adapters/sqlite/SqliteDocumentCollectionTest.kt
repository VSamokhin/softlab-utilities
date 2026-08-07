package org.softlab.datatransfer.adapters.sqlite

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.softlab.dataset.jdbc.withStatement
import org.softlab.datataset.test.initiators.withTestPool
import org.softlab.datatransfer.core.IndexDefinition
import org.softlab.datatransfer.core.IndexFieldDefinition
import org.softlab.datatransfer.core.PrimaryKeyDefinition
import org.softlab.datatransfer.core.SqlTableName
import kotlin.test.assertEquals
import kotlin.test.assertIs


class SqliteDocumentCollectionTest {
    @Test
    fun `readDocuments() casts SQLite integer-backed values by declared column type`() {
        withTestPool { pool ->
            pool.withStatement { statement ->
                statement.execute("CREATE TABLE users (id INTEGER, amount BIGINT, active BOOLEAN, name TEXT);")
                statement.execute(
                    "INSERT INTO users (id, amount, active, name) VALUES (1, 1234567890123, 1, 'Alice');"
                )
            }

            val document = runBlocking {
                SqliteDocumentCollection(
                    tableName = SqlTableName(null, "users"),
                    pool = pool,
                    dialect = SqliteDialect()
                ).readDocuments().toList().single()
            }

            assertEquals(1L, document["id"])
            assertIs<Long>(document["amount"])
            assertEquals(1234567890123L, document["amount"])
            assertIs<Boolean>(document["active"])
            assertEquals(true, document["active"])
            assertEquals("Alice", document["name"])
        }
    }

    @Test
    fun `fetchMetadata() includes SQLite primary key and supported secondary indices`() {
        withTestPool { pool ->
            pool.withStatement { statement ->
                statement.execute(
                    "CREATE TABLE users (tenant_id INTEGER, user_id INTEGER, email TEXT, " +
                        "PRIMARY KEY (tenant_id, user_id));"
                )
                statement.execute("CREATE UNIQUE INDEX idx_users_email ON users (email);")
                statement.execute("CREATE INDEX idx_users_tenant_user ON users (tenant_id, user_id);")
            }

            val metadata = runBlocking {
                SqliteDocumentCollection(
                    tableName = SqlTableName(null, "users"),
                    pool = pool,
                    dialect = SqliteDialect()
                ).fetchMetadata()
            }

            assertEquals("users", metadata.name)
            assertEquals(PrimaryKeyDefinition(null, listOf("tenant_id", "user_id")), metadata.primaryKey)
            assertEquals(
                listOf(
                    IndexDefinition("idx_users_email", listOf(IndexFieldDefinition("email")), unique = true),
                    IndexDefinition(
                        "idx_users_tenant_user",
                        listOf(IndexFieldDefinition("tenant_id"), IndexFieldDefinition("user_id"))
                    )
                ),
                metadata.indices
            )
        }
    }
}
