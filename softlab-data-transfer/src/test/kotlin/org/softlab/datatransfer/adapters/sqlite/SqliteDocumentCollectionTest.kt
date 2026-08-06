package org.softlab.datatransfer.adapters.sqlite

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.softlab.dataset.jdbc.withStatement
import org.softlab.datataset.test.initiators.withTestPool
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
}
