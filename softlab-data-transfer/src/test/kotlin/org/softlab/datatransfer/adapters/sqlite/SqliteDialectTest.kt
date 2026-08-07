package org.softlab.datatransfer.adapters.sqlite

import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.softlab.dataset.jdbc.withConnection
import org.softlab.dataset.jdbc.withStatement
import org.softlab.datataset.test.initiators.withTestPool
import org.softlab.datatransfer.core.IndexDefinition
import org.softlab.datatransfer.core.IndexFieldDefinition
import org.softlab.datatransfer.core.IndexSortOrder
import org.softlab.datatransfer.core.SqlTableName
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse


class SqliteDialectTest {
    @ParameterizedTest
    @ValueSource(strings = [ "users", "schema.users" ])
    fun `parseTableName() treats full value as table name`(actualTable: String) {
        assertEquals(
            SqlTableName(null, "users"),
            SqliteDialect().parseTableName(actualTable)
        )
    }

    @Test
    fun `properties describe SQLite behavior`() {
        val dialect = SqliteDialect()

        assertEquals("sqlite", dialect.backendName)
        assertFalse(dialect.supportsConcurrentInserts)
    }

    @Test
    fun `listColumns() normalizes SQLite integer types to int8`() {
        withTestPool { pool ->
            pool.withStatement { statement ->
                statement.execute("CREATE TABLE users (id INTEGER, amount BIGINT, name TEXT);")
            }

            val columns = pool.withConnection { connection ->
                SqliteDialect().listColumns(SqlTableName(null, "users"), connection)
            }

            assertContentEquals(listOf("INT8", "INT8", "TEXT"), columns.map { it.value.type })
        }
    }

    @Test
    fun `readIndices() reads named user-created indices through SQLite pragmas`() {
        withTestPool { pool ->
            pool.withStatement { statement ->
                statement.execute("CREATE TABLE users (id INTEGER, name TEXT, email TEXT);")
                statement.execute("CREATE INDEX idx_users_email ON users (email);")
                statement.execute("CREATE UNIQUE INDEX idx_users_name_id ON users (name ASC, id DESC);")
            }

            val indices = pool.withConnection { connection ->
                SqliteDialect().readIndices(SqlTableName(null, "users"), connection)
            }

            assertContentEquals(
                listOf(
                    IndexDefinition(
                        name = "idx_users_email",
                        fields = listOf(IndexFieldDefinition("email")),
                        unique = false
                    ),
                    IndexDefinition(
                        name = "idx_users_name_id",
                        fields = listOf(
                            IndexFieldDefinition("name"),
                            IndexFieldDefinition("id", IndexSortOrder.DESC)
                        ),
                        unique = true
                    )
                ),
                indices
            )
        }
    }

    @Test
    fun `readIndices() excludes implicit SQLite primary-key backing index`() {
        withTestPool { pool ->
            pool.withStatement { statement ->
                statement.execute(
                    "CREATE TABLE users (tenant_id INTEGER, user_id INTEGER, PRIMARY KEY (tenant_id, user_id));"
                )
            }

            val indices = pool.withConnection { connection ->
                SqliteDialect().readIndices(SqlTableName(null, "users"), connection)
            }

            assertContentEquals(emptyList(), indices)
        }
    }

    @Test
    fun `readIndices() skips partial and expression indexes`() {
        withTestPool { pool ->
            pool.withStatement { statement ->
                statement.execute("CREATE TABLE users (id INTEGER, name TEXT, active INTEGER);")
                statement.execute("CREATE INDEX idx_users_active_partial ON users (active) WHERE active = 1;")
                statement.execute("CREATE INDEX idx_users_name_expression ON users (lower(name));")
            }

            val indices = pool.withConnection { connection ->
                SqliteDialect().readIndices(SqlTableName(null, "users"), connection)
            }

            assertContentEquals(emptyList(), indices)
        }
    }
}
