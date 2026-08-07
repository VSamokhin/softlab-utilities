package org.softlab.datatransfer.adapters.sqlite

import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.contains
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.jdbc.withConnection
import org.softlab.dataset.jdbc.withStatement
import org.softlab.datatransfer.config.ConfigProvider
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.IndexDefinition
import org.softlab.datatransfer.core.IndexFieldDefinition
import org.softlab.datatransfer.core.IndexSortOrder
import org.softlab.datatransfer.core.PrimaryKeyDefinition
import org.softlab.datatransfer.core.SqlTableName
import java.nio.file.Files
import java.sql.SQLException
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue


class SqliteDestinationIT {
    companion object {
        private val dataTypeMappings = ConfigProvider.config.getDataTypeMappings()

        private inline fun <T> withPool(block: (SqliteConnectionPool) -> T): T {
            val dbFile = Files
                .createTempFile("data-transfer-sqlite", ".db")
                .apply { toFile().deleteOnExit() }
            return SqliteConnectionPool("jdbc:sqlite:$dbFile").use(block)
        }
    }

    @Test
    fun `class closes connection when configured`() {
        withPool { pool ->
            SqliteDestination(pool, dataTypeMappings).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata("users", mapOf("id" to FieldDefinition("id", "int")))
                    )
                }
            }
            assertTrue(pool.closed)
        }
    }

    @Test
    fun `createCollection() creates table with mapped column types`() {
        withPool { pool ->
            SqliteDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "users",
                            fields = mapOf(
                                "id" to FieldDefinition("id", "int", false),
                                "name" to FieldDefinition("name", "string"),
                                "active" to FieldDefinition("active", "boolean", false),
                                "payload" to FieldDefinition("payload", "blob")
                            )
                        )
                    )
                }
            }

            val columns = pool.withConnection {
                val dialect = SqliteDialect()
                dialect.listColumns(dialect.parseTableName("users"), it)
            }

            assertThat(columns.values,
                contains(
                    FieldDefinition("id", "INT8", false),
                    FieldDefinition("name", "TEXT"),
                    FieldDefinition("active", "INT8", false),
                    FieldDefinition("payload", "BLOB")
                )
            )
        }
    }

    @Test
    fun `createCollection() throws when table already exists`() {
        withPool { pool ->
            SqliteDestination(pool, dataTypeMappings, false).use { destination ->
                val metadata = CollectionMetadata("users", mapOf("id" to FieldDefinition("id", "int")))

                runBlocking {
                    destination.createCollection(metadata)

                    val exc = assertThrows<SQLException> {
                        destination.createCollection(metadata)
                    }
                    assertThat(exc.message, containsString("users"))
                }
            }
        }
    }

    @Test
    fun `createCollection() creates primary key inline`() {
        val metadata = CollectionMetadata(
            name = "users",
            fields = mapOf(
                "tenant_id" to FieldDefinition("tenant_id", "int"),
                "user_id" to FieldDefinition("user_id", "int"),
                "name" to FieldDefinition("name", "string")
            ),
            primaryKey = PrimaryKeyDefinition(null, listOf("tenant_id", "user_id"))
        )

        withPool { pool ->
            SqliteDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(metadata)
                }
            }

            val primaryKey = pool.withConnection { conn ->
                SqliteDialect().readPrimaryKey(SqlTableName(null, "users"), conn)
            }

            assertEquals(metadata.primaryKey, primaryKey)
        }
    }

    @Test
    fun `insertDocuments() fails on duplicate primary key before post-load metadata hook`() {
        val metadata = CollectionMetadata(
            name = "users",
            fields = mapOf(
                "id" to FieldDefinition("id", "int"),
                "name" to FieldDefinition("name", "string")
            ),
            primaryKey = PrimaryKeyDefinition(null, listOf("id"))
        )

        withPool { pool ->
            SqliteDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(metadata)

                    assertThrows<SQLException> {
                        destination.insertDocuments(
                            metadata.name,
                            flowOf(
                                mapOf("id" to 1, "name" to "Alice"),
                                mapOf("id" to 1, "name" to "Bob")
                            )
                        )
                    }
                }
            }
        }
    }

    @Test
    fun `createIndexesAndConstraints() creates supported indices after rows are inserted`() {
        val metadata = usersMetadataWithPrimaryKeyAndIndices()

        withPool { pool ->
            SqliteDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(metadata)
                    destination.insertDocuments(
                        metadata.name,
                        flowOf(
                            mapOf("id" to 1, "tenant_id" to 1, "name" to "Alice", "email" to "alice@example.com"),
                            mapOf("id" to 2, "tenant_id" to 1, "name" to "Bob", "email" to "bob@example.com")
                        )
                    )
                    destination.createIndexesAndConstraints(metadata)
                }
            }

            pool.withConnection { conn ->
                val dialect = SqliteDialect()
                val tableName = SqlTableName(null, "users")

                assertEquals(metadata.primaryKey, dialect.readPrimaryKey(tableName, conn))
                assertContentEquals(metadata.indices, dialect.readIndices(tableName, conn))
            }
        }
    }

    @Test
    fun `createIndexesAndConstraints() does nothing when metadata has no definitions`() {
        val metadata = CollectionMetadata(
            name = "users",
            fields = mapOf("id" to FieldDefinition("id", "int"))
        )

        withPool { pool ->
            SqliteDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(metadata)
                    destination.createIndexesAndConstraints(metadata)
                }
            }

            pool.withConnection { conn ->
                val tableName = SqlTableName(null, "users")

                assertNull(SqliteDialect().readPrimaryKey(tableName, conn))
                assertContentEquals(emptyList(), SqliteDialect().readIndices(tableName, conn))
            }
        }
    }

    @Test
    fun `createIndexesAndConstraints() uses fallback names when index names are absent`() {
        val metadata = CollectionMetadata(
            name = "users",
            fields = mapOf(
                "id" to FieldDefinition("id", "int"),
                "email" to FieldDefinition("email", "string")
            ),
            indices = listOf(
                IndexDefinition(
                    name = null,
                    fields = listOf(IndexFieldDefinition("email"))
                )
            )
        )

        withPool { pool ->
            SqliteDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(metadata)
                    destination.createIndexesAndConstraints(metadata)
                }
            }

            val indices = pool.withConnection { conn ->
                SqliteDialect().readIndices(SqlTableName(null, "users"), conn)
            }

            assertContentEquals(
                listOf(IndexDefinition("users_email_idx", listOf(IndexFieldDefinition("email")))),
                indices
            )
        }
    }

    @Test
    fun `createIndexesAndConstraints() fails when unique index data has duplicates`() {
        val metadata = CollectionMetadata(
            name = "users",
            fields = mapOf(
                "id" to FieldDefinition("id", "int"),
                "email" to FieldDefinition("email", "string")
            ),
            indices = listOf(
                IndexDefinition(
                    name = "users_email_unique_idx",
                    fields = listOf(IndexFieldDefinition("email")),
                    unique = true
                )
            )
        )

        withPool { pool ->
            SqliteDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(metadata)
                    destination.insertDocuments(
                        metadata.name,
                        flowOf(
                            mapOf("id" to 1, "email" to "duplicate@example.com"),
                            mapOf("id" to 2, "email" to "duplicate@example.com")
                        )
                    )

                    assertThrows<SQLException> {
                        destination.createIndexesAndConstraints(metadata)
                    }
                }
            }
        }
    }

    @Test
    fun `dropCollection() drops table with all data`() {
        withPool { pool ->
            SqliteDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "users",
                            fields = mapOf(
                                "id" to FieldDefinition("id", "int"),
                                "name" to FieldDefinition("name", "string")
                            )
                        )
                    )
                    destination.insertDocuments(
                        "users",
                        flowOf(
                            mapOf("id" to 1, "name" to "Alice"),
                            mapOf("id" to 2, "name" to "Bob")
                        )
                    )

                    destination.dropCollection("users")
                }

                pool.withConnection { conn ->
                    val dialect = SqliteDialect()
                    assertFalse(dialect.tableExists(dialect.parseTableName("users"), conn))
                }
            }
        }
    }

    @Test
    fun `dropCollection() doesn't fail if table not exists`() {
        withPool { pool ->
            SqliteDestination(pool, dataTypeMappings, false).use { destination ->
                assertDoesNotThrow { runBlocking { destination.dropCollection("something") } }
            }
        }
    }

    @Test
    fun `insertDocuments() writes expected data`() {
        withPool { pool ->
            SqliteDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata(
                            name = "users",
                            fields = mapOf(
                                "id" to FieldDefinition("id", "int"),
                                "name" to FieldDefinition("name", "string"),
                                "active" to FieldDefinition("active", "boolean")
                            )
                        )
                    )

                    destination.insertDocuments(
                        "users",
                        flowOf(
                            mapOf("id" to 1, "name" to "Alice", "active" to true),
                            mapOf("id" to 2, "name" to "Bob", "active" to false)
                        )
                    )
                }
            }

            pool.withStatement { stmt ->
                stmt.executeQuery("SELECT id, name, active FROM users ORDER BY id;").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(1, rs.getInt("id"))
                    assertEquals("Alice", rs.getString("name"))
                    assertEquals(true, rs.getBoolean("active"))

                    assertTrue(rs.next())
                    assertEquals(2, rs.getInt("id"))
                    assertEquals("Bob", rs.getString("name"))
                    assertEquals(false, rs.getBoolean("active"))

                    assertFalse(rs.next())
                }
            }
        }
    }

    @Test
    fun `insertDocuments() throws when document has more fields than table`() {
        withPool { pool ->
            SqliteDestination(pool, dataTypeMappings, false).use { destination ->
                runBlocking {
                    destination.createCollection(
                        CollectionMetadata("users", mapOf("id" to FieldDefinition("id", "int")))
                    )

                    val exc = assertThrows<IllegalStateException> {
                        destination.insertDocuments(
                            "users",
                            flowOf(mapOf("id" to 1, "extra" to "unexpected"))
                        )
                    }
                    assertThat(
                        exc.message,
                        allOf(
                            containsString("users"),
                            containsString("table=id"),
                            containsString("row=id, extra")
                        )
                    )
                }
            }
        }
    }

    private fun usersMetadataWithPrimaryKeyAndIndices(): CollectionMetadata =
        CollectionMetadata(
            name = "users",
            fields = mapOf(
                "id" to FieldDefinition("id", "int"),
                "tenant_id" to FieldDefinition("tenant_id", "int"),
                "name" to FieldDefinition("name", "string"),
                "email" to FieldDefinition("email", "string")
            ),
            primaryKey = PrimaryKeyDefinition(null, listOf("id")),
            indices = listOf(
                IndexDefinition(
                    name = "users_email_unique_idx",
                    fields = listOf(IndexFieldDefinition("email")),
                    unique = true
                ),
                IndexDefinition(
                    name = "users_tenant_name_idx",
                    fields = listOf(
                        IndexFieldDefinition("tenant_id"),
                        IndexFieldDefinition("name", IndexSortOrder.DESC)
                    )
                )
            )
        )
}
