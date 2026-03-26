package org.softlab.datataset.test.initiators

import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertEquals
import kotlin.test.assertTrue


@Testcontainers
class JdbcInitiatorIT {
    companion object {
        private const val CHANGELOG = "liquibase/changelog-postgres.yaml"
        private const val DATASET = "datasets/test-dataset.yml"

        @Container
        @JvmStatic
        private val postgres = createPostgresContainer()
    }

    @Test
    fun `initSchema() and seedData() prepare postgres database`() {
        JdbcInitiator(postgres.jdbcUrl, postgres.username, postgres.password).use { cut ->
            cut.cleanup()

            var callbackInvoked = false
            cut.initSchema(CHANGELOG) { connection ->
                callbackInvoked = true
                connection.createStatement().use { stmt ->
                    stmt.executeQuery(
                        """
                        SELECT COUNT(*)
                        FROM information_schema.tables
                        WHERE table_schema = 'test' AND table_name = 'test_table'
                        """.trimIndent()
                    ).use { rs ->
                        rs.next()
                        assertEquals(1, rs.getInt(1))
                    }
                }
            }
            cut.seedData(DATASET)

            cut.getConnection().use { connection ->
                connection.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT COUNT(*) FROM test.test_table").use { rs ->
                        rs.next()
                        assertEquals(2, rs.getInt(1))
                    }
                }
            }

            assertTrue(callbackInvoked)
        }
    }

    @Test
    fun `cleanup() drops schema objects and invokes callback`() {
        JdbcInitiator(postgres.jdbcUrl, postgres.username, postgres.password).use { cut ->
            cut.cleanup()
            cut.initSchema(CHANGELOG)
            cut.seedData(DATASET)

            var callbackInvoked = false
            cut.cleanup { connection ->
                callbackInvoked = true
                connection.createStatement().use { stmt ->
                    stmt.executeQuery(
                        """
                        SELECT COUNT(*)
                        FROM information_schema.tables
                        WHERE table_schema = 'test' AND table_name = 'test_table'
                        """.trimIndent()
                    ).use { rs ->
                        rs.next()
                        assertEquals(0, rs.getInt(1))
                    }
                }
            }

            assertTrue(callbackInvoked)
        }
    }
}
