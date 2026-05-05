package org.softlab.datataset.test.initiators

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertTrue


@Testcontainers
class RedisInitiatorIT {
    companion object {
        private const val MAPPING = "mappings/dbunit-to-redis-mapping.yml"
        private const val DATASET = "datasets/test-dataset.yml"

        @Container
        @JvmStatic
        private val redisContainer = createRedisContainer()
    }

    @Test
    fun `seedData() delegates to Redis dataset loader without throwing`() {
        RedisInitiator(redisContainer.redisURI, MAPPING).use { cut ->
            cut.cleanup()

            assertDoesNotThrow {
                cut.seedData(DATASET)
            }
        }
    }

    @Test
    fun `cleanup() flushes database and initSchema() invokes callback`() {
        RedisInitiator(redisContainer.redisURI, MAPPING).use { cut ->
            cut.cleanup()

            val commands = cut.redisConnection.sync()
            commands.set("tmp:key", "value")

            var invoked = false
            cut.cleanup()
            cut.initSchema("ignored") {
                invoked = true
                assertEquals(0L, it.sync().dbsize())
            }

            assertTrue(invoked)
            assertEquals(0L, commands.dbsize())
        }
    }
}
