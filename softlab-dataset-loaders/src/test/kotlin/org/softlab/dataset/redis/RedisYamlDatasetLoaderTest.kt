package org.softlab.dataset.redis

import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.nio.file.Files
import kotlin.io.path.writeText


class RedisYamlDatasetLoaderTest {
    @Test
    fun `load() flushes and writes mapped hashes and sets`() {
        val redis = mockk<RedisFacade>(relaxed = true)
        val mappingFile = Files.createTempFile("redis-mapping", ".yml")
        val datasetFile = Files.createTempFile("redis-dataset", ".yml")
        mappingFile.writeText(
            """
            tables:
              - table: users
                hashes:
                  - key: users:${'$'}{id}
                sets:
                  - key: users
                    member: ${'$'}{id}
            """.trimIndent()
        )
        datasetFile.writeText(
            """
            users:
              - id: 1
                name: Alice
            """.trimIndent()
        )

        RedisYamlDatasetLoader(redis, mappingFile.toString())
            .load(datasetFile.toString(), cleanBefore = true)

        verify(exactly = 1) { redis.flushDb() }
        verify(exactly = 1) {
            redis.hashSet("users:1", mapOf("id" to "1", "name" to "Alice"))
        }
        verify(exactly = 1) { redis.setAdd("users", setOf("1")) }
    }
}
