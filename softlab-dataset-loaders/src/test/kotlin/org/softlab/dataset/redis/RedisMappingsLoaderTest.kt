package org.softlab.dataset.redis

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.Files
import kotlin.io.path.writeText
import kotlin.test.assertEquals


class RedisMappingsLoaderTest {
    @Test
    fun `load() reads mappings from filesystem path`() {
        val mappingFile = Files.createTempFile("redis-mappings", ".yml")
        mappingFile.writeText(
            """
            tables:
              - table: users
                fields:
                  - name: id
                    type: integer
                hashes:
                  - key: users:${'$'}{id}
                sets:
                  - key: users
                    member: ${'$'}{id}
            """.trimIndent()
        )

        val mappings = RedisMappingsLoader.load(mappingFile.toString())

        assertEquals(1, mappings.tables.size)
        assertEquals("users", mappings.tables.single().table)
        assertEquals("id", mappings.tables.single().fields.single().name)
        assertEquals("users:\${id}", mappings.tables.single().hashes.single().key)
        assertEquals("\${id}", mappings.tables.single().sets.single().member)
    }

    @Test
    fun `load() throws for missing file`() {
        val exc = assertThrows<IllegalStateException> {
            RedisMappingsLoader.load("missing-redis-mapping.yml")
        }

        assertEquals("Could not read Redis mapping file: missing-redis-mapping.yml", exc.message)
    }
}
