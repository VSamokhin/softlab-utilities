package org.softlab.datatransfer.adapters.redis

import org.junit.jupiter.api.Test
import org.softlab.dataset.core.FieldDefinition
import org.softlab.datatransfer.core.CollectionMetadata
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue


class RedisMappingGeneratorTest {
    @Test
    fun `generate() creates a default mapping from collection metadata`() {
        val mappings = RedisMappingGenerator.generate(
            CollectionMetadata(
                name = "users",
                fields = listOf(
                    FieldDefinition("user_id", "integer"),
                    FieldDefinition("name", "text", nullable = true)
                )
            )
        )

        assertEquals(1, mappings?.tables?.size)
        val table = mappings!!.tables.single()
        assertEquals("users", table.table)
        assertEquals("users:\${user_id}", table.hashes.single().key)
        assertEquals("users", table.sets.single().key)
        assertEquals("\${user_id}", table.sets.single().member)
        assertEquals(listOf("user_id", "name"), table.fields.map { it.name })
    }

    @Test
    fun `generate() prefers exact id field over later suffix matches`() {
        val mappings = RedisMappingGenerator.generate(
            CollectionMetadata(
                name = "users",
                fields = listOf(
                    FieldDefinition("legacy_user_id", "integer"),
                    FieldDefinition("id", "integer")
                )
            )
        )

        assertEquals("users:\${id}", mappings!!.tables.single().hashes.single().key)
    }

    @Test
    fun `generate() returns null when source schema is unknown`() {
        assertNull(RedisMappingGenerator.generate(CollectionMetadata("users", emptyList())))
    }

    @Test
    fun `asYaml() renders generated mapping as yaml`() {
        val mappings = RedisMappingGenerator.generate(
            CollectionMetadata(
                name = "users",
                fields = listOf(FieldDefinition("id", "integer"))
            )
        )!!

        val yaml = RedisMappingGenerator.asYaml(mappings)

        assertTrue("users" in yaml)
        assertTrue("\${id}" in yaml)
        assertTrue("users:\${id}" in yaml)
    }
}
