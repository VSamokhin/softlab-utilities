package org.softlab.datatransfer.core

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Test
import org.softlab.dataset.core.FieldDefinition
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue


class CollectionMetadataTest {
    @Test
    fun `test collection metadata equals contract`() {
        EqualsVerifier.forClass(CollectionMetadata::class.java)
            .withNonnullFields("name", "fields", "indices")
            .withPrefabValues(
                FieldDefinition::class.java,
                FieldDefinition("id", "integer"),
                FieldDefinition("name", "string")
            )
            .verify()
    }

    @Test
    fun `test primary key metadata equals contract`() {
        EqualsVerifier.forClass(PrimaryKeyDefinition::class.java)
            .withNonnullFields("fields")
            .verify()
    }

    @Test
    fun `test index metadata equals contract`() {
        EqualsVerifier.forClass(IndexDefinition::class.java)
            .withNonnullFields("fields")
            .verify()
    }

    @Test
    fun `test index field metadata equals contract`() {
        EqualsVerifier.forClass(IndexFieldDefinition::class.java)
            .withNonnullFields("name", "order")
            .verify()
    }

    @Test
    fun `test collection metadata default values`() {
        val metadata = CollectionMetadata(
            name = "users",
            fields = createFieldDefinitions(),
            primaryKey = PrimaryKeyDefinition("users_pkey", listOf("user_id"))
        )
        val metadataWithDefaults = CollectionMetadata("users", createFieldDefinitions())
        val indexField = IndexFieldDefinition("name")
        val index = IndexDefinition("users_name_idx", listOf(indexField))

        assertEquals("users_pkey", metadata.primaryKey?.name)
        assertContentEquals(listOf("user_id"), metadata.primaryKey?.fields)
        assertTrue(metadataWithDefaults.indices.isEmpty())
        assertNull(metadataWithDefaults.primaryKey)
        assertTrue(metadata.indices.isEmpty())
        assertEquals(IndexSortOrder.ASC, indexField.order)
        assertFalse(index.unique)
    }

    private fun createFieldDefinitions() = mapOf(
        "user_id" to FieldDefinition("user_id", "string"),
        "name" to FieldDefinition("name", "string")
    )
}
