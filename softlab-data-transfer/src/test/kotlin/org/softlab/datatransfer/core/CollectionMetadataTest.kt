package org.softlab.datatransfer.core

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Test
import org.softlab.dataset.core.FieldDefinition
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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

    @Test
    fun `test collection metadata rejects blank names`() {
        assertFailsWith<IllegalArgumentException> {
            CollectionMetadata("", createFieldDefinitions())
        }
    }

    @Test
    fun `test collection metadata rejects blank field map keys`() {
        assertFailsWith<IllegalArgumentException> {
            CollectionMetadata(
                name = "users",
                fields = mapOf("" to FieldDefinition("id", "string"))
            )
        }
    }

    @Test
    fun `test collection metadata rejects duplicate field definition names`() {
        assertFailsWith<IllegalArgumentException> {
            CollectionMetadata(
                name = "users",
                fields = mapOf(
                    "id" to FieldDefinition("id", "string"),
                    "duplicate_id" to FieldDefinition("ID", "string")
                )
            )
        }
    }

    @Test
    fun `test collection metadata rejects primary key fields missing from known schema`() {
        assertFailsWith<IllegalArgumentException> {
            CollectionMetadata(
                name = "users",
                fields = createFieldDefinitions(),
                primaryKey = PrimaryKeyDefinition(null, listOf("missing_id"))
            )
        }
    }

    @Test
    fun `test collection metadata rejects index fields missing from known schema`() {
        assertFailsWith<IllegalArgumentException> {
            CollectionMetadata(
                name = "users",
                fields = createFieldDefinitions(),
                indices = listOf(IndexDefinition(null, listOf(IndexFieldDefinition("missing_name"))))
            )
        }
    }

    @Test
    fun `test collection metadata rejects duplicate named indices`() {
        assertFailsWith<IllegalArgumentException> {
            CollectionMetadata(
                name = "users",
                fields = createFieldDefinitions(),
                indices = listOf(
                    IndexDefinition("users_name_idx", listOf(IndexFieldDefinition("name"))),
                    IndexDefinition("users_name_idx", listOf(IndexFieldDefinition("user_id")))
                )
            )
        }
    }

    @Test
    fun `test collection metadata allows indexed fields when schema is unknown`() {
        val metadata = CollectionMetadata(
            name = "users",
            fields = emptyMap(),
            indices = listOf(IndexDefinition(null, listOf(IndexFieldDefinition("name"))))
        )

        assertTrue(metadata.fields.isEmpty())
        assertContentEquals(listOf(IndexFieldDefinition("name")), metadata.indices.single().fields)
    }

    @Test
    fun `test primary key metadata rejects invalid definitions`() {
        assertFailsWith<IllegalArgumentException> {
            PrimaryKeyDefinition("", listOf("id"))
        }
        assertFailsWith<IllegalArgumentException> {
            PrimaryKeyDefinition(null, emptyList())
        }
        assertFailsWith<IllegalArgumentException> {
            PrimaryKeyDefinition(null, listOf("id", ""))
        }
        assertFailsWith<IllegalArgumentException> {
            PrimaryKeyDefinition(null, listOf("id", "ID"))
        }
    }

    @Test
    fun `test index metadata rejects invalid definitions`() {
        assertFailsWith<IllegalArgumentException> {
            IndexDefinition("", listOf(IndexFieldDefinition("name")))
        }
        assertFailsWith<IllegalArgumentException> {
            IndexDefinition(null, emptyList())
        }
        assertFailsWith<IllegalArgumentException> {
            IndexDefinition(null, listOf(IndexFieldDefinition("name"), IndexFieldDefinition("NAME")))
        }
    }

    @Test
    fun `test index field metadata rejects blank names`() {
        assertFailsWith<IllegalArgumentException> {
            IndexFieldDefinition(" ")
        }
    }

    private fun createFieldDefinitions() = mapOf(
        "user_id" to FieldDefinition("user_id", "string"),
        "name" to FieldDefinition("name", "string")
    )
}
