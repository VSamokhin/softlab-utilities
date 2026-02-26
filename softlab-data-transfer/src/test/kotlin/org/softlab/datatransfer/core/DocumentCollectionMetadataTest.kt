package org.softlab.datatransfer.core

import org.junit.jupiter.api.Test
import org.softlab.dataset.core.FieldDefinition
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals


class DocumentCollectionMetadataTest {
    @Test
    fun `test collection metadata equality`() {
        val metadata1 = CollectionMetadata(
            name = "users",
            fields = listOf(
                FieldDefinition("user_id", "string"),
                FieldDefinition("name", "string")
            )
        )

        val metadata2 = CollectionMetadata(
            name = "users",
            fields = listOf(
                FieldDefinition("user_id", "string"),
                FieldDefinition("name", "string")
            )
        )

        val metadata3 = CollectionMetadata(
            name = "products",
            fields = listOf(
                FieldDefinition("user_id", "string"),
                FieldDefinition("name", "string")
            )
        )

        assertEquals(metadata1, metadata2)
        assertNotEquals(metadata1, metadata3)
    }

    @Test
    fun `test field metadata equality`() {
        val field1 = FieldDefinition("id", "string")
        val field2 = FieldDefinition("id", "string")
        val field3 = FieldDefinition("id", "integer")

        assertEquals(field1, field2)
        assertNotEquals(field1, field3)
    }
}
