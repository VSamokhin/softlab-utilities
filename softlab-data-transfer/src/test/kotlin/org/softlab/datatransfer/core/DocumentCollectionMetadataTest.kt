package org.softlab.datatransfer.core

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals


class DocumentCollectionMetadataTest {
    @Test
    fun `test collection metadata equality`() {
        val metadata1 = CollectionMetadata(
            name = "users",
            fields = listOf(
                FieldMetadata("user_id", "string"),
                FieldMetadata("name", "string")
            )
        )

        val metadata2 = CollectionMetadata(
            name = "users",
            fields = listOf(
                FieldMetadata("user_id", "string"),
                FieldMetadata("name", "string")
            )
        )

        val metadata3 = CollectionMetadata(
            name = "products",
            fields = listOf(
                FieldMetadata("user_id", "string"),
                FieldMetadata("name", "string")
            )
        )

        assertEquals(metadata1, metadata2)
        assertNotEquals(metadata1, metadata3)
    }

    @Test
    fun `test field metadata equality`() {
        val field1 = FieldMetadata("id", "string")
        val field2 = FieldMetadata("id", "string")
        val field3 = FieldMetadata("id", "integer")

        assertEquals(field1, field2)
        assertNotEquals(field1, field3)
    }
}
