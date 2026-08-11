package org.softlab.dataset.core

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals


class FieldDefinitionTest {
    @Test
    fun `test field definition equals contract`() {
        val field = FieldDefinition("id", "integer")
        val same = FieldDefinition("id", "integer")
        val different = FieldDefinition("id", "text")

        assertEquals(field, same)
        assertEquals(field.hashCode(), same.hashCode())
        assertNotEquals(field, different)
    }

    @Test
    fun `test field definition default values`() {
        assertFalse(FieldDefinition("id", "integer").nullable)
    }

    @Test
    fun `test field definition rejects blank name`() {
        assertFailsWith<IllegalArgumentException> {
            FieldDefinition("", "integer")
        }
    }

    @Test
    fun `test field definition rejects blank type`() {
        assertFailsWith<IllegalArgumentException> {
            FieldDefinition("id", " ")
        }
    }
}
