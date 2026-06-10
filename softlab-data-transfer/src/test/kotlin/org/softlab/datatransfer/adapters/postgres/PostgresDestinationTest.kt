package org.softlab.datatransfer.adapters.postgres

import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.softlab.datatransfer.config.DataTypeMappings
import kotlin.test.assertEquals


class PostgresDestinationTest {
    @Test
    fun `castValue() converts numeric values for boolean target columns`() {
        val destination = PostgresDestination(
            pool = mockk(),
            dialect = mockk(),
            dataTypeMappings = DataTypeMappings(),
            closePool = false
        )

        assertEquals(false, destination.castValue("boolean", 0.toByte()))
        assertEquals(true, destination.castValue("boolean", 1.toShort()))
        assertEquals(false, destination.castValue("bool", 0))
        assertEquals(true, destination.castValue("bool", 1L))
        assertEquals("true", destination.castValue("boolean", "true"))
        assertEquals(1, destination.castValue("integer", 1))
    }
}
