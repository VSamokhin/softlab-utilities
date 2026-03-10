package org.softlab.datatransfer.adapters.mongo

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertContains


class MongoSourceTest {
    @Test
    fun `constructor fails with wrong connection string`() {
        val exc = assertThrows<IllegalArgumentException> {
            MongoSource("invalid://connection:string")
        }
        assertContains(exc.message!!, "connection string is invalid")
    }
}
