package org.softlab.dataset.redis

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull


class RedisMappingTemplateTest {
    @Test
    fun `placeholders() returns template placeholders in encounter order`() {
        val cut = RedisMappingTemplate.of("users:\${id}:roles:\${role}")

        assertEquals(linkedSetOf("id", "role"), cut.placeholders())
    }

    @Test
    fun `render() replaces placeholders from values`() {
        val cut = RedisMappingTemplate.of("users:\${id}:roles:\${role}")

        val result = cut.render(mapOf("id" to "42", "role" to "admin"))

        assertEquals("users:42:roles:admin", result)
    }

    @Test
    fun `toGlob() replaces placeholders with wildcard`() {
        val cut = RedisMappingTemplate.of("users:\${id}:roles:\${role}")

        assertEquals("users:*:roles:*", cut.toGlob())
    }

    @Test
    fun `match() extracts values from rendered string`() {
        val cut = RedisMappingTemplate.of("users:\${id}:roles:\${role}")

        val result = cut.match("users:42:roles:admin")

        assertEquals(linkedMapOf("id" to "42", "role" to "admin"), result)
    }

    @Test
    fun `match() returns null for conflicting repeated placeholder values`() {
        val cut = RedisMappingTemplate.of("users:\${id}:shadow:\${id}")

        assertNull(cut.match("users:42:shadow:43"))
    }
}
