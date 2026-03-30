package org.softlab.dataset.redis

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull


class RedisMappingTemplateTest {
    @Test
    fun `of(table) uses anchor hash key template`() {
        val template = RedisMappingTemplate.of(
            RedisTableMapping(
                table = "users",
                hashes = listOf(
                    RedisHashMapping(key = "users:\${id}:meta", field = "status", value = "\${status}"),
                    RedisHashMapping(key = "users:\${id}")
                )
            )
        )

        assertEquals("users:*", template.toGlob())
    }

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
    fun `match() returns null when value does not match template`() {
        val cut = RedisMappingTemplate.of("users:\${id}:roles:\${role}")

        assertNull(cut.match("groups:42:roles:admin"))
    }

    @Test
    fun `match() returns null for conflicting repeated placeholder values`() {
        val cut = RedisMappingTemplate.of("users:\${id}:shadow:\${id}")

        assertNull(cut.match("users:42:shadow:43"))
    }

    @Test
    fun `exactPlaceholder() extracts placeholder name`() {
        assertEquals("id", RedisMappingTemplate.exactPlaceholder("\${id}"))
    }

    @Test
    fun `exactPlaceholder() throws for non-placeholder value`() {
        val exc = assertFailsWith<IllegalStateException> {
            RedisMappingTemplate.exactPlaceholder("users:\${id}")
        }

        assertThat(exc.message, containsString("users:\${id}"))
    }
}
