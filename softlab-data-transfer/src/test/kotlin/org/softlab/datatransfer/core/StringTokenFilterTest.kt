package org.softlab.datatransfer.core

import org.junit.jupiter.api.Assertions.assertIterableEquals
import org.junit.jupiter.api.Test
import java.nio.file.Files
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class StringTokenFilterTest {
    @Test
    fun `from() returns normalized provided tokens when no file is specified`() {
        val filter = StringTokenFilter.from(listOf(" users ", "orders"))

        assertIterableEquals(listOf("users", "orders"), filter.tokens)
    }

    @Test
    fun `from() loads normalized tokens from file`() {
        val tempFile = Files.createTempFile("source-filters", ".txt")
        try {
            Files.writeString(
                tempFile,
                """
                users

                 orders
                """.trimIndent()
            )

            val filter = StringTokenFilter.from(listOf(tempFile.toString()))

            assertIterableEquals(listOf("users", "orders"), filter.tokens)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `startsWith() matches any configured token prefix`() {
        val filter = StringTokenFilter.from(listOf("users", "profiles"))

        assertTrue(filter.startsWith("users_active"))
        assertTrue(filter.startsWith("profiles_archive"))
        assertFalse(filter.startsWith("orders"))
    }

    @Test
    fun `startsWith() matches everything when no tokens configured`() {
        val filter = StringTokenFilter.from(emptyList())

        assertTrue(filter.startsWith("anything"))
    }
}
