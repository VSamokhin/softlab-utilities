package org.softlab.dataset.core

import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.nio.file.Files
import kotlin.io.path.writeText
import kotlin.test.assertEquals
import kotlin.test.assertNull


class FileLoaderTest {
    @Test
    fun `readText() reads content from filesystem path`() {
        val file = Files.createTempFile("file-loader", ".txt")
        file.writeText("from-file")

        val content = FileLoader.readText(file.toString())

        assertEquals("from-file", content)
    }

    @Test
    fun `readText() falls back to classpath resource`() {
        val content = FileLoader.readText(
            "sample-resource.txt",
            classLoader = {
                object : ClassLoader() {
                    override fun getResourceAsStream(name: String?) =
                        if (name == "sample-resource.txt") {
                            ByteArrayInputStream("from-resource".toByteArray())
                        } else {
                            null
                        }
                }
            }
        )

        assertEquals("from-resource", content)
    }

    @Test
    fun `readText() prefers filesystem over classpath resource with same name`() {
        val file = Files.createTempFile("file-loader", ".txt")
        file.writeText("from-file")

        val content = FileLoader.readText(
            file.toString(),
            classLoader = {
                object : ClassLoader() {
                    override fun getResourceAsStream(name: String?) =
                        ByteArrayInputStream("from-resource".toByteArray())
                }
            }
        )

        assertEquals("from-file", content)
    }

    @Test
    fun `readText() returns null when file and resource are missing`() {
        val content = FileLoader.readText(
            "missing-resource.txt",
            classLoader = {
                object : ClassLoader() {
                    override fun getResourceAsStream(name: String?) = null
                }
            }
        )

        assertNull(content)
    }
}
