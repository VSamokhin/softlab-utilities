package org.softlab.datatransfer.config

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertTrue


class ConfigLoaderTest {
    companion object {
        private const val TEMP_DIR: String = "data-transfer-config-test"
    }

    @Test
    fun `load() reads default resource config`() {
        val mappings = ConfigLoader().load(ConfigProvider.CONFIG_FILE_NAME).getDataTypeMappings()
        assertEquals("INTEGER", mappings.destination("postgres")["int"])
        assertEquals("INTEGER", mappings.destination("postgres")["integer"])
        assertEquals("string", mappings.destination("mongo")["text"])
    }

    @ParameterizedTest
    @ValueSource(strings = [ "data-transfer.yml", ConfigProvider.CONFIG_FILE_NAME ])
    fun `load() prefers external config from current directory`(fileName: String) {
        val prevUserDir = System.getProperty("user.dir")
        val tempDir = Files.createTempDirectory(TEMP_DIR)
        try {
            Files.writeString(
                tempDir.resolve(fileName),
                """
                data-transfer:
                  data-type-mappings:
                    destination:
                      postgres:
                        NUMERIC: [int]
                      mongo:
                        text: [string]
                """.trimIndent()
            )
            System.setProperty("user.dir", tempDir.toString())

            val mappings = ConfigLoader().load(fileName).getDataTypeMappings()
            assertEquals("NUMERIC", mappings.destination("postgres")["int"])
            assertEquals("text", mappings.destination("mongo")["string"])
        } finally {
            System.setProperty("user.dir", prevUserDir)
            tempDir.toFile().deleteRecursively()
        }
    }

    @Test
    fun `load() fails on non-existent config file`() {
        val exc = assertThrows<IllegalStateException> {
            ConfigLoader().load("non-existent-config.yml")
        }
        assertThat(exc.message, containsString("non-existent-config.yml"))
    }

    @Test
    fun `getDataTypeMappings() fails on ambiguous mapping`() {
        val prevUserDir = System.getProperty("user.dir")
        val tempDir = Files.createTempDirectory(TEMP_DIR)
        try {
            val fileName = "ambiguous-config.yml"
            Files.writeString(
                tempDir.resolve(fileName),
                """
                data-transfer:
                  data-type-mappings:
                    destination:
                      postgres:
                        INTEGER: [int]
                        NUMERIC: [int]
                """.trimIndent()
            )
            System.setProperty("user.dir", tempDir.toString())

            val exc = assertThrows<IllegalStateException> {
                ConfigLoader().load(fileName).getDataTypeMappings()
            }
            assertThat(exc.message, containsString("'int'"))
        } finally {
            System.setProperty("user.dir", prevUserDir)
            tempDir.toFile().deleteRecursively()
        }
    }

    @Test
    fun `destination() is case-insensitive`() {
        val mappings = ConfigLoader().load(ConfigProvider.CONFIG_FILE_NAME).getDataTypeMappings()
        assertTrue(mappings.destination("PoStGrEs").containsKey("int"))
    }
}
