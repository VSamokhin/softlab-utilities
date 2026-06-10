package org.softlab.datatransfer.config

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class ConfigLoaderTest {
    companion object {
        private const val TEMP_DIR: String = "data-transfer-config-test"
    }

    @Test
    fun `load() reads default resource config`() {
        val mappings = ConfigLoader().load(ConfigProvider.CONFIG_FILE_NAME).getDataTypeMappings()
        assertEquals("INTEGER", mappings.destination("postgres").map("int"))
        assertEquals("DECIMAL", mappings.destination("postgres").map("decimal"))
        assertEquals("INTEGER", mappings.destination("sqlite").map("long"))
        assertEquals("INTEGER", mappings.destination("sqlite").map("bool"))
        assertEquals("TEXT", mappings.destination("sqlite").map("string"))
        assertEquals("string", mappings.destination("mongo").map("text"))
        assertEquals("int", mappings.source("redis").map("integer"))
        assertEquals("long", mappings.source("redis").map("bigint"))
        assertEquals("float", mappings.source("redis").map("real"))
        assertEquals("bool", mappings.source("redis").map("boolean"))
        assertEquals("timestamp", mappings.source("redis").map("timestamp"))
        assertEquals("bytearray", mappings.source("redis").map("bytea"))
        assertEquals("string", mappings.source("redis").map("text"))
        assertEquals("uuid", mappings.source("redis").map("uuid"))
        assertEquals("short", mappings.source("redis").map("smallint"))
        assertFalse("object" in mappings.destination("sqlite"))
        assertFalse("array" in mappings.destination("postgres"))
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
                    source:
                      redis:
                        number: [int]
                """.trimIndent()
            )
            System.setProperty("user.dir", tempDir.toString())

            val mappings = ConfigLoader().load(fileName).getDataTypeMappings()
            assertEquals("NUMERIC", mappings.destination("postgres").map("int"))
            assertEquals("text", mappings.destination("mongo").map("string"))
            assertEquals("number", mappings.source("redis").map("int"))
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
            assertThat(exc.message, containsString("int"))
        } finally {
            System.setProperty("user.dir", prevUserDir)
            tempDir.toFile().deleteRecursively()
        }
    }

    @Test
    fun `destination() is case-insensitive`() {
        val mappings = ConfigLoader().load(ConfigProvider.CONFIG_FILE_NAME).getDataTypeMappings()
        assertTrue("int" in mappings.destination("PoStGrEs"))
    }

    @Test
    fun `source() is case-insensitive`() {
        val mappings = ConfigLoader().load(ConfigProvider.CONFIG_FILE_NAME).getDataTypeMappings()
        assertTrue("int" in mappings.source("ReDiS"))
    }
}
