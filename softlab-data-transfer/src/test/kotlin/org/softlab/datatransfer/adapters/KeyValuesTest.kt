package org.softlab.datatransfer.adapters

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.NullAndEmptySource
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals


class KeyValuesTest {
    companion object {
        @JvmStatic
        fun getParseValues(): List<Arguments> = listOf(
            Arguments.of(
                mapOf("mapping" to "mappings/redis.yml", "mode" to "strict"),
                "mapping=mappings/redis.yml,mode=strict"
            ),
            Arguments.of(
                mapOf("param1" to "val1", "param2" to "val2 + val3"),
                " param1 = val1 ,  param2 =val2 + val3"
            )
        )
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = [" "])
    fun `parse() returns empty map for null or blank input`(options: String?) {
        assertEquals(emptyMap(), KeyValues.parse(options))
    }

    @ParameterizedTest
    @MethodSource("getParseValues")
    fun `parse() parses comma-separated key-value pairs`(expected: Map<String, String>, value: String) {
        assertEquals(expected, KeyValues.parse(value))
    }

    @Test
    fun `parse() throws for malformed option`() {
        val exc = assertThrows<IllegalStateException> {
            KeyValues.parse("mapping")
        }

        assertThat(exc.message,allOf(
            containsString("'mapping'"),
            containsString("key=value")))
    }
}
