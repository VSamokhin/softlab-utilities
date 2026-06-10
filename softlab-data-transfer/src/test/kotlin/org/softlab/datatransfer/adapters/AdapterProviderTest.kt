package org.softlab.datatransfer.adapters

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource


class AdapterProviderTest {
    companion object {
        @JvmStatic
        fun getSources(): List<Arguments> = listOf(
            Arguments.of("jdbc:postgres://localhost:5432/db", AdapterProvider.JOBS_HINT),
            Arguments.of("jdbc:sqlite:///path/to/db.sqlite", AdapterProvider.JOBS_HINT),
            Arguments.of("redis://localhost:6379/db", AdapterProvider.REDIS_SOURCE_MAPPING)
        )

        @JvmStatic
        fun getDestinations(): List<Arguments> = listOf(
            Arguments.of("jdbc:postgres://localhost:5432/db", AdapterProvider.JOBS_HINT),
            Arguments.of("redis://localhost:6379", AdapterProvider.REDIS_SOURCE_MAPPING)
        )
    }

    @Test
    fun `sourceFor() throws for unknown URI`() {
        val exc = assertThrows<IllegalStateException> {
            AdapterProvider.sourceFor("unknown://db")
        }
        assertThat(exc.message, containsString("unknown://db"))
    }

    @Test
    fun `destinationFor() throws for unknown URI`() {
        val exc = assertThrows<IllegalStateException> {
            AdapterProvider.destinationFor("unknown://db")
        }
        assertThat(exc.message, containsString("unknown://db"))
    }

    @ParameterizedTest
    @MethodSource("getSources")
    fun `sourceFor() throws when additional option is missing`(uri: String, option: String) {
        val exc = assertThrows<IllegalStateException> {
            AdapterProvider.sourceFor(uri)
        }
        assertThat(exc.message, containsString(option))
    }

    @ParameterizedTest
    @MethodSource("getDestinations")
    fun `destinationFor() throws when additional option is missing`(uri: String, option: String) {
        val exc = assertThrows<IllegalStateException> {
            AdapterProvider.destinationFor(uri)
        }
        assertThat(exc.message, containsString(option))
    }
}
