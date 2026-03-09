package org.softlab.datatransfer.adapters

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows


class AdapterProviderTest {
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
}
