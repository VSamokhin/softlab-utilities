package org.softlab.dataset

import io.github.oshai.kotlinlogging.KLogger
import io.mockk.mockk
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import java.nio.file.Files
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith


class YamlDatasetLoadingTest {
    @Test
    fun `load() reads YAML dataset and delegates parsed content`() {
        val file = Files
            .createTempFile("yaml-dataset-loading-test", ".yml")
            .apply { toFile().deleteOnExit() }
        file.writeText(
            """
            users:
              - id: 1
                name: Alice
            """.trimIndent()
        )
        val cut = TestYamlDatasetLoading()
        cut.load(file.toString(), cleanBefore = true)

        assertEquals(true, cut.cleanBefore)
        assertEquals(
            mapOf(
                "users" to listOf(
                    mapOf("id" to 1, "name" to "Alice")
                )
            ),
            cut.dataset
        )
    }

    @Test
    fun `load() throws when dataset cannot be found`() {
        val cut = TestYamlDatasetLoading()

        val exc = assertFailsWith<IllegalStateException> {
            cut.load("missing-dataset.yml")
        }

        assertThat(exc.message, containsString("missing-dataset.yml"))
    }

    private class TestYamlDatasetLoading : YamlDatasetLoading() {
        override val logger: KLogger = mockk(relaxed = true)
        var dataset: YamlTablesRows? = null
            private set
        var cleanBefore: Boolean? = null
            private set

        override fun loadImpl(dataset: YamlTablesRows, cleanBefore: Boolean) {
            this.dataset = dataset
            this.cleanBefore = cleanBefore
        }
    }
}
