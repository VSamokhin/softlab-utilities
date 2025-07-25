package org.softlab.web.openapi.model

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.lessThan
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals


class ParameterDataTypeComparatorTest {
    companion object {
        private val WEIGHTS = mapOf(
            "application/json" to 1,
            "application/xml" to 1,
            "text/plain" to 2
        )
    }

    @Test
    fun `compare() should prefer parameter with lower weight`() {
        val cut = ParameterDataTypeComparator(WEIGHTS)
        val param1 = Parameter("param1", ParameterType.QUERY, true, "application/json")
        val param2 = Parameter("param2", ParameterType.QUERY, false,"text/plain")

        val actual = cut.compare(param1, param2)

        assertThat(actual, lessThan(0))
    }

    @Test
    fun `compare() should use lexicographical ordering for equal weights`() {
        val cut = ParameterDataTypeComparator(WEIGHTS)
        val param1 = Parameter("param1", ParameterType.QUERY, true, "application/json")
        val param2 = Parameter("param2", ParameterType.QUERY, false,"application/xml")

        val actual = cut.compare(param1, param2)

        assertEquals(param1.dataType.compareTo(param2.dataType), actual)
    }

    @Test
    fun `compare() should handle unknown types correctly`() {
        val cut = ParameterDataTypeComparator(WEIGHTS)
        val param1 = Parameter("param1", ParameterType.QUERY, true, "application/json")
        val param2 = Parameter("param2", ParameterType.QUERY, false,"unknown/type")

        val actual = cut.compare(param1, param2)

        assertThat(actual, lessThan(0))
    }

    @Test
    fun `compare() should handle both unknown types correctly`() {
        val cut = ParameterDataTypeComparator(WEIGHTS)
        val param1 = Parameter("param1", ParameterType.QUERY, true,"custom/type1")
        val param2 = Parameter("param2", ParameterType.QUERY, false,"custom/type2")

        val actual = cut.compare(param1, param2)

        assertEquals(param1.dataType.compareTo(param2.dataType), actual)
    }

    @Test
    fun `compare() should throw exception for parameters with different inTypes`() {
        val cut = ParameterDataTypeComparator(WEIGHTS)
        val param1 = Parameter("param1", ParameterType.QUERY, true,"application/json")
        val param2 = Parameter("param2", ParameterType.PATH, false,"application/json")

        val exception = assertThrows<IllegalArgumentException> {
            cut.compare(param1, param2)
        }

        assertThat(exception.message, allOf(
            containsString("QUERY"),
            containsString("PATH"))
        )
    }

    @Test
    fun `compare() should maintain transitivity`() {
        val cut = ParameterDataTypeComparator(WEIGHTS)
        val param1 = Parameter("param1", ParameterType.QUERY, true,"application/json")
        val param2 = Parameter("param2", ParameterType.QUERY, true,"application/xml")
        val param3 = Parameter("param3", ParameterType.QUERY, true,"text/plain")

        val actual12 = cut.compare(param1, param2)
        val actual23 = cut.compare(param2, param3)
        val actual13 = cut.compare(param1, param3)

        assertTrue(actual12 < 0 && actual23 < 0 && actual13 < 0)
    }
}
