package org.softlab.web.openapi

import com.fasterxml.jackson.core.JsonParseException
import com.github.tomakehurst.wiremock.common.ConsoleNotifier
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.github.tomakehurst.wiremock.junit5.WireMockExtension
import com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy
import org.apache.hc.core5.ssl.SSLContextBuilder
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.equalTo
import org.http4k.client.ApacheClient
import org.http4k.core.Status
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.RegisterExtension
import org.softlab.web.openapi.OpenApiHandler.Companion.REQUEST_BODY_ATTR
import org.softlab.web.openapi.model.Parameter
import org.softlab.web.openapi.model.ParameterDataTypeComparator
import org.softlab.web.openapi.model.ParameterType
import org.softlab.web.openapi.model.StringValue


class OpenApiHandlerTest {
    companion object {
        private val COMPARATOR = ParameterDataTypeComparator(
            mapOf(
                "application/json" to 1,
                "string" to 2
            )
        )

        @RegisterExtension
        @JvmStatic
        private val WM: WireMockExtension = WireMockExtension.newInstance()
            .options(wireMockConfig()
                .dynamicPort()
                .notifier(ConsoleNotifier(true))
            ).build()
    }

    private val url = "http://localhost:${WM.port}"
    private val correctDefinitionUrl = "$url/openapi.json"


    private val httpClient = ApacheClient(testHttpClient())

    @Test
    fun `serverUrl returns correct value`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        assertEquals(url, cut.serverUrl)
    }

    @Test
    fun `loadDefinition() should throw exception on wrong definition format`() {
        val cut = OpenApiHandler(httpClient)
        assertThrows<JsonParseException> {
            cut.loadDefinition("$url/openapi.yaml", "json")
        }
    }

    @Test
    fun `loadDefinition() should throw exception on unsupported definition format`() {
        val cut = OpenApiHandler(httpClient)
        val exc = assertThrows<IllegalStateException> {
            cut.loadDefinition("$url/openapi.txt")
        }
        assertThat(exc.message, allOf(
            containsString("Unsupported"),
            containsString("format"),
            containsString("txt"))
        )
    }

    @Test
    fun `loadDefinition() should throw exception for failed responses`() {
        val cut = OpenApiHandler(httpClient)
        val exc = assertThrows<IllegalStateException> {
            cut.loadDefinition("$url/404-openapi.json")
        }
        assertThat(exc.message, containsString("404"))
    }

    @Test
    fun `loadDefinition() should throw exception on ambiguous operationId`() {
        val cut = OpenApiHandler(httpClient)
        val exc = assertThrows<IllegalStateException> {
            cut.loadDefinition("$url/invalid-openapi.json")
        }
        assertThat(exc.message, containsString("getTest"))
    }

    @Test
    fun `loadDefinition() should throw exception on referenced parameters`() {
        val cut = OpenApiHandler(httpClient)
        val exc = assertThrows<IllegalStateException> {
            cut.loadDefinition("$url/referenced-params-openapi.json")
        }
        assertThat(exc.message, allOf(
            containsString("\$ref"),
            containsString("""/test."#/components/parameters/TestParam""""))
        )
    }

    @Test
    fun `loadDefinition() should throw exception on missing parameter-name`() {
        val cut = OpenApiHandler(httpClient)
        val exc = assertThrows<IllegalStateException> {
            cut.loadDefinition("$url/missing-param-name-openapi.json")
        }
        assertThat(exc.message, allOf(
            containsString("name"),
            containsString("/test"))
        )
    }

    @Test
    fun `loadDefinition() should throw exception on missing parameter-inType`() {
        val cut = OpenApiHandler(httpClient)
        val exc = assertThrows<IllegalStateException> {
            cut.loadDefinition("$url/missing-param-in-openapi.json")
        }
        assertThat(exc.message, allOf(
            containsString("in"),
            containsString("/test.param1"))
        )
    }

    @Test
    fun `loadDefinition() should throw exception on missing parameter-schema`() {
        val cut = OpenApiHandler(httpClient)
        val exc = assertThrows<IllegalStateException> {
            cut.loadDefinition("$url/missing-param-schema-openapi.json")
        }
        assertThat(exc.message, allOf(
            containsString("schema"),
            containsString("/test.param1"))
        )
    }

    @Test
    fun `loadDefinition() should throw exception on missing parameter-schema-type`() {
        val cut = OpenApiHandler(httpClient)
        val exc = assertThrows<IllegalStateException> {
            cut.loadDefinition("$url/missing-param-schema-type-openapi.json")
        }
        assertThat(exc.message, allOf(
            containsString("type"),
            containsString("/test.param1"))
        )
    }

    @Test
    fun `loadDefinition() should throw exception on missing requestBody-content`() {
        val cut = OpenApiHandler(httpClient)
        val exc = assertThrows<IllegalStateException> {
            cut.loadDefinition("$url/missing-body-content-openapi.json")
        }
        assertThat(exc.message, allOf(
            containsString("content"),
            containsString("request body"),
            containsString("/test"))
        )
    }

    @Test
    fun `loadDefinition() should assign default content type on missing requestBody-content-schema`() {
        val cut = OpenApiHandler(httpClient).loadDefinition("$url/missing-body-content-schema-openapi.json")

        val ep = cut.endpoints.values.singleOrNull()
        assertNotNull(ep)

        val actual = ep?.parameters?.singleOrNull()
        assertNotNull(actual)
        assertEquals(ParameterType.BODY, actual?.inType)
        assertEquals("application/json", actual?.dataType)
    }

    @Test
    fun `loadDefinition() should assign default content type on missing requestBody-content-schema-type or ref`() {
        val cut = OpenApiHandler(httpClient).loadDefinition("$url/missing-body-content-schema-type-openapi.json")

        val ep = cut.endpoints.values.singleOrNull()
        assertNotNull(ep)

        val actual = ep?.parameters?.singleOrNull()
        assertNotNull(actual)
        assertEquals(ParameterType.BODY, actual?.inType)
        assertEquals("application/json", actual?.dataType)
    }

    @Test
    fun `request() should throw exception on unknown operationId`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        val exc = assertThrows<IllegalArgumentException> {
            cut.request("whatever1")
        }
        assertThat(exc.message, containsString("whatever1"))
    }

    @Test
    fun `request() should execute a GET request with required QUERY parameter`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        val response = cut.request("testGetQuery", mapOf("queryParam1" to StringValue("value1")))
        assertEquals(Status.OK, response.status)
        assertThat(
            response.bodyString(),
            hasJsonPath("$.testGetQuery", equalTo("success"))
        )
    }

    @Test
    fun `request() should throw exception on missing required QUERY parameter`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        val exc = assertThrows<IllegalStateException> {
            cut.request("testGetQuery")
        }
        assertThat(exc.message, containsString("testGetQuery.queryParam1") )
    }

    @Test
    fun `request() should execute a GET request without optional QUERY parameter`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        val response = cut.request("testGetQueryOptional")
        assertEquals(Status.OK, response.status)
        assertThat(
            response.bodyString(),
            hasJsonPath("$.testGetQueryOptional", equalTo("success"))
        )
    }

    @Test
    fun `request() should execute a GET request with required PATH parameter`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        val response = cut.request("testGetPath", mapOf("pathParam1" to StringValue("value1") ))
        assertEquals(Status.OK, response.status)
        assertThat(
            response.bodyString(),
            hasJsonPath("$.testGetPath", equalTo("success"))
        )
    }

    @Test
    fun `request() should throw exception on missing required PATH parameter`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        val exc = assertThrows<IllegalStateException> {
            cut.request("testGetPath")
        }
        assertThat(exc.message, allOf(
            containsString("PATH"),
            containsString("testGetPath.pathParam1"))
        )
    }

    @Test
    fun `request() should execute a GET request with PATH parameter from YAML definition`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition("$url/openapi.yml")
        val response = cut.request("testGetPath", mapOf("pathParam1" to StringValue("value1") ))
        assertEquals(Status.OK, response.status)
        assertThat(
            response.bodyString(),
            hasJsonPath("$.testGetPath", equalTo("success"))
        )
    }

    @Test
    fun `request() should send body for POST request with required body parameter`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        val response = cut.request(
            "testPostBody",
            mapOf(REQUEST_BODY_ATTR to StringValue("""{"param1": "value1"}""")))
        assertEquals(Status.OK, response.status)
        assertThat(
            response.bodyString(),
            hasJsonPath("$.answer", equalTo("success"))
        )
    }

    @Test
    fun `request() should throw exception on missing required BODY parameter`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        val exc = assertThrows<IllegalStateException> {
            cut.request("testPostBody")
        }
        assertThat(exc.message, allOf(
            containsString("BODY"),
            containsString("testPostBody.requestBody"))
        )
    }

    @Test
    fun `request() should execute a POST request without optional body parameter`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        val response = cut.request("testPostBodyOptional")
        assertEquals(Status.OK, response.status)
        assertThat(
            response.bodyString(),
            hasJsonPath("$.testPostBodyOptional", equalTo("success"))
        )
    }

    @Test
    fun `request() should execute a GET request with required HEADER parameter`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        val response = cut.request("testGetHeader", mapOf("header1" to StringValue("value1")))
        assertEquals(Status.OK, response.status)
        assertThat(
            response.bodyString(),
            hasJsonPath("$.testGetHeader", equalTo("success"))
        )
    }

    @Test
    fun `request() should throw exception on missing required HEADER parameter`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        val exc = assertThrows<IllegalStateException> {
            cut.request("testGetHeader")
        }
        assertThat(exc.message, containsString("testGetHeader.header1") )
    }

    @Test
    fun `request() should execute a GET request without optional HEADER parameter`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        val response = cut.request("testGetHeaderOptional")
        assertEquals(Status.OK, response.status)
        assertThat(
            response.bodyString(),
            hasJsonPath("$.testGetHeaderOptional", equalTo("success"))
        )
    }

    @Test
    fun `noEndpoints() should return true when no endpoints are loaded`() {
        val cut = OpenApiHandler(httpClient).loadDefinition("$url/empty-openapi.json")
        assertTrue(cut.noEndpoints())
    }

    @Test
    fun `loadDefinition() should return false when endpoints are loaded`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition("$url/openapi.yaml")
        assertFalse(cut.noEndpoints())
    }

    @Test
    fun `bodyPreferredType() should return correctly sorted content descriptor`() {
        val cut = OpenApiHandler(httpClient, COMPARATOR).loadDefinition(correctDefinitionUrl)
        val param1 = Parameter("param1", ParameterType.BODY, false, "string")
        val param2 = Parameter("param2", ParameterType.BODY, true, "int")
        val param3 = Parameter("param3", ParameterType.PATH, false, "string")
        val param4 = Parameter("param4", ParameterType.PATH, true, "application/json")

        val actual = with(cut) { listOf(param1, param2, param3, param4).bodyPreferredType() }
        assertSame(param1, actual)
    }

    private fun testHttpClient(): CloseableHttpClient = SSLContextBuilder()
        .loadTrustMaterial(null) { _,_ -> true }
        .build().run {
            HttpClientBuilder
                .create()
                .useSystemProperties()
                .setUserAgent("TestClient")
                .setDefaultRequestConfig(
                    RequestConfig
                        .copy(RequestConfig.DEFAULT)
                        .setHardCancellationEnabled(false)
                        .build()
                )
                .setConnectionManager(
                    PoolingHttpClientConnectionManagerBuilder
                        .create()
                        .setTlsSocketStrategy (DefaultClientTlsStrategy(this) { _, _ -> true })
                        .build()
            ).build()
    }
}
