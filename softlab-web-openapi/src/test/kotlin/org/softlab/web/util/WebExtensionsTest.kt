package org.softlab.web.util

import io.mockk.every
import io.mockk.mockk
import org.hamcrest.CoreMatchers.allOf
import org.hamcrest.CoreMatchers.containsString
import org.hamcrest.CoreMatchers.not
import org.hamcrest.MatcherAssert.assertThat
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.File

class WebExtensionsTest {
    @Test
    fun `checkIsOk should not throw exception for OK status`() {
        val response = mockk<Response> {
            every { status } returns Status.OK
        }

        response.checkIsOk { "Error message" }
    }

    @Test
    fun `checkIsOk should throw exception on non-OK status`() {
        val responseMessage = "Bad Request"
        val response = mockk<Response> {
            every { status } returns Status.BAD_REQUEST
            every { toMessage() } returns responseMessage
        }

        val errorMessage = "Request failed"
        val exception = assertThrows<IllegalStateException> {
            response.checkIsOk { errorMessage }
        }
        assertThat(exception.message, allOf(
            containsString(errorMessage),
            containsString(responseMessage))
        )
    }

    @Test
    fun `curl() should generate correct curl command for request with payload and headers`() {
        val payload = "test-payload"
        val request = Request(Method.POST, "/api/test")
            .header("Authorization", "Bearer token")
            .header("Custom-Header", "value")
            .body(payload)

        val curlCommand = request.curl()

        // Check that the curl command contains method, headers, payload file, and uri
        assertThat(curlCommand, containsString("-X POST"))
        assertThat(curlCommand, containsString("-H \"Authorization: Bearer token\""))
        assertThat(curlCommand, containsString("-H \"Custom-Header: value\""))
        assertThat(curlCommand, containsString("--data-binary \"@test\""))
        assertThat(curlCommand, containsString("\"/api/test\""))

        // Clean up the payload file
        File("test").delete()
    }

    @Test
    fun `curl() should generate correct curl command for request without payload`() {
        val request = Request(Method.GET, "/api/empty")
        val curlCommand = request.curl()
        assertThat(curlCommand, containsString("-X GET"))
        assertThat(curlCommand, containsString("\"/api/empty\""))
        assertThat(curlCommand, not(containsString("--data-binary")))
    }

    @Test
    fun `curl() should generate correct curl command for request without headers`() {
        val request = Request(Method.PUT, "/api/no-headers").body("no-header-body")
        val curlCommand = request.curl()
        assertThat(curlCommand, containsString("-X PUT"))
        assertThat(curlCommand, containsString("\"/api/no-headers\""))
        assertThat(curlCommand, containsString("--data-binary \"@no-headers\""))
        // Should not contain any -H header options
        assertThat(curlCommand, not(containsString("-H ")))

        // Clean up the payload file
        File("no-headers").delete()
    }

    @Test
    fun `curl() should generate correct curl command for request with header but no value`() {
        val request = Request(Method.PATCH, "/api/header-novalue")
            .header("X-Empty-Header", null)
            .body("body-content")
        val curlCommand = request.curl()
        assertThat(curlCommand, containsString("-X PATCH"))
        assertThat(curlCommand, containsString("\"/api/header-novalue\""))
        assertThat(curlCommand, containsString("--data-binary \"@header-novalue\""))
        // Should include the header with no value (as just the header name and colon)
        assertThat(curlCommand, containsString("-H \"X-Empty-Header;\""))

        // Clean up the payload file
        File("header-novalue").delete()
    }
}
