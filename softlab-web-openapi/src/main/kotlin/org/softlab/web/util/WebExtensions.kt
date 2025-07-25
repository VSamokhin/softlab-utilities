/**
 * Copyright (C) 2023-2025, Viktor Samokhin (wowyupiyo@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.softlab.web.util

import org.http4k.appendIfNotBlank
import org.http4k.appendIfNotEmpty
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.quoted
import java.io.File


/**
 * If the response is not OK, throw an exception with the provided error message
 */
fun Response.checkIsOk(errorMessage: () -> String) {
    check(this.status == Status.OK) {
        "${errorMessage()}: \n${this.toMessage()}".trimEnd()
    }
}

/**
 * Create a curl command from the request, saving the payload if present to a binary file
 */
fun Request.curl(): String {
    val payloadFile = if ((body.length ?: 0) > 0) {
        val fileName = uri.path.split("/").last()
        File(fileName).writeBytes(body.payload.array())
        fileName
    } else ""

    return StringBuilder("curl")
        .append(" -X $method")
        .appendIfNotEmpty(headers, headers.joinToString("") { h ->
            """ -H ${(h.first + (h.second?.let { ": $it" } ?: ";")).quoted()}"""
        })
        .appendIfNotBlank(payloadFile, """ --data-binary "@$payloadFile"""")
        .append(""" "$uri"""")
        .toString()
}
