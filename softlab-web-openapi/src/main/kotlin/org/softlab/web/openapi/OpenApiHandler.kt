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

package org.softlab.web.openapi

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.oshai.kotlinlogging.KotlinLogging
import org.http4k.core.HttpHandler
import org.http4k.core.MemoryBody
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.format.Jackson.asJsonObject
import org.softlab.web.openapi.model.Endpoint
import org.softlab.web.openapi.model.Parameter
import org.softlab.web.openapi.model.ParameterDataTypeComparator
import org.softlab.web.openapi.model.ParameterType
import org.softlab.web.openapi.model.ValueWrapper
import org.softlab.web.util.checkIsOk
import org.softlab.web.util.curl


/**
 * A handler for OpenAPI endpoints, which allows to load an OpenAPI definition and request its endpoints.
 * This class does not do any kind of parameter or response validation, it checks only for those fields it needs
 * for the correct work. The request body value is always converted to bytes, other values – to strings.
 * Proper parameter conversion is a matter of your knowledge of the backend
 * and correct implementation of [ValueWrapper].
 * @param client the HTTP client to use for making requests, initialize it to handle proper authentication, if any,
 * [using the client filters](https://www.http4k.org/howto/secure_and_auth_http)
 * @param contentTypeComparator a comparator to determine the preferable content type for request bodies
 */
class OpenApiHandler(
    val client: HttpHandler,
    private val contentTypeComparator: Comparator<Parameter> = ParameterDataTypeComparator(emptyMap())
) {

    companion object {
        const val REQUEST_BODY_ATTR: String = "requestBody"

        /**
         * Some of the headers like `Accept`, `Content-Type` or `Authorization` shall not have a parameter definition
         * so this is a data type stub for those headers
         */
        const val HEADER_DATA_TYPE: String = "string"

        private val YAML_MAPPER: ObjectMapper = ObjectMapper(YAMLFactory()).registerKotlinModule()
    }

    /**
     * Base server URL specified in the OpenAPI definition
     */
    lateinit var serverUrl: String

    private val logger = KotlinLogging.logger {}

    internal lateinit var endpoints: Map<String, Endpoint>

    /**
     * Show whether any endpoint is available after loading the OpenAPI definition
     */
    fun noEndpoints(): Boolean = endpoints.isEmpty()

    fun loadDefinition(url: String, format: String = url.substringAfterLast('.')): OpenApiHandler {
        val definition = when (format.lowercase()) {
            "json" -> fetchJson(url, client)
            "yaml", "yml" -> fetchYaml(url, client)
            else -> error("Unsupported OpenAPI definition format: $format")
        }
        serverUrl = definition["servers"]?.get(0)?.get("url")?.textValue() ?: let {
            url.substringBeforeLast('/')
        }
        logger.info {
            "Will be using the following server url: $serverUrl"
        }
        endpoints = (definition.get("paths")?.properties() ?: emptySet())
            .flatMap { parsePathItem(it) }
            .fold(mutableMapOf()) { map, entry ->
                val id = entry.operationId
                val oldId = map.put(id, entry)
                check(oldId == null) { "operationId is not unique: $id" }
                map
            }
        return this
    }

    /**
     * Request an OpenAPI endpoint by its operationId or path
     * @param operationId the ID of the operation to call or the endpoint path if operationId is not specified
     * @param values a map of parameter names to their values, which can be used to set PATH, QUERY and BODY parameters
     * @param outputCurlCommand indicates whether to log the generated cURL command for the request
     * (with the request payload if present)
     */
    @Suppress("CyclomaticComplexMethod")
    fun request(
        operationId: String,
        values: Map<String, ValueWrapper> = mapOf(),
        outputCurlCommand: Boolean = false
    ): Response {
        val endpoint = requireNotNull(endpoints[operationId]) {
            "Endpoint '$operationId' was not found among: ${endpoints.map { it.key }}"
        }

        var callUrl = "$serverUrl${endpoint.path}"

        // Set PATH parameters in URL
        endpoint.parameters.filter { it.inType == ParameterType.PATH }.map { p ->
            p.name to checkNotNull(
                values[p.name]?.asString(p)
            ) {
                "Could not find value for a required PATH parameter: $operationId.${p.name}"
            }
        }.forEach {
            callUrl = callUrl.replace("{${it.first}}", it.second)
        }

        var request = Request(endpoint.method, callUrl)

        // Set QUERY parameters
        endpoint.parameters.filter { it.inType == ParameterType.QUERY }.forEach { p ->
            val value = values[p.name]?.asString(p)
            check(!p.required || (p.required && value != null)) {
                "Could not find value for a required QUERY parameter: $operationId.${p.name}"
            }
            if (value != null) {
                request = request.query(p.name, value)
            }
        }

        val headers = linkedMapOf<String, LinkedHashSet<String>>()

        // Set BODY parameter
        endpoint.parameters.bodyPreferredType()?.also { p ->
            val value = values[p.name]?.asBytes(p)
            if (value != null) {
                request = request.body (MemoryBody(value))
            } else if (p.required) error(
                "Could not find value for a required BODY parameter: $operationId.$REQUEST_BODY_ATTR"
            )
            headers.computeIfAbsent("Content-Type") { linkedSetOf() }.add(p.dataType)
        }

        // Set HEADER parameters
        endpoint.parameters.filter { it.inType == ParameterType.HEADER }.forEach { p ->
            val value = values[p.name]?.asString(p)
            check(!p.required || (p.required && value != null)) {
                "Could not find value for a required HEADER parameter: $operationId.${p.name}"
            }
            if (value != null) {
                headers.computeIfAbsent(p.name) { linkedSetOf() }.add(value)
            }
        }

        // Set headers
        headers.forEach { (name, vals) ->
            vals.forEach { value ->
                request = request.header(name, value)
            }
        }

        if (outputCurlCommand) logger.info { "Curl: ${request.curl()}" }
        logger.debug { "Calling '$operationId' at ${request.uri}" }
        return client(request)
    }

    private fun parsePathItem(pathNode: Map.Entry<String, JsonNode>): Sequence<Endpoint> = sequence {
        val endpoint = pathNode.key
        pathNode.value.properties().map {
            val methodType = Method.valueOf(it.key.uppercase())
            logger.debug { "Parsing '$methodType.$endpoint'" }
            val methodNode = it.value
            val operationId = methodNode.get("operationId")?.textValue() ?: run {
                logger.warn {
                    "operationId is not specified for '$methodType.$endpoint', using endpoint as operationId"
                }
                endpoint
            }
            val parameters = mutableListOf<Parameter>().apply {
                addAll(parseParameters(endpoint, methodNode.get("parameters")))
                addAll(parseRequestBody(endpoint, methodNode.get(REQUEST_BODY_ATTR)))
            }
            yield(Endpoint(endpoint, methodType, operationId, parameters))
        }
    }

    // Ideally I would not care about the parameter data type because all values finally are converted into strings,
    // but I still can imagine some cases where one would need additional information to be able to perform the
    // conversion correctly, thus I keep this overhead
    private fun parseParameters(endpoint: String, parametersNode: JsonNode?): List<Parameter> {
        return parametersNode?.map { parameter ->
            val ref = parameter["\$ref"]
            check(ref == null) {
                "\$ref in parameters is not supported: $endpoint.$ref"
            }
            val name = parameter["name"]?.textValue() ?: error(
                "Property 'name' is mandatory for parameter: '$endpoint'"
            )
            val inType = parameter["in"]?.textValue()?.let {
                ParameterType.valueOf(it.uppercase())
            } ?: error("Property 'in' is mandatory for parameter: $endpoint.$name")
            // For PATH parameter is always required and must be true
            val required = if (inType == ParameterType.PATH) true else parameter["required"]?.asBoolean() ?: false
            val schema = parameter["schema"]
            val dataType = if (schema == null) {
                if (inType == ParameterType.HEADER) HEADER_DATA_TYPE else error(
                    "Property 'schema' is mandatory for parameter: $endpoint.$name"
                )
            } else schema.get("type")?.textValue() ?: error(
                "Property 'type' is mandatory for schema of: $endpoint.$name"
            )
            Parameter(name, inType, required, dataType)
        }?.toList() ?: listOf()
    }

    private fun parseRequestBody(endpoint: String, bodyNode: JsonNode?): List<Parameter> {
        return bodyNode?.let {
            val name = REQUEST_BODY_ATTR
            val required = bodyNode["required"]?.asBoolean() ?: false
            val inType = ParameterType.BODY
            bodyNode["content"]?.properties()?.map { content ->
                Parameter(name, inType, required, content.key)
            } ?: error(
                "Property 'content' is mandatory for request body of: $endpoint"
            )
        } ?: listOf()
    }

    internal fun List<Parameter>.bodyPreferredType(): Parameter? =
        this.filter { it.inType == ParameterType.BODY }.sortedWith(contentTypeComparator).firstOrNull()

    private fun fetchFromUrl(url: String, client: HttpHandler): String {
        client(Request(Method.GET, url)).use {
            it.checkIsOk {
                "Received unexpected response ${it.status.code} while requesting $url"
            }
            return it.bodyString()
        }
    }

    private fun fetchJson(url: String, client: HttpHandler): JsonNode {
        logger.debug { "Fetching JSON from: $url" }
        return fetchFromUrl(url, client).asJsonObject()
    }

    private fun fetchYaml(url: String, client: HttpHandler): JsonNode {
        logger.debug { "Fetching YAML from: $url" }
        val content = fetchFromUrl(url, client)
        return YAML_MAPPER.readValue(content)
    }
}
