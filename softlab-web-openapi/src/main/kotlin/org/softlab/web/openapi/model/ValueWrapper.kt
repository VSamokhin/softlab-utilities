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

package org.softlab.web.openapi.model


interface ValueWrapper {
    /**
     * To use while setting QUERY or PATH parameter values
     * @param hint target content type
     * @return a value for the target content type or throw an exception if conversion is not possible
     */
    fun asString(hint: Parameter): String

    /**
     * To use while setting BODY parameter values
     * @param hint target content type
     * @return a value for the target content type or throw an exception if conversion is not possible
     */
    fun asBytes(hint: Parameter): ByteArray
}
