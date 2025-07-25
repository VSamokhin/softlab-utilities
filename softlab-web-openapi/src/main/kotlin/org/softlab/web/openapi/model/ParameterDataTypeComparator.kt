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


/**
 * Compare two [Parameter] instances on their preference based on the provided weights of their data/content types
 * @param weights a map of data/content types to their weights, lower weight means higher preference
 */
class ParameterDataTypeComparator(private val weights: Map<String, Int>) : Comparator<Parameter> {
    override fun compare(o1: Parameter, o2: Parameter): Int {
        require(o1.inType == o2.inType) {
            "Cannot compare parameters with different inTypes: ${o1.inType} and ${o2.inType}"
        }
        val weight1 = weights[o1.dataType] ?: Int.MAX_VALUE
        val weight2 = weights[o2.dataType] ?: Int.MAX_VALUE
        return if (weight1 != weight2) {
            weight1 - weight2
        } else {
            o1.dataType.compareTo(o2.dataType)
        }
    }
}
