/**
 * Copyright (C) 2026, Viktor Samokhin (wowyupiyo@gmail.com)
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

package org.softlab.datataset.test.initiators


interface DatabaseInitiator<T> : AutoCloseable {
    /**
     * The underlying database's URL
     */
    val dbUrl: String

    /**
     * Do a full database cleanup (e.g. drop schema or database)
     */
    fun cleanup(additionalSteps: (T) -> Unit = {})

    /**
     * Initialize the database schema using the provided Liquibase changelog file
     */
    fun initSchema(changelogPath: String, additionalSteps: (T) -> Unit = {})

    /**
     * Seed the database with test data given in the DBUnit YAML format
     */
    fun seedData(datasetPath: String)
}