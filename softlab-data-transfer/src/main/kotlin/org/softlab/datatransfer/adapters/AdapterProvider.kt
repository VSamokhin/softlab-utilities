/**
 * Copyright (C) 2025-2026, Viktor Samokhin (wowyupiyo@gmail.com)
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

package org.softlab.datatransfer.adapters

import org.softlab.datatransfer.adapters.mongo.MongoDestination
import org.softlab.datatransfer.adapters.mongo.MongoSource
import org.softlab.datatransfer.adapters.postgres.PostgresDestination
import org.softlab.datatransfer.adapters.postgres.PostgresSource
import org.softlab.datatransfer.config.ConfigProvider
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.util.Mongo
import org.softlab.datatransfer.util.Postgres


object AdapterProvider {
    fun sourceFor(uri: String): DatabaseSource = when {
        Postgres.isPostgresUri(uri) -> PostgresSource(uri)
        Mongo.isMongoUri(uri) -> MongoSource(uri)
        else -> error("Unknown source database: $uri")
    }

    fun destinationFor(uri: String): DatabaseDestination {
        val dataTypeMappings = ConfigProvider.config.getDataTypeMappings()
        return when {
            Postgres.isPostgresUri(uri) -> PostgresDestination(
                uri,
                dataTypeMappings = dataTypeMappings.destination(PostgresDestination.BACKEND)
            )
            Mongo.isMongoUri(uri) -> MongoDestination(
                uri,
                dataTypeMappings = dataTypeMappings.destination(MongoDestination.BACKEND)
            )
            else -> error("Unknown destination database: $uri")
        }
    }
}
