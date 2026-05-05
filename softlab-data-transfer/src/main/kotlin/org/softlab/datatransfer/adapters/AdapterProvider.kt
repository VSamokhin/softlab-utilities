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
import org.softlab.datatransfer.adapters.postgres.ConnectionPool
import org.softlab.datatransfer.adapters.postgres.PostgresDestination
import org.softlab.datatransfer.adapters.postgres.PostgresSource
import org.softlab.datatransfer.adapters.redis.RedisDestination
import org.softlab.datatransfer.adapters.redis.RedisSource
import org.softlab.datatransfer.config.ConfigProvider
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.util.Mongo
import org.softlab.datatransfer.util.Postgres
import org.softlab.datatransfer.util.Redis


object AdapterProvider {
    private const val MAPPING_OPTION = "mapping"

    fun sourceFor(uri: String, options: Map<String, String> = emptyMap()): DatabaseSource = when {
        Postgres.isPostgresUri(uri) -> PostgresSource(ConnectionPool(uri))
        Mongo.isMongoUri(uri) -> MongoSource(uri)
        Redis.isRedisUri(uri) -> RedisSource(uri, requireRedisMappings(uri, options))
        else -> error("Unknown source database: $uri")
    }

    fun destinationFor(uri: String, options: Map<String, String> = emptyMap()): DatabaseDestination {
        val dataTypeMappings = ConfigProvider.config.getDataTypeMappings()
        return when {
            Postgres.isPostgresUri(uri) -> PostgresDestination(
                ConnectionPool(uri),
                dataTypeMappings = dataTypeMappings.destination(PostgresDestination.BACKEND)
            )
            Mongo.isMongoUri(uri) -> MongoDestination(
                uri,
                dataTypeMappings = dataTypeMappings.destination(MongoDestination.BACKEND)
            )
            Redis.isRedisUri(uri) -> RedisDestination(
                uri,
                requireRedisMappings(uri, options)
            )
            else -> error("Unknown destination database: $uri")
        }
    }

    private fun requireRedisMappings(uri: String, options: Map<String, String>): String =
        checkNotNull(options[MAPPING_OPTION]) {
            "Redis adapter for '$uri' requires additional option '$MAPPING_OPTION'"
        }
}
