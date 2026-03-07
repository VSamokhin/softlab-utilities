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

package org.softlab.datatransfer

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.help
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.softlab.datatransfer.adapters.mongo.MongoDestination
import org.softlab.datatransfer.adapters.mongo.MongoSource
import org.softlab.datatransfer.adapters.postgres.PostgresDestination
import org.softlab.datatransfer.adapters.postgres.PostgresSource
import org.softlab.datatransfer.config.ConfigProvider
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.core.StringTokenFilter
import org.softlab.datatransfer.migration.Migrator
import org.softlab.datatransfer.util.Mongo
import org.softlab.datatransfer.util.Postgres
import org.softlab.datatransfer.util.StopWatch


class Main : CliktCommand() {
    private val logger = KotlinLogging.logger {}

    private val source by argument()
        .help("Source DB connection string (URL)")
    private val dest by argument()
        .help("Destination DB connection string (URL)")
    private val dropTargets by option()
        .flag(default = false)
        .help("Drop target collections/tables before migration")
    private val sourceFilter by option()
        .help("Source name prefixes or a file containing one prefix per line")
        .multiple()

    override fun run() {
        val dataTypeMappings = ConfigProvider.config.getDataTypeMappings()
        val sourceNameFilter = StringTokenFilter.from(sourceFilter)
        val sourceAdapter: DatabaseSource = when {
            Postgres.isPostgresUri(source) -> PostgresSource(source)
            Mongo.isMongoUri(source) -> MongoSource(source)
            else -> error("Unsupported source database")
        }

        val destAdapter: DatabaseDestination = when {
            Postgres.isPostgresUri(dest) -> PostgresDestination(
                dest,
                dataTypeMappings = dataTypeMappings.destination(PostgresDestination.BACKEND)
            )
            Mongo.isMongoUri(dest) -> MongoDestination(
                dest,
                dataTypeMappings = dataTypeMappings.destination(MongoDestination.BACKEND)
            )
            else -> error("Unsupported destination type")
        }

        val timer = StopWatch().start()
        try {
            runBlocking {
                if (dropTargets) {
                    logger.info { "First drop target collections" }
                    sourceAdapter.listCollections()
                        .map { it.fetchMetadata().name }
                        .filter { sourceNameFilter.startsWith(it) }
                        .collect { destAdapter.dropCollection(it) }
                }
                logger.info { "Now migrate data" }
                Migrator(sourceFilter = sourceNameFilter).migrate(sourceAdapter, destAdapter)
            }
        } finally {
            sourceAdapter.close()
            destAdapter.close()
        }
        logger.info { "Job done in ${timer.stopAndGetTimeStr()}" }
    }
}

fun main(args: Array<String>) = Main().main(args)
