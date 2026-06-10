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
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.softlab.datatransfer.adapters.AdapterProvider
import org.softlab.datatransfer.adapters.KeyValues
import org.softlab.datatransfer.core.StringTokenFilter
import org.softlab.datatransfer.migration.Migrator
import org.softlab.datatransfer.util.StopWatch


class Main : CliktCommand() {
    companion object {
        private const val MIN_DEFAULT_JOBS = 1
        private const val MAX_DEFAULT_JOBS = 8
    }
    private val logger = KotlinLogging.logger {}

    private val defaultJobs: Int = Runtime.getRuntime()
        .availableProcessors()
        .coerceAtLeast(MIN_DEFAULT_JOBS)
        .coerceAtMost(MAX_DEFAULT_JOBS)

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
    private val sourceOptions by option()
        .help("Additional source adapter options as comma-separated key=value pairs")
    private val destOptions by option()
        .help("Additional destination adapter options as comma-separated key=value pairs")
    private val jobs by option()
        .help("Hint of simultaneous jobs count used for migration (default: $defaultJobs)")
        .int()
        .default(defaultJobs)

    override fun run() {
        val sourceNameFilter = StringTokenFilter.from(sourceFilter)
        val parsedSourceOptions = KeyValues.parse(sourceOptions).apply {
            put(AdapterProvider.JOBS_HINT, jobs.toString())
        }
        val parsedDestOptions = KeyValues.parse(destOptions).apply {
            put(AdapterProvider.JOBS_HINT, jobs.toString())
        }

        logger.info { "Will be using $jobs threads for the data transfer if the database supports parallelism" }

        val dropTimer = StopWatch()
        val migrateTimer = StopWatch()
        AdapterProvider.sourceFor(source, parsedSourceOptions).use { sourceAdapter ->
            AdapterProvider.destinationFor(dest, parsedDestOptions).use { destAdapter ->
                runBlocking {
                    if (dropTargets) {
                        logger.info { "First drop target collections" }
                        dropTimer.start()
                        sourceAdapter.listCollections()
                            .map { it.fetchMetadata().name }
                            .filter { sourceNameFilter.startsWith(it) }
                            .collect { destAdapter.dropCollection(it) }
                        dropTimer.stop()
                    }
                    logger.info { "Now migrate data" }
                    migrateTimer.start()
                    Migrator(sourceNameFilter,jobs)
                        .migrate(sourceAdapter, destAdapter)
                    migrateTimer.stop()
                }
            }
        }
        logger.info {
            "Job done in ${StopWatch.formatTime(dropTimer.getStopTime() + migrateTimer.getStopTime())}, " +
                "drop took ${dropTimer.getStopTimeStr()}, migration took ${migrateTimer.getStopTimeStr()}"
        }
    }
}

fun main(args: Array<String>) = Main().main(args)
