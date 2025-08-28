/**
 * Copyright (C) 2025, Viktor Samokhin (wowyupiyo@gmail.com)
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
import org.softlab.datatransfer.adapters.mongo.MongoDestination
import org.softlab.datatransfer.adapters.mongo.MongoSource
import org.softlab.datatransfer.adapters.postgres.PostgresDestination
import org.softlab.datatransfer.adapters.postgres.PostgresSource
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.migration.Migrator


class Main : CliktCommand() {
    private val source by argument(name = "--source", help = "Source DB connection string (URL)")
    private val dest by argument(name = "--dest", help = "Destination DB connection string (URL)")

    override fun run() {
        val sourceAdapter: DatabaseSource = when {
            source.startsWith("jdbc:postgres") -> PostgresSource(source)
            source.startsWith("mongodb") -> MongoSource(source)
            else -> error("Unsupported source type")
        }

        val destAdapter: DatabaseDestination = when {
            dest.startsWith("jdbc:postgres") -> PostgresDestination(dest)
            dest.startsWith("mongodb") -> MongoDestination(dest)
            else -> error("Unsupported destination type")
        }

        Migrator().migrate(sourceAdapter, destAdapter)

        sourceAdapter.close()
        destAdapter.close()
    }
}

fun main(args: Array<String>) = Main().main(args)
