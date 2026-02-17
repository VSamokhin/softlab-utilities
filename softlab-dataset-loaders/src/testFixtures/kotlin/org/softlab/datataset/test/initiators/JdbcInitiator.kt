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

package org.softlab.datataset.test.initiators

import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.resource.ClassLoaderResourceAccessor
import org.softlab.dataset.jdbc.JdbcDatasetLoader
import java.sql.Connection
import java.sql.DriverManager


class JdbcInitiator(
    override val dbUrl: String,
    private val username: String,
    private val password: String
) : DatabaseInitiator<Connection> {

    private val lazyConnection = lazy { getConnection() }
    private val conn: Connection by lazyConnection

    override fun cleanup(additionalSteps: (Connection) -> Unit) {
        DatabaseFactory.getInstance()
            .openDatabase(
                dbUrl,
                username,
                password,
                null,
                null
            ).use { database ->
                database.dropDatabaseObjects(database.defaultSchema)
            }

        additionalSteps(conn)
    }

    override fun initSchema(changelogPath: String, additionalSteps: (Connection) -> Unit) {
        val database = DatabaseFactory.getInstance()
            .openDatabase(
                dbUrl,
                username,
                password,
                null,
                null
            )
        Liquibase(
            changelogPath,
            ClassLoaderResourceAccessor(),
            database
        ).use { liquibase -> liquibase.update() }

        additionalSteps(conn)
    }

    override fun seedData(datasetPath: String) {
        JdbcDatasetLoader(conn).load(datasetPath)
    }

    override fun close() {
        if (lazyConnection.isInitialized()) conn.close()
    }

    fun getConnection(): Connection =
        DriverManager.getConnection(dbUrl, username, password)
}
