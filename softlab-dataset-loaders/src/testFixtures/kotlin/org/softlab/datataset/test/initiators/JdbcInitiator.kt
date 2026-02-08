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
) : DatabaseInitiator {
    private val lazyDatabase = lazy {
        DatabaseFactory.getInstance()
            .openDatabase(
                dbUrl,
                username,
                password,
                null,
                null
            )
    }
    private val database by lazyDatabase

    private val lazySeedConnection = lazy { getConnection() }
    private val seedConnection by lazySeedConnection

    override fun initSchema(changelog: String) {
        Liquibase(
            changelog,
            ClassLoaderResourceAccessor(),
            database
        ).use { liquibase -> liquibase.update() }
    }

    override fun seedData(dataset: String) {
        JdbcDatasetLoader(seedConnection).load(dataset, true)
    }

    override fun close() {
        if (lazyDatabase.isInitialized()) database.close()
        if (lazySeedConnection.isInitialized()) seedConnection.close()
    }

    fun getConnection(): Connection =
        DriverManager.getConnection(dbUrl, username, password)
}
