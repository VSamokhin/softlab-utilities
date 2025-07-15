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

package org.softlab.dataset.jdbc

import com.github.database.rider.core.api.dataset.SeedStrategy
import com.github.database.rider.core.configuration.DBUnitConfig
import com.github.database.rider.core.configuration.DataSetConfig
import com.github.database.rider.core.connection.ConnectionHolderImpl
import com.github.database.rider.core.dataset.DataSetExecutorImpl
import io.github.oshai.kotlinlogging.KotlinLogging
import org.softlab.dataset.DatasetLoader
import java.sql.Connection


/**
 * This is a wrapper which allows to load DBUnit datasets with DBRider in environments where standard DBRider
 * annotations will not work, e.g. while using SpringBoot + Cucumber
 */
class JdbcDatasetLoader(connection: Connection) : DatasetLoader {
    private val logger = KotlinLogging.logger {}

    private val executor = DataSetExecutorImpl.instance(ConnectionHolderImpl(connection)).apply {
        dbUnitConfig = DBUnitConfig()
            .cacheConnection(false)
            .addDBUnitProperty("caseSensitiveTableNames", true)
            .addDBUnitProperty("qualifiedTableNames", true)
    }

    override fun load(datasetPath: String, cleanBefore: Boolean) {
        logger.info { "Loading dataset with DBRider: (cleanBefore=$cleanBefore) $datasetPath" }

        val datasetConfig = DataSetConfig(datasetPath)
            .strategy(SeedStrategy.INSERT) // Do not clean transparently, better fail
            .cleanBefore(cleanBefore)
        executor.createDataSet(datasetConfig)
    }

    fun compare(datasetPath: String, vararg ignoreCols: String) {
        logger.info { "Checking expected dataset with DBRider: (ignoreCols=$ignoreCols) $datasetPath" }

        val expectedDatasetConfig = DataSetConfig(datasetPath)
        executor.compareCurrentDataSetWith(expectedDatasetConfig, ignoreCols)
    }
}
