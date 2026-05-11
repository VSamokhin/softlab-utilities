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

package org.softlab.datatransfer.adapters.sqlite

import org.softlab.datatransfer.adapters.jdbc.JdbcDestination
import org.softlab.datatransfer.config.DataTypeMappingsConfig


class SqliteDestination(
    pool: SqliteConnectionPool,
    dialect: SqliteDialect,
    dataTypeMappings: Map<String, String>,
    closePool: Boolean
) : JdbcDestination(
    pool = pool,
    dialect = dialect,
    dataTypeMappings = dataTypeMappings,
    closePool = closePool
) {
    constructor(
        pool: SqliteConnectionPool,
        typeMappingsConfig: DataTypeMappingsConfig,
        closePool: Boolean = true
    ) : this(pool, SqliteDialect(), typeMappingsConfig, closePool)

    private constructor(
        pool: SqliteConnectionPool,
        dialect: SqliteDialect,
        typeMappingsConfig: DataTypeMappingsConfig,
        closePool: Boolean = true
    ) : this(pool, SqliteDialect(), typeMappingsConfig.destination(dialect.backendName), closePool)
}
