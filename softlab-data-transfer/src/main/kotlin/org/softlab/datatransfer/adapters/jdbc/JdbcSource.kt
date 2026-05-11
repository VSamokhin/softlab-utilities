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

package org.softlab.datatransfer.adapters.jdbc

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.softlab.dataset.jdbc.JdbcConnectionProvider
import org.softlab.dataset.jdbc.withConnection
import org.softlab.dataset.jdbc.withStatement
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.core.DocumentCollection


open class JdbcSource(
    private val pool: JdbcConnectionProvider,
    private val dialect: JdbcDialect,
    private val closePool: Boolean,
    private val collectionFactory: (JdbcTableName) -> DocumentCollection =
        { tableName -> JdbcDocumentCollection(tableName, pool, dialect) }
) : DatabaseSource {
    override fun getBackendName(): String = dialect.backendName

    override fun listCollections(): Flow<DocumentCollection> = flow {
        pool.withConnection { connection ->
            dialect.listTables(connection).forEach { table ->
                emit(collectionFactory(table))
            }
        }
    }

    override suspend fun countDocuments(collectionName: String): Long {
        val tableName = dialect.parseTableName(collectionName)
        pool.withStatement { stmt ->
            stmt.executeQuery("SELECT COUNT(*) FROM $tableName;").use { rs ->
                return if (rs.next()) rs.getLong(1) else error("Could not count rows in $tableName")
            }
        }
    }

    override fun close() {
        if (closePool) pool.close()
    }
}
