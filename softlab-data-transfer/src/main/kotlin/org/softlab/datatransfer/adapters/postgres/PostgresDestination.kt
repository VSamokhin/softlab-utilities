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

package org.softlab.datatransfer.adapters.postgres

import org.softlab.dataset.jdbc.withStatement
import org.softlab.datatransfer.adapters.jdbc.JdbcDestination
import org.softlab.datatransfer.config.DataTypeMappings
import org.softlab.datatransfer.config.DataTypeMappingsConfig
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.IndexDefinition
import org.softlab.datatransfer.core.PrimaryKeyDefinition
import org.softlab.datatransfer.core.SqlTableName
import org.softlab.datatransfer.core.toSql
import org.softlab.datatransfer.core.toSqlIdentifier


class PostgresDestination(
    pool: PostgresConnectionPool,
    dialect: PostgresDialect,
    dataTypeMappings: DataTypeMappings,
    closePool: Boolean
) : JdbcDestination(
    pool = pool,
    dialect = dialect,
    dataTypeMappings = dataTypeMappings,
    closePool = closePool
) {
    constructor(
        pool: PostgresConnectionPool,
        typeMappingsConfig: DataTypeMappingsConfig,
        closePool: Boolean = true
    ) : this(pool, PostgresDialect(), typeMappingsConfig, closePool)

    private constructor(
        pool: PostgresConnectionPool,
        dialect: PostgresDialect,
        typeMappingsConfig: DataTypeMappingsConfig,
        closePool: Boolean = true
    ) : this(pool, PostgresDialect(), typeMappingsConfig.destination(dialect.backendName), closePool)

    override fun castValue(type: String, value: Any?): Any? =
        if (type.isBoolean()) {
            when (value) {
                is Byte -> value.toLong() != 0L
                is Short -> value.toLong() != 0L
                is Int -> value.toLong() != 0L
                is Long -> value != 0L
                else -> value
            }
        } else {
            value
        }

    override suspend fun createIndexesAndConstraints(metadata: CollectionMetadata) {
        val tableName = dialect.parseTableName(metadata.name)
        val statements =
            listOfNotNull(metadata.primaryKey?.let { createPrimaryKeySql(tableName, it) }) +
                metadata.indices.map { createIndexSql(tableName, it) }

        pool.withStatement { stmt ->
            statements.forEach(stmt::execute)
        }
    }

    private fun String.isBoolean(): Boolean = this in setOf("boolean", "bool")

    private fun createPrimaryKeySql(tableName: SqlTableName, primaryKey: PrimaryKeyDefinition): String {
        val name = primaryKey.name ?: "${tableName.table}_pkey"
        val fields = primaryKey.fields.joinToString(", ") { it.toSqlIdentifier() }
        return "ALTER TABLE ${tableName.toSql()} " +
            "ADD CONSTRAINT ${name.toSqlIdentifier()} PRIMARY KEY ($fields);"
    }

    private fun createIndexSql(tableName: SqlTableName, index: IndexDefinition): String {
        val name = index.name ?: fallbackIndexName(tableName, index)
        val unique = if (index.unique) "UNIQUE " else ""
        val fields = index.fields.joinToString(", ") { field ->
            "${field.name.toSqlIdentifier()} ${field.order.toSql()}"
        }
        return "CREATE ${unique}INDEX ${name.toSqlIdentifier()} ON ${tableName.toSql()} ($fields);"
    }

    private fun fallbackIndexName(tableName: SqlTableName, index: IndexDefinition): String =
        "${tableName.table}_${index.fields.joinToString("_") { it.name }}_idx"
}
