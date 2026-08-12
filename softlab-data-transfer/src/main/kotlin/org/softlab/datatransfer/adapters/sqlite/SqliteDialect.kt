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

import io.github.oshai.kotlinlogging.KotlinLogging
import org.softlab.dataset.core.FieldDefinition
import org.softlab.datatransfer.adapters.jdbc.JdbcDialect
import org.softlab.datatransfer.core.IndexDefinition
import org.softlab.datatransfer.core.IndexFieldDefinition
import org.softlab.datatransfer.core.IndexSortOrder
import org.softlab.datatransfer.core.SqlTableName
import org.softlab.datatransfer.core.toSqlIdentifier
import java.sql.Connection
import java.sql.ResultSet


class SqliteDialect : JdbcDialect(
    backendName = "sqlite",
    supportsConcurrentInserts = false
) {
    private val logger = KotlinLogging.logger {}

    override fun parseTableName(tableName: String): SqlTableName =
        SqlTableName.of(tableName).copy(schema = null)

    override fun listColumns(tableName: SqlTableName, connection: Connection): Map<String, FieldDefinition> {
        return super.listColumns(tableName, connection).mapValues { field ->
            // All integer types in SQLite are int8
            if (setOf("INTEGER", "BIGINT").contains(field.value.type)) {
                FieldDefinition(field.value.name, "INT8", field.value.nullable)
            } else field.value
        }
    }

    override fun readIndices(tableName: SqlTableName, connection: Connection): List<IndexDefinition> =
        readIndexMetadata(tableName, connection)
            .mapNotNull { metadata -> readIndex(tableName, connection, metadata) }
            .sortedBy { it.name }
            .map { index -> IndexDefinition(index.name, index.fields, index.unique) }

    private fun readIndexMetadata(tableName: SqlTableName, connection: Connection): List<SqliteIndexMetadata> =
        connection.createStatement().use { statement ->
            statement.executeQuery("PRAGMA index_list(${tableName.table.toSqlIdentifier()});").use { rs ->
                val indices = mutableListOf<SqliteIndexMetadata>()
                while (rs.next()) {
                    indices += SqliteIndexMetadata(
                        name = rs.getString("name"),
                        unique = rs.getInt("unique") == 1,
                        origin = rs.getString("origin"),
                        partial = rs.getInt("partial") == 1
                    )
                }
                indices
            }
        }

    private fun readIndex(
        tableName: SqlTableName,
        connection: Connection,
        metadata: SqliteIndexMetadata
    ): SqliteIndex? =
        when {
            metadata.origin != SQLITE_CREATED_INDEX_ORIGIN -> {
                logger.warn {
                    "Skipping SQLite implicit index '${metadata.name}' on table '$tableName' " +
                        "with origin '${metadata.origin}'"
                }
                null
            }
            metadata.partial -> {
                logger.warn { "Skipping SQLite partial index '${metadata.name}' on table '$tableName'" }
                null
            }
            else -> readIndexFields(tableName, connection, metadata.name)?.let { fields ->
                SqliteIndex(metadata.name, fields, metadata.unique)
            }
        }

    private fun readIndexFields(
        tableName: SqlTableName,
        connection: Connection,
        indexName: String
    ): List<IndexFieldDefinition>? {
        val fields = connection.createStatement().use { statement ->
            statement.executeQuery("PRAGMA index_xinfo(${indexName.toSqlIdentifier()});").use { rs ->
                readIndexFields(tableName, indexName, rs)
            }
        }
        return when {
            fields == null -> null
            fields.isEmpty() -> {
                logger.warn {
                    "Skipping SQLite index '$indexName' on table '$tableName' because it has no portable fields"
                }
                null
            }
            else -> fields.sortedBy { it.first }.map { it.second }
        }
    }

    private fun readIndexFields(
        tableName: SqlTableName,
        indexName: String,
        rs: ResultSet
    ): List<Pair<Int, IndexFieldDefinition>>? {
        val fields = mutableListOf<Pair<Int, IndexFieldDefinition>>()
        while (rs.next()) {
            if (rs.getInt("key") == 1) {
                val field = readIndexField(
                    tableName,
                    indexName,
                    rs.getInt("seqno"),
                    rs.getString("name"),
                    rs.getInt("desc")
                ) ?: return null
                fields += field
            }
        }
        return fields
    }

    private fun readIndexField(
        tableName: SqlTableName,
        indexName: String,
        position: Int,
        columnName: String?,
        descending: Int
    ): Pair<Int, IndexFieldDefinition>? =
        if (columnName.isNullOrBlank()) {
            logger.warn {
                "Skipping SQLite index '$indexName' on table '$tableName' because it has an " +
                    "unsupported expression column"
            }
            null
        } else {
            position to IndexFieldDefinition(
                name = columnName.lowercase(),
                order = if (descending == 1) IndexSortOrder.DESC else IndexSortOrder.ASC
            )
        }

    private data class SqliteIndexMetadata(
        val name: String,
        val unique: Boolean,
        val origin: String,
        val partial: Boolean
    )

    private data class SqliteIndex(
        val name: String,
        val fields: List<IndexFieldDefinition>,
        val unique: Boolean
    )

    companion object {
        private const val SQLITE_CREATED_INDEX_ORIGIN = "c"
    }
}
