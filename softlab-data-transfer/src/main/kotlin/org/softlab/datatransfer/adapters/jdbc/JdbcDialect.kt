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

import org.softlab.dataset.core.FieldDefinition
import org.softlab.datatransfer.core.IndexDefinition
import org.softlab.datatransfer.core.IndexFieldDefinition
import org.softlab.datatransfer.core.IndexSortOrder
import org.softlab.datatransfer.core.PrimaryKeyDefinition
import org.softlab.datatransfer.core.SqlTableName
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet


abstract class JdbcDialect(
    val backendName: String,
    val supportsConcurrentInserts: Boolean
) {
    abstract fun parseTableName(tableName: String): SqlTableName

    open fun listTables(connection: Connection): List<SqlTableName> {
        val results = mutableListOf<SqlTableName>()
        connection.metaData
            .getTables(null, null, "%", arrayOf("TABLE")).use { rs ->
                while (rs.next()) {
                    results +=
                        SqlTableName(
                            schema = rs.getString("TABLE_SCHEM"),
                            table = rs.getString("TABLE_NAME")
                        )
                }
            }
        return results
    }

    open fun listColumns(tableName: SqlTableName, connection: Connection): Map<String, FieldDefinition> {
        val results = mutableMapOf<String, FieldDefinition>()
        connection.metaData
            .getColumns(null, tableName.schema, tableName.table, "%")
            .use { rs ->
                while (rs.next()) {
                    val columnName = rs.getString("COLUMN_NAME")
                    results +=
                        columnName.lowercase() to FieldDefinition(
                            name = columnName,
                            type = rs.getString("TYPE_NAME"),
                            nullable = rs.getInt("NULLABLE") != java.sql.DatabaseMetaData.columnNoNulls
                        )
                }
            }
        return results
    }

    open fun tableExists(tableName: SqlTableName, connection: Connection): Boolean =
        connection.metaData
            .getTables(null, tableName.schema, tableName.table, arrayOf("TABLE"))
            .use { rs -> rs.next() }

    open fun readPrimaryKey(tableName: SqlTableName, connection: Connection): PrimaryKeyDefinition? {
        val fields = mutableListOf<Pair<Int, String>>()
        var name: String? = null
        connection.metaData
            .getPrimaryKeys(null, tableName.schema, tableName.table)
            .use { rs ->
                while (rs.next()) {
                    name = rs.getString("PK_NAME")
                    fields += rs.getShort("KEY_SEQ").toInt() to rs.getString("COLUMN_NAME").lowercase()
                }
            }
        return if (fields.isEmpty()) {
            null
        } else {
            PrimaryKeyDefinition(name, fields.sortedBy { it.first }.map { it.second })
        }
    }

    open fun readIndices(tableName: SqlTableName, connection: Connection): List<IndexDefinition> {
        val primaryKey = readPrimaryKey(tableName, connection)
        val indices = mutableListOf<JdbcIndexColumn>()
        connection.metaData
            .getIndexInfo(null, tableName.schema, tableName.table, false, false)
            .use { rs ->
                while (rs.next()) {
                    readIndexColumn(rs, primaryKey)?.let(indices::add)
                }
            }
        return indices
            .groupBy { it.indexName }
            .toSortedMap()
            .map { (indexName, columns) ->
                IndexDefinition(
                    name = indexName,
                    fields = columns
                        .sortedBy { it.columnPosition }
                        .map { IndexFieldDefinition(it.columnName, it.order) },
                    unique = columns.first().unique
                )
            }
            .filterNot { it.isPrimaryKeyBackingIndex(primaryKey) }
    }

    open fun beforeCreateTable(tableName: SqlTableName, connection: Connection) {
        // Default dialects do not need schema setup.
    }

    private fun readIndexColumn(rs: ResultSet, primaryKey: PrimaryKeyDefinition?): JdbcIndexColumn? {
        val indexName = rs.getString("INDEX_NAME")
        val columnName = rs.getString("COLUMN_NAME")
        return if (isSupportedIndexColumn(rs, indexName, columnName, primaryKey)) {
            JdbcIndexColumn(
                indexName = indexName!!,
                columnPosition = rs.getShort("ORDINAL_POSITION").toInt(),
                columnName = columnName!!.lowercase(),
                order = if (rs.getString("ASC_OR_DESC") == "D") IndexSortOrder.DESC else IndexSortOrder.ASC,
                unique = !rs.getBoolean("NON_UNIQUE")
            )
        } else {
            null
        }
    }

    private fun isSupportedIndexColumn(
        rs: ResultSet,
        indexName: String?,
        columnName: String?,
        primaryKey: PrimaryKeyDefinition?
    ): Boolean =
        !indexName.isNullOrBlank() &&
            !columnName.isNullOrBlank() &&
            indexName != primaryKey?.name &&
            rs.getShort("TYPE").toInt() != DatabaseMetaData.tableIndexStatistic.toInt()

    private fun IndexDefinition.isPrimaryKeyBackingIndex(primaryKey: PrimaryKeyDefinition?): Boolean =
        primaryKey != null &&
            primaryKey.name == null &&
            unique &&
            fields.map { it.name } == primaryKey.fields

    private data class JdbcIndexColumn(
        val indexName: String,
        val columnPosition: Int,
        val columnName: String,
        val order: IndexSortOrder,
        val unique: Boolean
    )
}
