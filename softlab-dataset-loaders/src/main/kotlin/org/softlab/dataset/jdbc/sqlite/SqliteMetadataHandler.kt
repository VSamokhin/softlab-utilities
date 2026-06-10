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

package org.softlab.dataset.jdbc.sqlite

import org.dbunit.database.DefaultMetadataHandler
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.Types
import javax.sql.RowSetMetaData
import javax.sql.rowset.RowSetMetaDataImpl
import javax.sql.rowset.RowSetProvider


class SqliteMetadataHandler(
    private val schemaAlias: String
) : DefaultMetadataHandler() {
    override fun getColumns(databaseMetaData: DatabaseMetaData, schemaName: String?, tableName: String?): ResultSet =
        super.getColumns(databaseMetaData, sqliteSchema(schemaName), sqliteTable(tableName))

    @Suppress("NestedBlockDepth")
    override fun getTables(
        metaData: DatabaseMetaData,
        schemaName: String?,
        tableTypes: Array<out String>?
    ): ResultSet {
        val connection = metaData.connection

        val rowSet = RowSetProvider.newFactory().createCachedRowSet()
        rowSet.setMetaData(tableMetadata())

        val tableType = "TABLE"
        if (tableType !in (tableTypes?.toSet() ?: setOf(tableType))) return rowSet

        connection.createStatement().use { stmt ->
            connection.sqliteListDbs()
                .filterKeys { it != "temp" }
                .filterKeys { schemaName == null || schemaName == it }
                .forEach { db ->
                    val dbName = db.key
                    val schema = if (dbName == "main") null else dbName

                    stmt.executeQuery(
                        """
                        SELECT tbl_name
                        FROM ${quoteSqliteIdentifier(dbName)}.sqlite_schema
                        WHERE type = 'table'
                        ORDER BY tbl_name
                        """.trimIndent()
                    ).use { rs ->
                        while (rs.next()) {
                            rowSet.moveToInsertRow()
                            rowSet.updateString("TABLE_CAT", null)
                            rowSet.updateString("TABLE_SCHEM", schema)
                            rowSet.updateString("TABLE_NAME", rs.getString("tbl_name"))
                            rowSet.updateString("TABLE_TYPE", tableType)
                            rowSet.updateString("REMARKS", null)
                            rowSet.updateString("TYPE_CAT", null)
                            rowSet.updateString("TYPE_SCHEM", null)
                            rowSet.updateString("TYPE_NAME", null)
                            rowSet.updateString("SELF_REFERENCING_COL_NAME", null)
                            rowSet.updateString("REF_GENERATION", null)
                            rowSet.insertRow()
                            rowSet.moveToCurrentRow()
                        }
                    }
                }
        }

        rowSet.beforeFirst()
        return rowSet
    }

    override fun tableExists(
        metaData: DatabaseMetaData,
        schema: String?,
        tableName: String
    ): Boolean {
        getTables(metaData, schema, arrayOf("TABLE")).use { rs ->
            while (rs.next()) {
                if (rs.getString("TABLE_NAME") == tableName) {
                    return true
                }
            }
        }
        return false
    }

    @Suppress("MagicNumber")
    private fun tableMetadata(): RowSetMetaData {
        return RowSetMetaDataImpl().apply {
            columnCount = 10

            setVarcharColumn(1, "TABLE_CAT")
            setVarcharColumn(2, "TABLE_SCHEM")
            setVarcharColumn(3, "TABLE_NAME")
            setVarcharColumn(4, "TABLE_TYPE")
            setVarcharColumn(5, "REMARKS")
            setVarcharColumn(6, "TYPE_CAT")
            setVarcharColumn(7, "TYPE_SCHEM")
            setVarcharColumn(8, "TYPE_NAME")
            setVarcharColumn(9, "SELF_REFERENCING_COL_NAME")
            setVarcharColumn(10, "REF_GENERATION")
        }
    }

    private fun RowSetMetaDataImpl.setVarcharColumn(index: Int, name: String) {
        setColumnName(index, name)
        setColumnLabel(index, name)
        setColumnType(index, Types.VARCHAR)
        setNullable(index, ResultSetMetaData.columnNullable)
    }

    private fun quoteSqliteIdentifier(identifier: String): String {
        return "\"" + identifier.replace("\"", "\"\"") + "\""
    }

    override fun getPrimaryKeys(
        databaseMetaData: DatabaseMetaData,
        schemaName: String?,
        tableName: String?
    ): ResultSet =
        super.getPrimaryKeys(databaseMetaData, sqliteSchema(schemaName), sqliteTable(tableName))

    override fun getSchema(resultSet: ResultSet): String = schemaAlias

    override fun matches(
        columnsResultSet: ResultSet,
        catalog: String?,
        schema: String?,
        table: String?,
        column: String?,
        caseSensitive: Boolean
    ): Boolean =
        super.matches(
            columnsResultSet,
            catalog,
            sqliteSchema(schema),
            sqliteTable(table),
            column,
            caseSensitive
        )

    private fun sqliteSchema(schemaName: String?): String? =
        if (schemaName == schemaAlias) null else schemaName

    private fun sqliteTable(tableName: String?): String? =
        tableName?.substringAfter("$schemaAlias.")
}
