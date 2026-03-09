package org.softlab.datatransfer.util

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class PostgresTest {
    companion object {
        @JvmStatic
        fun getSchemaNames(): List<Arguments> = listOf(
            Arguments.of("schema1.users", "schema1", "users"),
            Arguments.of("users", "public", "users"),
            Arguments.of("schema2.table.table", "schema2", "table.table")
        )
    }

    @ParameterizedTest
    @ValueSource(strings = [ "jdbc:postgresql://localhost:5432/testdb", "jdbc:postgres://localhost:5432/testdb" ])
    fun `isPostgresUri() returns true for postgresql URI`(uri: String) {
        assertTrue(Postgres.isPostgresUri(uri))
    }

    @ParameterizedTest
    @ValueSource(strings = [ "mongodb://localhost:27017/testdb", "http://localhost/testdb" ])
    fun `isPostgresUri() returns false for mongo URI`(uri: String) {
        assertFalse(Postgres.isPostgresUri(uri))
    }

    @ParameterizedTest
    @MethodSource("getSchemaNames")
    fun `getSchemaTable() returns expected schema and table`(
        collectionName: String,
        schemaName: String,
        tableName: String
    ) {
        val schemaTable = Postgres.getSchemaTable(collectionName)
        assertEquals(schemaName, schemaTable.first)
        assertEquals(tableName, schemaTable.second)
    }
}
