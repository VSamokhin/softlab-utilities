package org.softlab.datatransfer.util

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class MongoTest {
    companion object {
        @JvmStatic
        fun getDatabaseNameArgs(): List<Arguments> = listOf(
            Arguments.of("testdb", "mongodb://localhost:27017/testdb"),
            Arguments.of("sample", "mongodb://user:pass@localhost:27017/sample"),
            Arguments.of("analytics", "mongodb://localhost:27017/analytics?retryWrites=true&w=majority"),
            Arguments.of("clusterdb", "mongodb+srv://cluster0.example.mongodb.net/clusterdb?retryWrites=true"),
            Arguments.of("", "mongodb://localhost:27017/")
        )
    }

    @ParameterizedTest
    @ValueSource(strings = [ "mongodb://localhost:27017/testdb", "mongodb+srv://cluster0.example.mongodb.net/testdb" ])
    fun `isMongoUri() returns true for mongodb URI`(uri: String) {
        assertTrue(Mongo.isMongoUri(uri))
    }

    @ParameterizedTest
    @ValueSource(strings = [ "jdbc:postgresql://localhost:5432/testdb", "http://localhost/testdb" ])
    fun `isMongoUri() returns false for postgres URI`(uri: String) {
        assertFalse(Mongo.isMongoUri(uri))
    }

    @ParameterizedTest
    @MethodSource("getDatabaseNameArgs")
    fun `getDatabaseName() extracts expected database name`(expected: String, uri: String) {
        assertEquals(expected, Mongo.getDatabaseName(uri))
    }
}
