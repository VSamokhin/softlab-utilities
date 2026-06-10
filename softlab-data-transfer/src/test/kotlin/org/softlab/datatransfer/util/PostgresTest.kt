package org.softlab.datatransfer.util

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class PostgresTest {
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
}
