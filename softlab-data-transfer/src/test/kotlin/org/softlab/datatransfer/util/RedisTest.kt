package org.softlab.datatransfer.util

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class RedisTest {
    @ParameterizedTest
    @ValueSource(strings = [ "redis://localhost:6379/testdb", "rediss://cluster0.example.mongodb.net",
        "redis-socket://user:pass@localhost:6379", "redis-sentinel://localhost.org :6379/testdb"])
    fun `isRedisUri() returns true for redis URI`(uri: String) {
        assertTrue(Redis.isRedisUri(uri))
    }

    @ParameterizedTest
    @ValueSource(strings = [ "jdbc:postgresql://localhost:5432/testdb", "http://localhost/testdb" ])
    fun `isRedisUri() returns false for other URIs`(uri: String) {
        assertFalse(Redis.isRedisUri(uri))
    }
}
