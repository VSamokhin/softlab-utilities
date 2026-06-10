package org.softlab.datatransfer.adapters.redis

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.allOf
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.core.FieldDefinition
import org.softlab.dataset.redis.RedisHashMapping
import org.softlab.dataset.redis.RedisSetMapping
import org.softlab.dataset.redis.RedisTableMapping
import org.softlab.dataset.redis.RedisTableMappings
import org.softlab.datatransfer.core.CollectionMetadata


class RedisMappingValidatorTest {
    @Test
    fun `validateSourceMappings() validates every table mapping`() {
        val exc = assertThrows<IllegalArgumentException> {
            RedisMappingValidator.validateSourceMappings(
                RedisTableMappings(
                    tables = listOf(
                        sourceTable("users"),
                        RedisTableMapping(
                            table = "orders",
                            fields = listOf(FieldDefinition("id", "integer"))
                        )
                    )
                )
            )
        }

        assertThat(exc.message, containsString("orders"))
    }

    @Test
    fun `validateSourceMapping() rejects hash field mapping without hash value mapping`() {
        val exc = assertThrows<IllegalStateException> {
            RedisMappingValidator.validateSourceMapping(
                RedisTableMapping(
                    table = "users",
                    fields = listOf(
                        FieldDefinition("id", "integer"),
                        FieldDefinition("status", "text")
                    ),
                    hashes = listOf(
                        RedisHashMapping(key = "users:\${id}"),
                        RedisHashMapping(key = "users:\${id}:status", field = "\${status}")
                    )
                )
            )
        }

        assertThat(exc.message, allOf(
            containsString("users"),
            containsString("hash.field"),
            containsString("hash.value")
        ))
    }

    @Test
    fun `validateDestinationMapping() allows empty source metadata`() {
        assertDoesNotThrow {
            RedisMappingValidator.validateDestinationMapping(
                RedisTableMapping(
                    table = "users",
                    hashes = listOf(RedisHashMapping(key = "users:\${missing}")),
                    sets = listOf(RedisSetMapping(key = "users:\${missing}", member = "\${missing}"))
                ),
                CollectionMetadata("users", emptyMap())
            )
        }
    }

    @Test
    fun `validateDestinationMapping() allows known fields in hashes and sets`() {
        assertDoesNotThrow {
            RedisMappingValidator.validateDestinationMapping(
                RedisTableMapping(
                    table = "users",
                    hashes = listOf(
                        RedisHashMapping(
                            key = "users:\${id}",
                            field = "profile:\${name}",
                            value = "\${status}"
                        )
                    ),
                    sets = listOf(RedisSetMapping(key = "active:\${status}", member = "\${id}"))
                ),
                usersMetadata()
            )
        }
    }

    @Test
    fun `validateDestinationMapping() rejects unknown hash key fields`() {
        val exc = assertThrows<IllegalStateException> {
            RedisMappingValidator.validateDestinationMapping(
                RedisTableMapping(
                    table = "users",
                    hashes = listOf(RedisHashMapping(key = "users:\${missing_hash_key}"))
                ),
                usersMetadata()
            )
        }

        assertThat(exc.message, allOf(
            containsString("users"),
            containsString("missing_hash_key")
        ))
    }

    @Test
    fun `validateDestinationMapping() rejects unknown hash field fields`() {
        val exc = assertThrows<IllegalStateException> {
            RedisMappingValidator.validateDestinationMapping(
                RedisTableMapping(
                    table = "users",
                    hashes = listOf(RedisHashMapping(key = "users:\${id}", field = "\${missing_hash_field}"))
                ),
                usersMetadata()
            )
        }

        assertThat(exc.message, allOf(
            containsString("users"),
            containsString("missing_hash_field")
        ))
    }

    @Test
    fun `validateDestinationMapping() rejects unknown hash value fields`() {
        val exc = assertThrows<IllegalStateException> {
            RedisMappingValidator.validateDestinationMapping(
                RedisTableMapping(
                    table = "users",
                    hashes = listOf(RedisHashMapping(key = "users:\${id}", value = "\${missing_hash_value}"))
                ),
                usersMetadata()
            )
        }

        assertThat(exc.message, allOf(
            containsString("users"),
            containsString("missing_hash_value")
        ))
    }

    @Test
    fun `validateDestinationMapping() rejects unknown set key fields`() {
        val exc = assertThrows<IllegalStateException> {
            RedisMappingValidator.validateDestinationMapping(
                RedisTableMapping(
                    table = "users",
                    sets = listOf(RedisSetMapping(key = "users:\${missing_set_key}"))
                ),
                usersMetadata()
            )
        }

        assertThat(exc.message, allOf(
            containsString("users"),
            containsString("missing_set_key")
        ))
    }

    @Test
    fun `validateDestinationMapping() rejects unknown set member fields`() {
        val exc = assertThrows<IllegalStateException> {
            RedisMappingValidator.validateDestinationMapping(
                RedisTableMapping(
                    table = "users",
                    sets = listOf(RedisSetMapping(key = "users", member = "\${missing_set_member}"))
                ),
                usersMetadata()
            )
        }

        assertThat(exc.message, allOf(
            containsString("users"),
            containsString("missing_set_member")
        ))
    }

    private fun sourceTable(table: String): RedisTableMapping =
        RedisTableMapping(
            table = table,
            fields = listOf(FieldDefinition("id", "integer")),
            hashes = listOf(RedisHashMapping(key = "$table:\${id}"))
        )

    private fun usersMetadata(): CollectionMetadata =
        CollectionMetadata(
            "users",
            mapOf(
                "id" to FieldDefinition("id", "integer"),
                "name" to FieldDefinition("name", "text"),
                "status" to FieldDefinition("status", "text")
            )
        )
}
