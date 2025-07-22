package org.softlab.dataset.mongo

import com.mongodb.kotlin.client.coroutine.MongoClient
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase
import liquibase.resource.ClassLoaderResourceAccessor
import org.bson.Document
import org.bson.types.Binary
import org.hamcrest.CoreMatchers.allOf
import org.hamcrest.CoreMatchers.containsString
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.mongo.coroutine.CoroutineMongoDatabase
import org.testcontainers.containers.MongoDBContainer
import java.time.ZonedDateTime
import java.util.Date


class MongoYamlDatasetLoaderTest {
    companion object {
        private const val DATABASE: String = "testdb"

        val mongoContainer: MongoDBContainer = MongoDBContainer("mongo:latest")

        private lateinit var mongoClient: MongoClient
        private lateinit var mongoDb: CoroutineMongoDatabase

        @BeforeAll
        @JvmStatic
        fun setup() {
            mongoContainer.start()

            val connectionString = "${mongoContainer.connectionString}/$DATABASE"
            mongoClient = MongoClient.create(connectionString)
            val database = runBlocking { mongoClient.getDatabase(DATABASE) }
            mongoDb = CoroutineMongoDatabase(database)

            val liquiDb = DatabaseFactory.getInstance()
                .openDatabase(
                    connectionString,
                    null,
                    null,
                    null,
                    null
                ) as MongoLiquibaseDatabase
            val liquibase = Liquibase(
                "liquibase/changelog-mongo.yaml",
                ClassLoaderResourceAccessor(),
                liquiDb
            )
            liquibase.update()
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            mongoClient.close()
            mongoContainer.stop()
        }
    }

    @Test
    fun `load() loads dataset correctly`() {
        val cut = MongoYamlDatasetLoader(mongoDb)
        cut.load("datasets/test-dataset.yml")

        val db = mongoClient.getDatabase(DATABASE)
        val testTable = db.getCollection<Document>("test.test_table")
        val docs = runBlocking { testTable.find().toList() }

        assertEquals(2, docs.size)

        val alice = docs.single { it["text_column"].toString() == "Alice" }
        assertEquals(8, alice.size) // 7 fields + _id
        assertEquals(1, alice["int_column_pk"])
        assertEquals(123.456789, alice["double_column"])
        val bin = alice["bin_column"] as Binary
        assertEquals("a small brown fox jumps over the lazy dog",  String(bin.data))
        assertEquals(true, alice["bool_column"])
        val date = alice["timestamp_column"] as Date
        assertEquals(
            ZonedDateTime.parse("2023-10-01T12:00:00Z").toInstant().toEpochMilli(),
            date.toInstant().toEpochMilli()
        )
        assertEquals(1234567890123456789, alice["long_column"])

        val bob = docs.single { it["text_column"].toString() == "Bob" }
        assertEquals(4, bob.size) // 3 fields + _id
        assertEquals(2, bob["int_column_pk"])
        assertEquals(true, bob["bool_column"])
    }

    @Test
    fun `load() should throw exception for non existent collection`() {
        val cut = MongoYamlDatasetLoader(mongoDb)
        val exception = assertThrows<IllegalStateException> {
            cut.load("datasets/test-dataset-mongo-collection-does-not-exist.yml")
        }
        assertThat(exception.message, containsString("test.non_existent_collection"))
    }

    @Test
    fun `load() should throw exception for non existent field`() {
        val cut = MongoYamlDatasetLoader(mongoDb)
        val exception = assertThrows<IllegalStateException> {
            cut.load("datasets/test-dataset-mongo-field-does-not-exist.yml")
        }
        assertThat(exception.message, containsString("non_existent_column"))
    }

    @Test
    fun `load() should throw exception for collection without validator`() {
        val cut = MongoYamlDatasetLoader(mongoDb)
        val exception = assertThrows<IllegalStateException> {
            cut.load("datasets/test-dataset-mongo-collection-without-validator.yml")
        }
        assertThat(
            exception.message, allOf(
            containsString("validator"),
            containsString("test.collection_without_validator"))
        )
    }

    @Test
    fun `load() should throw exception for collection with incorrect type`() {
        val cut = MongoYamlDatasetLoader(mongoDb)
        assertThrows<IllegalArgumentException> {
            cut.load("datasets/test-dataset-mongo-collection-with-incorrect-type.yml")
        }
    }

    @Test
    fun `load() should throw exception for collection with not supported type`() {
        val cut = MongoYamlDatasetLoader(mongoDb)
        val exception = assertThrows<IllegalStateException> {
            cut.load("datasets/test-dataset-mongo-collection-with-not-supported-type.yml")
        }
        assertThat(
            exception.message, allOf(
                containsString("decimal"),
                containsString("not_supported_type_column"))
        )
    }
}
