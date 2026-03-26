package org.softlab.datataset.test.initiators

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertEquals
import kotlin.test.assertTrue


@Testcontainers
class MongoInitiatorIT {
    companion object {
        private const val DATABASE = "initiator_test_db"
        private const val CHANGELOG = "liquibase/changelog-mongo.yaml"
        private const val DATASET = "datasets/test-dataset.yml"

        @Container
        @JvmStatic
        private val mongo = createMongoContainer()
    }

    @Test
    fun `initSchema() and seedData() prepare mongo database`() {
        MongoInitiator("${mongo.connectionString}/$DATABASE").use { cut ->
            cut.cleanup()

            var callbackInvoked = false
            cut.initSchema(CHANGELOG) { database ->
                callbackInvoked = true
                val collectionNames = runBlocking { database.listCollectionNames().toList() }
                assertTrue("test.test_table" in collectionNames)
            }
            cut.seedData(DATASET)

            val docs = runBlocking {
                cut.mongoDb.getCollection<Document>("test.test_table").find().toList()
            }
            assertEquals(2, docs.size)
            assertTrue(callbackInvoked)
        }
    }

    @Test
    fun `cleanup() drops mongo database and invokes callback`() {
        MongoInitiator("${mongo.connectionString}/$DATABASE").use { cut ->
            cut.cleanup()

            cut.initSchema(CHANGELOG)
            cut.seedData(DATASET)

            var callbackInvoked = false
            cut.cleanup { database ->
                callbackInvoked = true
                val collectionNames = runBlocking { database.listCollectionNames().toList() }
                assertEquals(emptyList(), collectionNames)
            }

            assertTrue(callbackInvoked)
        }
    }
}
