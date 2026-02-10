package org.softlab.datatransfer.migration

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.bson.BsonDocument
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.softlab.datataset.test.initiators.MongoInitiator
import org.softlab.datatransfer.adapters.mongo.MongoDestination
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.core.DocumentCollection
import org.softlab.datatransfer.core.FieldMetadata
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer


@Testcontainers
class MigratorTest {
    companion object {
        private const val DATABASE = "testdb"

        private const val NUM_RECORDS = 100_000

        @Container
        @JvmStatic
        private val mongoContainer = MongoDBContainer("mongo:latest")

        private lateinit var mongoInitiator: MongoInitiator

        @BeforeAll
        @JvmStatic
        fun setup() {
            mongoInitiator = MongoInitiator("${mongoContainer.connectionString}/${DATABASE}")
        }
    }

    @Test
    fun `migrate() copies data from source to destination`() {
        val mockSource = mockk<DatabaseSource>()
        val mockDest = mockk<DatabaseDestination>()
        val mockCollection = mockk<DocumentCollection>()

        val testMetadata = CollectionMetadata(
            name = "users",
            fields = listOf(
                FieldMetadata("id", "string"),
                FieldMetadata("name", "string")
            )
        )

        val testDocuments = flowOf(
            mapOf("id" to "1", "name" to "Test User 1"),
            mapOf("id" to "2", "name" to "Test User 2")
        )

        every { mockCollection.metadata } returns testMetadata
        every { mockCollection.readDocuments() } returns testDocuments
        every { mockSource.listCollections() } returns flowOf(mockCollection)
        coEvery { mockDest.createCollection(eq(testMetadata)) } returns Unit
        coEvery { mockDest.insertDocuments(eq("users"), eq(testDocuments)) } returns Unit

        runBlocking { Migrator().migrate(mockSource, mockDest) }

        verify(exactly = 1) { mockSource.listCollections() }
        verify(exactly = 2) { mockCollection.metadata }
        verify(exactly = 1) { mockCollection.readDocuments() }
        coVerify(exactly = 1) { mockDest.createCollection(testMetadata) }
        coVerify(exactly = 1) { mockDest.insertDocuments(testMetadata.name, any()) }
    }

    @Test
    fun `migrate() works correctly with empty source`() {
        val mockSource = mockk<DatabaseSource>()
        val mockDest = mockk<DatabaseDestination>()

        every { mockSource.listCollections() } returns emptyFlow()

        runBlocking { Migrator().migrate(mockSource, mockDest) }

        verify(exactly = 1) { mockSource.listCollections() }
        coVerify(exactly = 0) { mockDest.createCollection(any()) }
        coVerify(exactly = 0) { mockDest.insertDocuments(any(), any()) }
    }

    @Test
    fun `migrate() handles large datasets efficiently`() = runBlocking {
        val mockSource = mockk<DatabaseSource>()
        val dest = MongoDestination("${mongoContainer.connectionString}/${DATABASE}")
        val mockCollection = mockk<DocumentCollection>()

        val testMetadata = CollectionMetadata(
            name = "large_table",
            fields = listOf(FieldMetadata("id", "string"))
        )

        // Generate a large number of test documents
        var recordsCount = 0
        val testDocuments = flow {
            while (recordsCount < NUM_RECORDS) {
                emit(mapOf("id" to recordsCount.toString()))
                recordsCount++
            }
        }

        every { mockCollection.metadata } returns testMetadata
        every { mockCollection.readDocuments() } returns testDocuments
        every { mockSource.listCollections() } returns flowOf(mockCollection)

        Migrator().migrate(mockSource, dest)

        assertEquals(NUM_RECORDS, recordsCount)
        assertEquals(
            NUM_RECORDS.toLong(),
            mongoInitiator.mongoDb.getCollection<BsonDocument>("large_table").countDocuments()
        )
    }
}
