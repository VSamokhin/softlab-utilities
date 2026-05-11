package org.softlab.datatransfer.migration

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.softlab.dataset.core.FieldDefinition
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.DatabaseSource
import org.softlab.datatransfer.core.DocumentCollection
import org.softlab.datatransfer.core.StringTokenFilter
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds


class MigratorTest {
    @Test
    fun `migrate() copies data from source to destination`() {
        val testMetadata = CollectionMetadata(
            name = "users",
            fields = listOf(
                FieldDefinition("id", "string"),
                FieldDefinition("name", "string")
            )
        )

        val testDocuments = flowOf(
            mapOf("id" to "1", "name" to "Test User 1"),
            mapOf("id" to "2", "name" to "Test User 2")
        )

        val mockCollection = mockk<DocumentCollection> {
            coEvery { fetchMetadata() } returns testMetadata
            every { readDocuments() } returns testDocuments
        }
        val mockSource = mockk<DatabaseSource> {
            every { listCollections() } returns flowOf(mockCollection)
        }
        val mockDest = mockk<DatabaseDestination> {
            coEvery { createCollection(eq(testMetadata)) } returns Unit
            coEvery { insertDocuments(eq("users"), eq(testDocuments)) } returns Unit
        }

        runBlocking { Migrator().migrate(mockSource, mockDest) }

        verify(exactly = 1) { mockSource.listCollections() }
        coVerify(exactly = 1) { mockCollection.fetchMetadata() }
        verify(exactly = 1) { mockCollection.readDocuments() }
        coVerify(exactly = 1) { mockDest.createCollection(testMetadata) }
        coVerify(exactly = 1) { mockDest.insertDocuments(testMetadata.name, any()) }
    }

    @Test
    fun `migrate() works correctly with empty source`() {
        val mockSource = mockk<DatabaseSource> {
            every { listCollections() } returns emptyFlow()
        }
        val mockDest = mockk<DatabaseDestination>()

        runBlocking { Migrator().migrate(mockSource, mockDest) }

        verify(exactly = 1) { mockSource.listCollections() }
        coVerify(exactly = 0) { mockDest.createCollection(any()) }
        coVerify(exactly = 0) { mockDest.insertDocuments(any(), any()) }
    }

    @Test
    fun `migrate() filters collections by source name prefixes`() {
        val includedMetadata = CollectionMetadata(
            name = "users_active",
            fields = listOf(FieldDefinition("id", "string"))
        )
        val skippedMetadata = CollectionMetadata(
            name = "orders",
            fields = listOf(FieldDefinition("id", "string"))
        )
        val includedDocuments = flowOf(mapOf("id" to "1"))

        val includedCollection = mockk<DocumentCollection> {
            coEvery { fetchMetadata() } returns includedMetadata
            every { readDocuments() } returns includedDocuments
        }
        val skippedCollection = mockk<DocumentCollection> {
            coEvery { fetchMetadata() } returns skippedMetadata
            every { readDocuments() } returns flowOf(mapOf("id" to "2"))
        }
        val mockSource = mockk<DatabaseSource> {
            every { listCollections() } returns flowOf(includedCollection, skippedCollection)
        }
        val mockDest = mockk<DatabaseDestination> {
            coEvery { createCollection(eq(includedMetadata)) } returns Unit
            coEvery { insertDocuments(eq(includedMetadata.name), any()) } returns Unit
        }

        runBlocking {
            Migrator(sourceFilter = StringTokenFilter.from(listOf("users", "profiles")))
                .migrate(mockSource, mockDest)
        }

        coVerify(exactly = 1) { includedCollection.fetchMetadata() }
        verify(exactly = 1) { includedCollection.readDocuments() }
        coVerify(exactly = 1) { skippedCollection.fetchMetadata() }
        verify(exactly = 0) { skippedCollection.readDocuments() }
        coVerify(exactly = 1) { mockDest.createCollection(includedMetadata) }
        coVerify(exactly = 0) { mockDest.createCollection(skippedMetadata) }
        coVerify(exactly = 1) { mockDest.insertDocuments(includedMetadata.name, any()) }
        coVerify(exactly = 0) { mockDest.insertDocuments(skippedMetadata.name, any()) }
    }

    @Test
    fun `migrate() processes collections concurrently with configured threads`() {
        val activeCreates = AtomicInteger(0)
        val peakCreates = AtomicInteger(0)

        val source = object : DatabaseSource {
            override fun getBackendName(): String = "test"

            override fun listCollections() = flowOf(
                testCollection("users_1"),
                testCollection("users_2")
            )

            override suspend fun countDocuments(collectionName: String): Long = 0

            override fun close() = Unit
        }
        val destination = object : DatabaseDestination {
            override fun getBackendName(): String = "test"

            override suspend fun createCollection(metadata: CollectionMetadata) {
                val concurrentCalls = activeCreates.incrementAndGet()
                peakCreates.accumulateAndGet(concurrentCalls) { current, update -> maxOf(current, update) }
                delay(100.milliseconds)
                activeCreates.decrementAndGet()
            }

            override suspend fun dropCollection(collectionName: String) = Unit

            override suspend fun insertDocuments(
                collectionName: String,
                documents: kotlinx.coroutines.flow.Flow<Map<String, Any?>>
            ) {
                documents.collect { }
            }

            override fun close() = Unit
        }

        runBlocking { Migrator(workerThreads = 2).migrate(source, destination) }

        assertEquals(2, peakCreates.get())
    }

    private fun testCollection(name: String) = object : DocumentCollection {
        override suspend fun fetchMetadata() = CollectionMetadata(
            name = name,
            fields = listOf(FieldDefinition("id", "string"))
        )

        override fun readDocuments() = flowOf(mapOf("id" to name))
    }

    @Test
    fun `migrate() handles large datasets efficiently`() {
        val testMetadata = CollectionMetadata(
            name = "public.large_table",
            fields = listOf(FieldDefinition("id", "string"))
        )

        // Generate a large number of test documents
        var recordsCount = 0L
        val testDocuments = flow {
            while (recordsCount < NUM_RECORDS) {
                emit(mapOf("id" to recordsCount.toString()))
                recordsCount++
            }
        }

        val mockCollection = mockk<DocumentCollection> {
            coEvery { fetchMetadata() } returns testMetadata
            every { readDocuments() } returns testDocuments
        }
        val mockSource = mockk<DatabaseSource> {
            every { listCollections() } returns flowOf(mockCollection)
        }

        val mockDestination = mockk<DatabaseDestination> {
            coEvery { createCollection(testMetadata) } returns Unit
            coEvery { insertDocuments(testMetadata.name, testDocuments) } coAnswers {
                testDocuments.collect { }
            }
        }

        runBlocking { Migrator().migrate(mockSource, mockDestination) }

        assertEquals(NUM_RECORDS, recordsCount)
        coVerify(exactly = 1) { mockDestination.createCollection(testMetadata) }
        coVerify(exactly = 1) { mockDestination.insertDocuments(testMetadata.name, testDocuments) }
    }

    companion object {
        private const val NUM_RECORDS = 1_000_000L
    }
}
