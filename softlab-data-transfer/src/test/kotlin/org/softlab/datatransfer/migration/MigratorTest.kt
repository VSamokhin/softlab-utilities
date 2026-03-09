package org.softlab.datatransfer.migration

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.emptyFlow
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


class MigratorTest {
    @Test
    fun `migrate() copies data from source to destination`() {
        val mockSource = mockk<DatabaseSource>()
        val mockDest = mockk<DatabaseDestination>()
        val mockCollection = mockk<DocumentCollection>()

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

        coEvery { mockCollection.fetchMetadata() } returns testMetadata
        every { mockCollection.readDocuments() } returns testDocuments
        every { mockSource.listCollections() } returns flowOf(mockCollection)
        coEvery { mockDest.createCollection(eq(testMetadata)) } returns Unit
        coEvery { mockDest.insertDocuments(eq("users"), eq(testDocuments)) } returns Unit

        runBlocking { Migrator().migrate(mockSource, mockDest) }

        verify(exactly = 1) { mockSource.listCollections() }
        coVerify(exactly = 1) { mockCollection.fetchMetadata() }
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
    fun `migrate() filters collections by source name prefixes`() = runBlocking {
        val mockSource = mockk<DatabaseSource>()
        val mockDest = mockk<DatabaseDestination>()
        val includedCollection = mockk<DocumentCollection>()
        val skippedCollection = mockk<DocumentCollection>()

        val includedMetadata = CollectionMetadata(
            name = "users_active",
            fields = listOf(FieldDefinition("id", "string"))
        )
        val skippedMetadata = CollectionMetadata(
            name = "orders",
            fields = listOf(FieldDefinition("id", "string"))
        )
        val includedDocuments = flowOf(mapOf("id" to "1"))

        coEvery { includedCollection.fetchMetadata() } returns includedMetadata
        every { includedCollection.readDocuments() } returns includedDocuments
        coEvery { skippedCollection.fetchMetadata() } returns skippedMetadata
        every { skippedCollection.readDocuments() } returns flowOf(mapOf("id" to "2"))
        every { mockSource.listCollections() } returns flowOf(includedCollection, skippedCollection)
        coEvery { mockDest.createCollection(eq(includedMetadata)) } returns Unit
        coEvery { mockDest.insertDocuments(eq(includedMetadata.name), any()) } returns Unit

        Migrator(sourceFilter = StringTokenFilter.from(listOf("users", "profiles")))
            .migrate(mockSource, mockDest)

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
    fun `migrate() processes collections concurrently with configured threads`() = runBlocking {
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
                delay(100)
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

        Migrator(workerThreads = 2).migrate(source, destination)

        assertEquals(2, peakCreates.get())
    }

    private fun testCollection(name: String) = object : DocumentCollection {
        override suspend fun fetchMetadata() = CollectionMetadata(
            name = name,
            fields = listOf(FieldDefinition("id", "string"))
        )

        override fun readDocuments() = flowOf(mapOf("id" to name))
    }
}
