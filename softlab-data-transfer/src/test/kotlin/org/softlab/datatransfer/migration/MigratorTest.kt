package org.softlab.datatransfer.migration

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyOrder
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
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


class MigratorTest {
    @Test
    fun `migrate() copies data from source to destination`() {
        val testMetadata = CollectionMetadata(
            name = "users",
            fields = mapOf(
                "id" to FieldDefinition("id", "string"),
                "name" to FieldDefinition("name", "string")
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
            coEvery { createIndexesAndConstraints(eq(testMetadata)) } returns Unit
        }

        runBlocking { Migrator().migrate(mockSource, mockDest) }

        verify(exactly = 1) { mockSource.listCollections() }
        coVerify(exactly = 1) { mockCollection.fetchMetadata() }
        verify(exactly = 1) { mockCollection.readDocuments() }
        coVerifyOrder {
            mockDest.createCollection(testMetadata)
            mockDest.insertDocuments(testMetadata.name, testDocuments)
            mockDest.createIndexesAndConstraints(testMetadata)
        }
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
        coVerify(exactly = 0) { mockDest.createIndexesAndConstraints(any()) }
    }

    @Test
    fun `migrate() filters collections by source name prefixes`() {
        val includedMetadata = CollectionMetadata(
            name = "users_active",
            fields = mapOf("id" to FieldDefinition("id", "string"))
        )
        val skippedMetadata = CollectionMetadata(
            name = "orders",
            fields = mapOf("id" to FieldDefinition("id", "string"))
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
            coEvery { createIndexesAndConstraints(eq(includedMetadata)) } returns Unit
        }

        runBlocking {
            Migrator(sourceFilter = StringTokenFilter.from(listOf("users", "profiles")))
                .migrate(mockSource, mockDest)
        }

        coVerify(exactly = 1) { includedCollection.fetchMetadata() }
        verify(exactly = 1) { includedCollection.readDocuments() }
        coVerify(exactly = 1) { skippedCollection.fetchMetadata() }
        verify(exactly = 0) { skippedCollection.readDocuments() }
        coVerifyOrder {
            mockDest.createCollection(includedMetadata)
            mockDest.insertDocuments(includedMetadata.name, includedDocuments)
            mockDest.createIndexesAndConstraints(includedMetadata)
        }
        coVerify(exactly = 0) { mockDest.createCollection(skippedMetadata) }
        coVerify(exactly = 0) { mockDest.insertDocuments(skippedMetadata.name, any()) }
        coVerify(exactly = 0) { mockDest.createIndexesAndConstraints(skippedMetadata) }
    }

    @Test
    fun `migrate() handles large datasets efficiently`() {
        val testMetadata = CollectionMetadata(
            name = "public.large_table",
            fields = mapOf("id" to FieldDefinition("id", "string"))
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
            coEvery { createIndexesAndConstraints(testMetadata) } returns Unit
        }

        runBlocking { Migrator().migrate(mockSource, mockDestination) }

        assertEquals(NUM_RECORDS, recordsCount)
        coVerify(exactly = 1) { mockDestination.createCollection(testMetadata) }
        coVerify(exactly = 1) { mockDestination.insertDocuments(testMetadata.name, testDocuments) }
        coVerify(exactly = 1) { mockDestination.createIndexesAndConstraints(testMetadata) }
    }

    companion object {
        private const val NUM_RECORDS = 1_000_000L
    }
}
