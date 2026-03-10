package org.softlab.dataset.mongo.coroutine

import com.mongodb.MongoNamespace
import com.mongodb.client.model.DeleteOptions
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.kotlin.client.coroutine.MongoCollection
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import org.bson.BsonDocument
import org.bson.conversions.Bson
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals


class CoroutineMongoCollectionTest {
    @Test
    fun `name returns wrapped collection name`() {
        val wrapped = mockk<MongoCollection<BsonDocument>>(relaxed = true)
        every { wrapped.namespace } returns MongoNamespace("testdb", "users")

        val cut = CoroutineMongoCollection(wrapped)

        assertEquals("users", cut.name)
    }

    @Test
    fun `deleteAll() delegates to wrapped collection`() {
        val wrapped = mockk<MongoCollection<BsonDocument>>(relaxed = true)
        val cut = CoroutineMongoCollection(wrapped)

        cut.deleteAll()

        coVerify(exactly = 1) { wrapped.deleteMany(any<Bson>(), any<DeleteOptions>()) }
    }

    @Test
    fun `insertMany() delegates to wrapped collection`() {
        val wrapped = mockk<MongoCollection<BsonDocument>>(relaxed = true)
        val cut = CoroutineMongoCollection(wrapped)
        val docs = listOf(BsonDocument("id", org.bson.BsonInt32(1)))

        cut.insertMany(docs)

        coVerify(exactly = 1) { wrapped.insertMany(docs, any<InsertManyOptions>()) }
    }
}
