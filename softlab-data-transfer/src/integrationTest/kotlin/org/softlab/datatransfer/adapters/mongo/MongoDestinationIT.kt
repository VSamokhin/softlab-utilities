package org.softlab.datatransfer.adapters.mongo

import com.mongodb.MongoCommandException
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.bson.BsonDocument
import org.bson.Document
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.hasKey
import org.hamcrest.Matchers.not
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.softlab.dataset.core.FieldDefinition
import org.softlab.datataset.test.initiators.MongoInitiator
import org.softlab.datataset.test.initiators.createMongoContainer
import org.softlab.datatransfer.config.ConfigProvider
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.IndexDefinition
import org.softlab.datatransfer.core.IndexFieldDefinition
import org.softlab.datatransfer.core.IndexSortOrder
import org.softlab.datatransfer.core.PrimaryKeyDefinition
import org.softlab.datatransfer.core.SqlTableName
import org.softlab.datatransfer.core.sqlName
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue


@Testcontainers
class MongoDestinationIT {
    companion object {
        private const val DATABASE = "testdb"

        @Container
        @JvmStatic
        private val mongoContainer = createMongoContainer()

        private lateinit var mongoInitiator: MongoInitiator

        private val dataTypeMappings = ConfigProvider.config.getDataTypeMappings()

        @BeforeAll
        @JvmStatic
        fun setup() {
            mongoInitiator = MongoInitiator("${mongoContainer.connectionString}/$DATABASE")
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            mongoInitiator.close()
        }
    }

    @BeforeEach
    fun seedData() = runBlocking {
        mongoInitiator.getConnection().listCollections()
            .map { it.getString("name") }
            .collect { collection ->
                mongoInitiator.getConnection()
                    .getCollection<Document>(collection)
                    .drop()
            }
    }

    @Test
    fun `createCollection() creates expected collection`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(
                CollectionMetadata("new_collection",
                    mapOf(
                        "id" to FieldDefinition("id", "string"),
                        "nickname" to FieldDefinition("nickname", "string", nullable = true)
                    ))
            )
            destination.createCollection(
                CollectionMetadata("existing", emptyMap())
            )
        }

        assertThat(
            mongoInitiator.getConnection().listCollections().map { it["name"] }.toList(),
            containsInAnyOrder(
                "existing", "new_collection"
            )
        )

        val newValidator = getJsonSchemaValidator("new_collection")
        val newProperties = newValidator.get("properties", Document::class.java)

        assertEquals(listOf("id"), newValidator.getList("required", String::class.java))
        assertEquals("string", newProperties.get("id", Document::class.java).getString("bsonType"))
        assertEquals(
            listOf("string", "null"),
            newProperties.get("nickname", Document::class.java).getList("bsonType", String::class.java)
        )

        val existingCollection = mongoInitiator.getConnection().listCollections()
            .toList()
            .first { it.getString("name") == "existing" }
        val existingOptions = existingCollection.get("options", Document::class.java)
        assertNotNull(existingOptions)
        assertTrue(existingOptions.isEmpty())
    }

    @Test
    fun `createCollection() forms expected validator descriptor`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(
                CollectionMetadata(
                    "validator_shape",
                    mapOf(
                        "user_id" to FieldDefinition("user_id", "long"),
                        "nickname" to FieldDefinition("nickname", "string", nullable = true),
                        "active" to FieldDefinition("active", "bool")
                    )
                )
            )
        }

        val validator = getJsonSchemaValidator("validator_shape")
        val expected = Document(
            mapOf(
                "bsonType" to "object",
                "properties" to Document(
                    mapOf(
                        "user_id" to Document(mapOf("bsonType" to "long")),
                        "nickname" to Document(mapOf("bsonType" to listOf("string", "null"))),
                        "active" to Document(mapOf("bsonType" to "bool"))
                    )
                ),
                "required" to listOf("user_id", "active")
            )
        )
        assertEquals(expected, validator)
    }

    @Test
    fun `createCollection() omits required when all fields nullable`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(
                CollectionMetadata(
                    "validator_all_nullable",
                    mapOf(
                        "a" to FieldDefinition("a", "string", nullable = true),
                        "b" to FieldDefinition("b", "double", nullable = true)
                    )
                )
            )
        }

        val validator = getJsonSchemaValidator("validator_all_nullable")
        assertThat(validator, not(hasKey("required")))
    }

    @Test
    fun `createCollection() maps aliases to bson types using config`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(
                CollectionMetadata(
                    "validator_with_aliases",
                    mapOf(
                        "age" to FieldDefinition("age", "integer"),
                        "memo" to FieldDefinition("memo", "text", nullable = true),
                        "enabled" to FieldDefinition("enabled", "boolean")
                    )
                )
            )
        }

        val validator = getJsonSchemaValidator("validator_with_aliases")
        val properties = validator.get("properties", Document::class.java)
        assertEquals("int", properties.get("age", Document::class.java).getString("bsonType"))
        assertEquals(
            listOf("string", "null"),
            properties.get("memo", Document::class.java).getList("bsonType", String::class.java)
        )
        assertEquals("bool", properties.get("enabled", Document::class.java).getString("bsonType"))
    }

    @Test
    fun `createCollection() causes mongo to throw for unknow field type`() {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            val col = CollectionMetadata("users",
                mapOf("notes" to FieldDefinition("notes", "custom"))
            )

            val exc = assertThrows<MongoCommandException> {
                runBlocking { destination.createCollection(col) }
            }
            assertThat(exc.message, containsString("custom"))
        }
    }

    @Test
    fun `createCollection() throws when collection already exists`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            val metadata = CollectionMetadata("users", emptyMap())

            destination.createCollection(metadata)

            val exc = assertThrows<IllegalStateException> {
                destination.createCollection(metadata)
            }
            assertThat(exc.message, containsString("users"))
        }
    }

    @Test
    fun `createIndexesAndConstraints() creates primary key and supported secondary indices after documents are inserted`() = runBlocking {
        val metadata = usersMetadataWithPrimaryKeyAndIndices()

        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(metadata)
            destination.insertDocuments(
                metadata.name,
                flowOf(
                    mapOf("id" to 1, "tenant_id" to 1, "name" to "Alice", "email" to "alice@example.com"),
                    mapOf("id" to 2, "tenant_id" to 1, "name" to "Bob", "email" to "bob@example.com")
                )
            )
            destination.createIndexesAndConstraints(metadata)
        }

        val actualMetadata = MongoDocumentCollection(metadata.name, mongoInitiator.getConnection()).fetchMetadata()

        assertNull(actualMetadata.primaryKey)
        assertContentEquals(metadata.expectedMongoIndices(), actualMetadata.indices)
    }

    @Test
    fun `createIndexesAndConstraints() creates primary key when metadata has no secondary indices`() = runBlocking {
        val metadata = CollectionMetadata(
            name = "users",
            fields = mapOf("id" to FieldDefinition("id", "int")),
            primaryKey = PrimaryKeyDefinition("users_pkey", listOf("id"))
        )

        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(metadata)
            destination.createIndexesAndConstraints(metadata)
        }

        val actualMetadata = MongoDocumentCollection(metadata.name, mongoInitiator.getConnection()).fetchMetadata()

        assertNull(actualMetadata.primaryKey)
        assertContentEquals(
            listOf(IndexDefinition("users_pkey", listOf(IndexFieldDefinition("id")), unique = true)),
            actualMetadata.indices
        )
    }

    @Test
    fun `createIndexesAndConstraints() fails when primary key data has duplicates`() = runBlocking {
        val metadata = CollectionMetadata(
            name = "users",
            fields = mapOf("id" to FieldDefinition("id", "int")),
            primaryKey = PrimaryKeyDefinition("users_pkey", listOf("id"))
        )

        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(metadata)
            destination.insertDocuments(
                metadata.name,
                flowOf(
                    mapOf("id" to 1),
                    mapOf("id" to 1)
                )
            )

            assertThrows<MongoCommandException> {
                destination.createIndexesAndConstraints(metadata)
            }
        }
    }

    @Test
    fun `createIndexesAndConstraints() fails when unique index data has duplicates`() = runBlocking {
        val metadata = CollectionMetadata(
            name = "users",
            fields = mapOf(
                "id" to FieldDefinition("id", "int"),
                "email" to FieldDefinition("email", "string")
            ),
            indices = listOf(
                IndexDefinition(
                    name = "users_email_unique_idx",
                    fields = listOf(IndexFieldDefinition("email")),
                    unique = true
                )
            )
        )

        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(metadata)
            destination.insertDocuments(
                metadata.name,
                flowOf(
                    mapOf("id" to 1, "email" to "duplicate@example.com"),
                    mapOf("id" to 2, "email" to "duplicate@example.com")
                )
            )

            assertThrows<MongoCommandException> {
                destination.createIndexesAndConstraints(metadata)
            }
        }
    }

    @Test
    fun `getBackendName() returns mongo`() {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            assertEquals("mongo", destination.getBackendName())
        }
    }

    @Test
    fun `dropCollection() drops collection with all data`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(CollectionMetadata("users", emptyMap()))
            destination.insertDocuments(
                "users",
                flowOf(
                    mapOf("user_id" to "1", "name" to "Alice"),
                    mapOf("user_id" to "2", "name" to "Bob")
                )
            )
            destination.dropCollection("users")
        }

        val collectionNames = mongoInitiator.getConnection().listCollections()
            .map { it.getString("name") }
            .toList()
        assertTrue("users" !in collectionNames)
    }

    @Test
    fun `dropCollection() doesn't fail if collection not exists`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            assertDoesNotThrow { destination.dropCollection("something") }
        }
    }

    @Test
    fun `insertDocuments() writes expected data`() = runBlocking {
        MongoDestination(mongoInitiator.dbUrl, dataTypeMappings).use { destination ->
            destination.createCollection(CollectionMetadata("users", emptyMap()))
            destination.insertDocuments(
                "users",
                flowOf(
                    mapOf("user_id" to "1", "name" to "Alice"),
                    mapOf("user_id" to "2", "name" to "Bob")
                )
            )
        }

        val docs = mongoInitiator.getConnection()
            .getCollection<BsonDocument>("users")
            .find()
            .toList()

        assertThat(
            docs.map { it["user_id"]?.asString()?.value }.toList(),
            containsInAnyOrder("1", "2")
        )
        assertThat(
            docs.map { it["name"]?.asString()?.value }.toList(),
            containsInAnyOrder("Alice", "Bob")
        )
    }

    private suspend fun getJsonSchemaValidator(collectionName: String): Document {
        val collectionInfo = mongoInitiator.getConnection().listCollections()
            .toList()
            .first { it.getString("name") == collectionName }
        return collectionInfo
            .get("options", Document::class.java)
            .get("validator", Document::class.java)
            .get("\$jsonSchema", Document::class.java)
    }

    private fun usersMetadataWithPrimaryKeyAndIndices(): CollectionMetadata =
        CollectionMetadata(
            name = "users",
            fields = mapOf(
                "id" to FieldDefinition("id", "int"),
                "tenant_id" to FieldDefinition("tenant_id", "int"),
                "name" to FieldDefinition("name", "string"),
                "email" to FieldDefinition("email", "string")
            ),
            primaryKey = PrimaryKeyDefinition("users_pkey", listOf("id")),
            indices = listOf(
                IndexDefinition(
                    name = "users_email_unique_idx",
                    fields = listOf(IndexFieldDefinition("email")),
                    unique = true
                ),
                IndexDefinition(
                    name = "users_tenant_name_idx",
                    fields = listOf(
                        IndexFieldDefinition("tenant_id"),
                        IndexFieldDefinition("name", IndexSortOrder.DESC)
                    )
                )
            )
        )

    private fun CollectionMetadata.expectedMongoIndices(): List<IndexDefinition> =
        (
            listOfNotNull(
                primaryKey?.let {
                    IndexDefinition(
                        name = it.sqlName(SqlTableName.of(name)),
                        fields = it.fields.map { field -> IndexFieldDefinition(field) },
                        unique = true
                    )
                }
            ) + indices
        ).sortedBy { it.name }
}
