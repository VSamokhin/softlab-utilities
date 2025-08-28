/**
 * Copyright (C) 2025, Viktor Samokhin (wowyupiyo@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.softlab.datatransfer.adapters.postgres

import kotlinx.coroutines.flow.Flow
import org.softlab.datatransfer.core.CollectionMetadata
import org.softlab.datatransfer.core.DatabaseDestination
import org.softlab.datatransfer.core.Document
import java.sql.Connection
import java.sql.DriverManager


class PostgresDestination(private val connString: String) : DatabaseDestination {
    private val connection: Connection = DriverManager.getConnection(connString)

    override suspend fun createCollection(metadata: CollectionMetadata) {
        val columnsDef = metadata.fields.joinToString(", ") {
            "\"${it.name}\" ${mapType(it.type)}"
        }
        val sql = "CREATE TABLE IF NOT EXISTS \"${metadata.name}\" ($columnsDef)"
        connection.createStatement().use { it.execute(sql) }
    }

    private fun mapType(type: String): String {
        return when (type.lowercase()) {
            "int", "integer" -> "INTEGER"
            "text", "string" -> "TEXT"
            "boolean" -> "BOOLEAN"
            else -> "TEXT"
        }
    }

    override suspend fun insertDocuments(collectionName: String, documents: Flow<Document>) {
        documents.collect { doc ->
            val columns = doc.keys.joinToString(", ") { "\"$it\"" }
            val placeholders = doc.keys.joinToString(", ") { "?" }
            val stmt = connection.prepareStatement(
                "INSERT INTO \"$collectionName\" ($columns) VALUES ($placeholders)"
            )
            doc.values.forEachIndexed { i, value ->
                stmt.setObject(i + 1, value)
            }
            stmt.executeUpdate()
            stmt.close()
        }
    }

    override fun close() {
        connection.close()
    }
}
