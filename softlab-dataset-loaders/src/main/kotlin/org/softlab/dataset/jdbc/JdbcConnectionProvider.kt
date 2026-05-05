/**
 * Copyright (C) 2026, Viktor Samokhin (wowyupiyo@gmail.com)
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

package org.softlab.dataset.jdbc

import java.sql.Connection
import java.sql.Statement


interface JdbcConnectionProvider : AutoCloseable {
    fun openConnection(): Connection
    val closed: Boolean
}

inline fun <T> JdbcConnectionProvider.withConnection(block: (Connection) -> T): T =
    this.openConnection().use(block)

inline fun <T> JdbcConnectionProvider.withStatement(block: (Statement) -> T): T =
    this.openConnection().use { it.createStatement().use(block) }
