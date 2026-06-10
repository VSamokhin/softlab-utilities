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

package org.softlab.datatransfer.adapters.jdbc

import org.apache.commons.dbcp2.BasicDataSource
import org.softlab.dataset.jdbc.JdbcConnectionPool
import java.sql.Connection


open class BasicJdbcConnectionPool(
    jdbcUrl: String,
    driverClassName: String,
    username: String? = null,
    password: String? = null,
    maxTotal: Int = 10,
    protected val dataSource: BasicDataSource = BasicDataSource()
) : JdbcConnectionPool {

    init {
        dataSource.apply {
            this.driverClassName = driverClassName
            this.url = jdbcUrl
            this.username = username
            this.password = password
            this.maxTotal = maxTotal
        }
    }

    override fun openConnection(): Connection = dataSource.connection

    override val closed: Boolean get() = dataSource.isClosed

    override fun close() {
        dataSource.close()
    }
}

