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

package org.softlab.datataset.test.initiators

import com.redis.testcontainers.RedisContainer
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import org.softlab.dataset.redis.RedisYamlDatasetLoader
import org.softlab.dataset.redis.lettuce.LettuceRedis
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName


const val REDIS_CONTAINER = "redis:8.6.1-alpine"

fun createRedisContainer(container: String = REDIS_CONTAINER): RedisContainer =
    RedisContainer(DockerImageName.parse(container))
        // Workaround for Rancher Desktop on Mac, somehow redis container is not ready while the tests start
        .waitingFor(Wait.forListeningPorts(6379))

class RedisInitiator(
    override val dbUrl: String,
    private val mappingPath: String
) : DatabaseInitiator<StatefulRedisConnection<String, String>> {
    val redisClient: RedisClient = RedisClient.create(dbUrl)
    val redisConnection: StatefulRedisConnection<String, String> = redisClient.connect()

    override fun cleanup(additionalSteps: (StatefulRedisConnection<String, String>) -> Unit) {
        redisConnection.sync().flushdb()
        additionalSteps(redisConnection)
    }

    override fun initSchema(
        changelogPath: String,
        additionalSteps: (StatefulRedisConnection<String, String>) -> Unit
    ) {
        additionalSteps(redisConnection)
    }

    override fun seedData(datasetPath: String) {
        RedisYamlDatasetLoader(
            LettuceRedis(redisConnection),
            mappingPath
        ).load(datasetPath, cleanBefore = false)
    }

    override fun close() {
        redisConnection.close()
        redisClient.shutdown()
    }
}
