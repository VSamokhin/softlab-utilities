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
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

/**
 * I just follow the same file naming schema
 */

const val REDIS_CONTAINER = "redis:8.6.1-alpine"

fun createRedisContainer(container: String = REDIS_CONTAINER): RedisContainer =
    RedisContainer(DockerImageName.parse(container))
        // Workaround for Rancher Desktop on Mac, somehow redis container is not ready while the tests start
        .waitingFor(Wait.forListeningPorts(6379))