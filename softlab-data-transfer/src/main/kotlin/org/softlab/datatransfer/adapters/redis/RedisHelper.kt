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

package org.softlab.datatransfer.adapters.redis

import io.lettuce.core.ScanArgs
import io.lettuce.core.api.async.RedisAsyncCommands
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await


const val KEYS_SCAN_SIZE = 2000L

fun RedisAsyncCommands<String, String>.scanKeys(
    pattern: String,
    scanCount: Long = KEYS_SCAN_SIZE
): Flow<List<String>> = flow {
    val scanArgs = ScanArgs().match(pattern).limit(scanCount)
    var cursor = this@scanKeys.scan(scanArgs).await()
    while (true) {
        if (cursor.keys.isNotEmpty()) {
            emit(cursor.keys)
        }
        if (cursor.isFinished) {
            break
        }
        cursor = this@scanKeys.scan(cursor, scanArgs).await()
    }
}
