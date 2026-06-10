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

package org.softlab.datatransfer.util

import kotlinx.coroutines.Job
import kotlinx.coroutines.runBlocking
import java.util.LinkedList


/**
 * Remove finished jobs from the list and, if it is needed, wait for some other to finish to keep count of pending jobs
 * under the given limit
 */
fun LinkedList<Job>.reduce(limit: Int) {
    require(limit > 0) { "Limit must be > 0" }
    runBlocking {
        var iterator = listIterator()
        while (iterator.hasNext()) {
            if (iterator.next().isCompleted) {
                iterator.remove()
            }
        }
        if (size > limit) {
            iterator = listIterator()
            do {
                iterator.next().join()
                iterator.remove()
            } while (size > limit)
        }
    }
}
