* This is a Kotlin Gradle, multi-module project providing different tools and utilities.

* Regarding the project structure, dependencies, or configuration, please refer to the following guidelines:
  * Organize all Kotlin source files under `src/main/kotlin` of every subproject, according to their package declarations. Place unit test files under `src/test/kotlin` and integration test files under `src/integrationTest/kotlin` accordingly.
  * This project uses Gradle's convention plugins to share configuration across multiple subprojects, these plugins are located in the `buildSrc/src/main/groovy` directory.
  * If using WireMock in tests, then place the WireMock stubs under `src/test/resources/__files` and the mappings under `src/test/resources/mappings` of a corresponding subproject.
  * The project uses `detekt` for static code analysis, and its configuration is located in `etc/detekt/detekt-config.yml`.
  * After adding a new dependency, don't change `versions.properties` file directly, but instead execute `./gradlew build` to allow `refershVersions` plugin to update it with the new dependency version.
  * Report all important internal state changes, allowed corner cases to a log using `io.github.oshai:kotlin-logging-jvm` with `ch.qos.logback:logback-classic` as its backend.
    * distinguish between the following logging levels:
      * `info` — basic output of the most important events and actions, may include a work progress report, etc.
      * `debug` — some more information on the internal state changes
      * `trace` — most detailed output with e.g. SQL-queries or similar information
      * `warn` — report exceptional however acceptable states if any
      * `error` — use it almost never as it is rather a matter of an exception.
  * Prefer the fail-fast approach, don't work around problems which are not part of an expected execution flow, better let the code fail with a clear exception message.

* Creating a new source file or changing an existing one:
  * Ensure there is a single trailing empty line
  * Add or update a header in the beginning of it and with correct year or range of years, name and e-mail, with the following content:
```kotlin
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
```

* Please follow these guidelines in regard to tests: 
  * When writing new code or changing existing code, always add unit tests for it, and if possible, add integration tests as well.
  * Keep corresponding tests and test classes up to date when changing the code or renaming source code modules.
  * For test modules a file header is not needed.
  * Writing source code, assume a possibility of direct mocking in tests, without the need to apply global mocks, or mock hooks, or static mocks.
  * To define mocks in tests, use `io.mockk:mockk` library, but if adding it as a dependency, exclude all its transient dependencies to all modules of `org.junit.jupiter`.
  * When testing equality correctness, use `nl.jqno.equalsverifier:equalsverifier` library.
  * When testing `toString()` correctness, use `com.jparams:to-string-verifier` library.
  * Do not wrap entire test body with `runBlocking`, but instead use `runBlocking` only on particular suspend call sites.
  * For assertion of iterables, use `kotlin.test.assertContentEquals`
  * Don't provide an error message to assertion functions as most of the time it's excessive.

* To update the JaCoCo coverage thresholds:
  * Build the whole project with `./gradlew clean build`.
  * After that in every subproject there should appear a file `build/reports/jacoco/test/jacocoTestReport.xml`.
  * In this file there are counters under the root element:
    * Use the `INSTRUCTION` counter to calculate the current test coverage value, round it always down to two decimal places, it should then be set as an expected minimum.
    * Use the `CLASS` counter to set the maximal allowed number of missed classes.
  * Update corresponding parameters of `jacocoVerification` in `build.gradle` of every subproject.