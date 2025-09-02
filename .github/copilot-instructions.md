<!-- Use this file to provide workspace-specific custom instructions to Copilot. For more details, visit https://code.visualstudio.com/docs/copilot/copilot-customization#_use-a-githubcopilotinstructionsmd-file -->

This is a Kotlin Gradle, multi-module project providing different tools and utilities.

While answering questions in the 'Ask' mode, please don't use the local project context until explicitly asked to do so.

When asked about project structure, dependencies, or configuration, please refer to the following guidelines:

Organize all Kotlin source files under `src/main/kotlin` of every subproject, according to their package declarations. Place unit test files under `src/test/kotlin` and integration test files under `src/integrationTest/kotlin` accordingly.

This project uses Gradle's convention plugins to share configuration across multiple subprojects, these plugins are located in the `buildSrc/src/main/groovy` directory.

If using WireMock in tests, then place the WireMock stubs under `src/test/resources/__files` and the mappings under `src/test/resources/mappings` of a corresponding subproject.

To define mocks in tests, use the `io.mockk:mockk` library, but if adding it as a dependency, exclude all its transient dependencies to all modules of `org.junit.jupiter`.

To update the JaCoCo coverage thresholds:
* Build the whole project with `./gradlew clean build`
* After that in every subproject there should appear a file `build/reports/jacoco/test/jacocoTestReport.xml`
* In this file there are counters under the root element
  * Use the `INSTRUCTION` counter to calculate the current test coverage value, round it always down to two decimal places, it should then be set as an expected minimum
  * Use the `CLASS` counter to set the maximal allowed number of missed classes
* Update corresponding parameters of `jacocoVerification` in `build.gradle` of every subproject.