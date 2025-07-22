# General

This is just another set of utilities which may help you to make the development or writing robust tests easier, and it is written in Kotlin.

One day I decided to re-cap and re-think about different tricky implementation and test cases I have already faced during my career, 
and I came up writing some basic solutions for those of them, where I did not find any ready-to-use library or code snippet.

In addition, I use this project as a template for my Gradle-based (Kotlin) projects, because it well illustrates some approaches I like to use while developing:
* use of Gradle's convention plugins to share common code and configuration across multiple subprojects
* instant dependency updates with `de.fayard.refreshVersions` Gradle plugin
* Kotlin coding style checks with `io.gitlab.arturbosch.detekt` Gradle plugin
* test coverage report and verification with `jacoco` Gradle plugin (I decided not to use `org.jetbrains.kotlinx.kover` instead, because it does not allow to verify count of missed classes)
* semantical versioning with `org.shipkit.shipkit-auto-version`

# Projects

## Database Seeding with `softlab-dataset-loaders`

The project contains a set of utilities to mimic what DBUnit/DBRider already do, but with No-SQL databases. 
It may especially be handy to use them in tests for an application which supports multiple database backends, so you can share the same datasets across all of them.
Basically, these utilities allow to load DBUnit/YAML datasets into MongoDB and Redis (and a JDBC/SQL database as well). 
* For Mongo the database schema is transferred almost 1:1 where each dataset table becomes a Mongo collection and each table record becomes a Mongo document.
* For to work with Redis, you have to supply an additional YAML mapping which is used to transfer the dataset to Redis hashes. This approach have certain /obvious/ limitations, but it does its work with at least simple database schemas.
* JDBC loader is a bonus as it is just a wrapper around DBRider, but can be used in some environments, where the standard provided DBRider annotations do not work (e.g. Spring Boot + Cucumber).

The loaders can accommodate different database drivers to connect to the database, please refer to the corresponding `build.gradle` to see what dependencies you have to add to your project to let them working, depending on your particular setup.

### DBUnit Dataset –> Redis Hashes

Because this operation is not as trivial as others, and it needs an additional YAML mapping to function properly, I am going to describe it in more details.

1. The source data is saved to Redis hash and set as strings, for binary arrays or BLOBs it is thus converted into Base64 strings.
2. The main issue the aforementioned mapping aims to solve is how to transform a table where every row shares the same set of fields
to Redis hashes and/or sets where every key of any structure and every field in the same hash must be unique, namely must have a unique name.
3. Every source dataset table is processed row-by-row, accordingly the `hashws` and `sets` section of the mapping defines 
what comes to Redis in the context of a single source row.

For a simple table like depicted below there are several different mapping approaches possible, taking into account the following mapping basics:
* `table` – source table from the dataset
* `sets.key` – unique key (name) for the Redis set, when left empty, then the original table name is used.
* `sets.member` – Redis set members are unique values stored in the set, when left empty, then ALL columns of the row will be processed.
You can also use a single column value placeholder like ${column_name} to retrieve only this particular column.
* `hashes.key` – unique key (name) for the Redis hash, when left empty, then the original table name is used.
You can also define a custom key using string literals and column values given as ${column_name} placeholders.
* `hashes.field` – Redis hash field name, when left empty, then the column name is used. Remember that every field in the same Redis hash must be unique. 
You can also define a custom field name using string literals and column values given as ${column_name} placeholders.
* `hashes.value` – Redis hash field value, when left empty, then ALL columns of the row will be processed.
You can also use a single column value placeholder like ${column_name} to retrieve only this particular column.

| user_id | user_name | user_age |
|---------| --------- |----------|
| 1       | John      | 42       |
| 2       | Jane      | 42       |

#### Mapping Example 1

This kind of mapping would allow to retrieve all the source user ids at once using `SMEMBERS` or `SSCAN` Redis command and then iterate through 
every source row by its user id with `HGETALL` command.
It produces one set and two hashes, where the set contains all the source user ids as its members and the hashes each 
represents a single row from the source table.

```yaml
tables:
  - table: simple_table
    sets:
      - key:
        member: ${user_id}
    hashes:
      - key: simple_table:${user_id}
        field:
        value:
```
#### Mapping Example 2

A simpler and straightforward variant with only two hashes where if knowing already the source user ids, one could iterate through 
every source row by its user id using `HGETALL` command. It produces two hashes, where each hash represents a single row from the source table.

```yaml
tables:
  - table: simple_table
    hashes:
      - key: simple_table:${user_id}
        field:
        value:
```

# Build

To build the project, run in its root directory:

```
./gradlew clean build
```
