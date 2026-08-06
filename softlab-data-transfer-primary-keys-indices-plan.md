# softlab-data-transfer Primary Keys and Indices Plan

## Design Decision

Keep primary keys and indices as separate metadata concepts.

Primary keys are identity constraints. They imply non-nullability, usually uniqueness, and often determine how relational backends enforce row identity. Indices are access-path definitions. They may be unique or non-unique, may be partial or sparse in some backends, may use backend-specific sort direction or options, and do not necessarily define identity.

The implementation can still reuse lower-level helpers for rendering backend DDL/options, but the public model should make the distinction explicit:

```kotlin
data class CollectionMetadata(
    val name: String,
    val fields: Collection<FieldDefinition>,
    val primaryKey: PrimaryKeyDefinition? = null,
    val indices: Collection<IndexDefinition> = emptyList()
)

data class PrimaryKeyDefinition(
    val name: String?,
    val fields: List<String>
)

data class IndexDefinition(
    val name: String?,
    val fields: List<IndexFieldDefinition>,
    val unique: Boolean = false
)

data class IndexFieldDefinition(
    val name: String,
    val order: IndexSortOrder = IndexSortOrder.ASC
)
```

This intentionally starts with a portable core. Backend-specific options such as PostgreSQL index method, Mongo sparse/partial indexes, expression indexes, included columns, collation, or operator classes should be added later only after a real transfer use case requires them.

## Task 1: Add Portable Metadata Models (Done)

Scope:

* Extend `CollectionMetadata` in `softlab-data-transfer/src/main/kotlin/org/softlab/datatransfer/core/Interfaces.kt` with `primaryKey` and `indices` properties using default values to keep existing call sites source-compatible.
* Add `PrimaryKeyDefinition`, `IndexDefinition`, `IndexFieldDefinition`, and `IndexSortOrder` in the same core package.
* Define minimal validation in constructors or `init` blocks:
  * primary key field list must not be empty
  * index field list must not be empty
  * field names must not be blank
  * duplicate field names inside one definition should be rejected unless a later backend-specific reason appears
* Keep `fields` semantics unchanged.

Tests:

* Rename or replace `DocumentCollectionMetadataTest` with `CollectionMetadataTest` if appropriate.
* Cover equality and default values.
* Cover invalid primary key and index definitions.

Commit outcome:

* Core metadata can represent primary keys and secondary indices without changing backend behavior.

## Task 2: Add Destination Post-Load Metadata Hook (Done)

Scope:

* Add a default method to `DatabaseDestination`, for example:

```kotlin
suspend fun createIndexesAndConstraints(metadata: CollectionMetadata) = Unit
```

* Call this method in `Migrator.migrateCollection()` after `insertDocuments()`.
* Keep `createCollection()` responsible only for the collection/table and basic field schema.
* Make the default implementation a no-op so Redis and unsupported backends do not need immediate behavior.

Tests:

* Update `MigratorTest` to verify call order:
  * `createCollection(metadata)`
  * `insertDocuments(metadata.name, documents)`
  * `createIndexesAndConstraints(metadata)`
* Verify skipped collections do not call the hook.
* Verify empty sources do not call the hook.

Commit outcome:

* Migration flow supports delayed creation of primary keys and indices after data transfer, which is preferable for bulk insert performance.

## Task 3: Read PostgreSQL Primary Keys and Indices (Done)

Scope:

* Extend the shared `JdbcDialect` metadata path with default JDBC `DatabaseMetaData` helpers:
  * `readPrimaryKey(tableName, conn): PrimaryKeyDefinition?`
  * `readIndices(tableName, conn): List<IndexDefinition>`
* Use JDBC `DatabaseMetaData.getPrimaryKeys()` and `DatabaseMetaData.getIndexInfo()` first, avoiding database-specific catalog queries while driver metadata quality is sufficient.
* Exclude the primary-key backing index from `indices`.
* Preserve deterministic ordering by constraint/index name and column position.
* Extend `JdbcDocumentCollection.fetchMetadata()` to include both definitions.
* Keep SQLite primary-key and index metadata disabled until Task 5 because its JDBC driver does not preserve all required portable metadata such as descending index order.

Tests:

* Add a generic JDBC dialect integration contract using multiple JDBC drivers/connections as inputs.
* Extend the PostgreSQL Liquibase changelog with tables that have:
  * a single-column primary key
  * a composite primary key
  * a non-unique index
  * a unique index
  * a descending index column
* Use Postgres integration tests to verify the PostgreSQL JDBC driver reports the expected portable metadata.
* Assert fetched PostgreSQL collection metadata includes the expected definitions.

Commit outcome:

* PostgreSQL sources expose portable primary-key and index metadata.

## Task 4: Recreate PostgreSQL Primary Keys and Indices (Done)

Scope:

* Implement `PostgresDestination.createIndexesAndConstraints(metadata)`.
* Generate `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (...)` for `primaryKey`.
* Generate `CREATE [UNIQUE] INDEX ... ON ... (...)` for `indices`.
* Add safe identifier handling before generating DDL. Current code interpolates names directly; this task should either introduce a small PostgreSQL identifier renderer or intentionally document the existing limitation and keep the implementation consistent.
* Decide naming fallback when metadata names are absent:
  * primary key: `<table>_pkey`
  * index: `<table>_<field_names>_idx`
* Ensure the method does nothing when no definitions are present.

Tests:

* Extend `PostgresDestinationIT` to create a table, insert rows, then call `createIndexesAndConstraints()` and verify constraints/indexes in PostgreSQL catalogs.
* Cover duplicate data causing primary-key or unique-index creation failure.
* Cover metadata with no definitions.

Commit outcome:

* PostgreSQL destinations can recreate primary keys and secondary indices after bulk data transfer.

## Task 5: Read SQLite Primary Keys and Indices

Scope:

* Extend the shared JDBC metadata path where possible so SQL backends can expose primary keys and indices through `JdbcDialect`.
* Add SQLite-specific helpers in `SqliteDialect` or `Sqlite.kt`, for example:
  * `readPrimaryKey(tableName, connection): PrimaryKeyDefinition?`
  * `readIndices(tableName, connection): List<IndexDefinition>`
* Use SQLite PRAGMA metadata:
  * `PRAGMA table_info(<table>)` or `PRAGMA table_xinfo(<table>)` for primary-key column positions
  * `PRAGMA index_list(<table>)` for index names, uniqueness, origin, and partial flag
  * `PRAGMA index_xinfo(<index>)` for ordered indexed columns and sort direction
* Exclude SQLite auto-created primary-key and unique-constraint backing indexes from `indices` when they represent constraints rather than user-created secondary indexes.
* Skip unsupported index definitions with a warning:
  * expression index entries where `PRAGMA index_xinfo` has no column name
  * partial indexes unless the portable model is extended with predicates
  * implicit indexes created only for constraints
* Preserve deterministic ordering by primary-key column position, index name, and index column sequence.
* Extend `JdbcDocumentCollection.fetchMetadata()` or `SqliteDocumentCollection` so SQLite metadata includes the primary key and supported secondary indices.

Tests:

* Extend `SqliteDialectTest` or `SqliteTest` with metadata extraction cases for:
  * single-column primary key
  * composite primary key
  * non-unique index
  * unique index
  * descending index column
  * skipped expression or partial index behavior
* Extend `SqliteDocumentCollectionTest` or `SqliteSourceIT` to assert fetched collection metadata includes the expected definitions.

Commit outcome:

* SQLite sources expose portable primary-key and supported secondary-index metadata.

## Task 6: Recreate SQLite Primary Keys and Indices

Scope:

* Implement SQLite metadata recreation in `SqliteDestination` or the shared `JdbcDestination` through dialect hooks.
* Handle primary keys carefully: SQLite cannot add a primary key to an existing table with `ALTER TABLE` after rows are loaded. Therefore one of these decisions must be made explicitly:
  * create the primary key inline during `createCollection()` for SQLite, accepting possible insert-time overhead
  * do not recreate SQLite primary keys in the first implementation and document this limitation
  * rebuild the table after transfer, which is technically possible but higher risk and should be a separate task if chosen
* Recommended first implementation: create SQLite primary keys inline in `createCollection()` when `metadata.primaryKey` is present, and create secondary indices after `insertDocuments()` in `createIndexesAndConstraints()`.
* Generate `CREATE [UNIQUE] INDEX ... ON ... (...)` for supported `indices`.
* Add SQLite identifier rendering or reuse a shared JDBC identifier renderer if introduced for PostgreSQL.
* Ensure `createIndexesAndConstraints()` does nothing when no supported definitions are present.

Tests:

* Extend `SqliteDestinationIT` to create a table from metadata with a primary key and verify it through PRAGMA metadata.
* Insert rows first, then call `createIndexesAndConstraints()` and verify supported unique and non-unique indices.
* Cover duplicate data causing unique-index creation failure.
* Cover metadata with no definitions.
* Cover the chosen behavior for SQLite primary-key timing so future changes do not accidentally move it after the load.

Commit outcome:

* SQLite destinations can preserve primary keys according to the chosen SQLite-specific timing and can recreate supported secondary indices after data transfer.

## Task 7: Read Mongo Indices

Scope:

* Extend `MongoDocumentCollection.fetchMetadata()` to read `listIndexes()`.
* Map Mongo `_id_` index to `primaryKey` only if the transfer model intends to preserve `_id`. Current `readDocuments()` excludes `_id`, so the first implementation should not expose `_id_` as a primary key unless `_id` transfer behavior is changed in the same commit.
* Map other Mongo indexes to `IndexDefinition`.
* Preserve:
  * index name
  * ordered fields
  * ascending/descending direction
  * unique flag
* Ignore unsupported index shapes initially, such as text, hashed, wildcard, geo, partial, sparse, TTL, and collation-sensitive indexes, or skip them with a warning.

Tests:

* Extend `MongoDocumentCollectionIT` to create collections with unique and non-unique ascending/descending indexes and assert metadata.
* Add coverage that unsupported index types are skipped or reported according to the chosen behavior.

Commit outcome:

* Mongo sources expose portable secondary-index metadata without pretending Mongo `_id` is transferred when it is currently excluded.

## Task 8: Recreate Mongo Indices

Scope:

* Implement `MongoDestination.createIndexesAndConstraints(metadata)`.
* Create indexes with `Indexes.ascending`, `Indexes.descending`, or compound index definitions depending on `IndexDefinition.fields`.
* Apply `IndexOptions().name(name).unique(unique)` when values are present.
* Do not create primary keys for Mongo in the first implementation unless `_id` transfer support is added. Mongo always has `_id`; creating a relational-style primary key would be misleading.
* Keep `createCollection()` focused on validator creation.

Tests:

* Extend `MongoDestinationIT` to create a collection, insert data, call `createIndexesAndConstraints()`, and verify `listIndexes()`.
* Cover unique index creation failure with duplicate documents.
* Cover no definitions.

Commit outcome:

* Mongo destinations can recreate supported secondary indices after documents are inserted.

## Task 9: Add End-to-End Transfer Coverage

Scope:

* Extend integration datasets or liquibase changelogs under `softlab-data-transfer/src/integrationTest/resources` with source tables/collections that include primary keys and indices.
* Add focused integration tests for:
  * PostgreSQL to PostgreSQL transfer preserving primary keys and indices
  * SQLite to SQLite transfer preserving primary keys and supported indices
  * PostgreSQL to SQLite transfer preserving compatible primary keys and supported indices
  * SQLite to PostgreSQL transfer preserving compatible primary keys and supported indices
  * Mongo to Mongo transfer preserving supported indices
  * PostgreSQL to Mongo transfer mapping secondary indices where field names/types are compatible
  * Mongo to PostgreSQL transfer mapping supported indices, with primary-key behavior explicitly asserted as absent unless `_id` transfer is introduced
  * SQLite to Mongo transfer mapping secondary indices where field names/types are compatible
  * Mongo to SQLite transfer mapping supported indices, with primary-key behavior explicitly asserted as absent unless `_id` transfer is introduced
* Keep Redis assertions explicit: no primary-key/index recreation is expected unless Redis mapping format is extended later.

Tests:

* New or updated `MigratorIT` cases that verify post-transfer metadata on destination backends.

Commit outcome:

* The feature is proven through realistic transfers, not only individual adapter tests.

## Task 10: Update User Documentation

Scope:

* Update the `softlab-data-transfer` section of `README.md`.
* Replace the current limitation that says indexes and constraints are not migrated.
* Document the supported first version:
  * PostgreSQL primary keys
  * PostgreSQL secondary indices
  * SQLite primary keys, including the chosen creation timing/limitation
  * SQLite secondary indices
  * Mongo secondary indices
  * delayed metadata creation after document/row transfer
* Document unsupported cases:
  * foreign keys
  * check constraints
  * sequences
  * triggers
  * views
  * expression/partial/sparse/TTL/text/geo/hash indexes unless implemented
  * SQLite expression and partial indexes unless implemented
  * Mongo `_id` primary-key transfer unless explicitly added

Tests:

* No code tests required.
* Run markdown lint if the project has one; otherwise verify formatting manually.

Commit outcome:

* Users understand what metadata is preserved and what is intentionally out of scope.

## Task 11: Recalculate Coverage Thresholds

Scope:

* Run `./gradlew clean build`.
* Read `softlab-data-transfer/build/reports/jacoco/test/jacocoTestReport.xml`.
* Calculate the `INSTRUCTION` coverage rounded down to two decimal places.
* Update `jacocoVerification` in `softlab-data-transfer/build.gradle` with:
  * current instruction coverage as expected minimum
  * current missed class count from the `CLASS` counter

Tests:

* `./gradlew clean build`

Commit outcome:

* Build verification thresholds match the new tested implementation.
