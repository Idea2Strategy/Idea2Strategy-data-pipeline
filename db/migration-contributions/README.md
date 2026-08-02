# Pipeline migration contribution root (COM07)

This directory is `data-pipeline`'s **contribution root**. It is not a Flyway
project. The canonical schema lives in the root `db/schema.dbml`, the executable
baseline lives in `backend/db-migration` (`V1__initial_schema.sql`, owned by A),
and the central assembler folds every owner repository's contribution into one
ordered bundle. This repository never runs Flyway itself.

## Contract

`contribution.properties`:

| Key | Value | Why |
|---|---|---|
| `contract.version` | `1` | The only version the central assembler supports. |
| `owner` | `pipeline` | One of the `MigrationOwner` tokens: `backend`, `trading`, `backtest`, `pipeline`, `shared`. |
| `schemas` | `market_data,storage` | Every schema this contribution touches (spec section 2.4). `market_data` is owned; `storage` is SHARED and therefore declared but **not mutable** — see below. |
| `migrations.directory` | `migrations` | The only directory that enters the central bundle. |
| `fixtures.directory` | `fixtures` | Test data and legacy standalone SQL. Never enters the bundle. |
| `filename.regex` | `^V[0-9]{14}__pipeline_[a-z0-9]+(?:_[a-z0-9]+)*[.]sql$` | Must be at least as strict as the central rule. |
| `runtime.flyway.enabled` | `false` | Owner applications must not claim migration execution. |

## Filename rule

```
V<YYYYMMDDHHMMSS>__pipeline_<slug>.sql
```

- The 14-digit version is a **UTC timestamp** and is globally unique across all
  owners. Legacy `V001`-style numbering is rejected by the central assembler and
  by `validate_contribution.py`.
- `<slug>` is lowercase `a-z0-9` words separated by single underscores.
- Example: `V20260802120000__pipeline_add_stream_watermark_index.sql`.

## Declared scope vs. mutable scope

`schemas=` names every schema the contribution *touches*. It does not grant the
right to change them. `validate_contribution.py` computes `mutable_schemas` as
the declared schemas whose `DatabaseAccessPolicy` owner is `pipeline`, and
`check_migration_content` rejects any DDL statement in `migrations/` that targets
a schema outside that set — with the same message the central assembler uses.

For this repository: declared = `{market_data, storage}`, mutable =
`{market_data}`.

## What may go in `migrations/`

Only DDL that mutates `market_data`. Nothing else. In particular
`market_data.pipeline_partitions` must never appear: it is absent from the
canonical `db/schema.dbml` and spec section 2.4 forbids adding it.

**`migrations/` is intentionally empty right now.** Every `market_data` table
this bundle needs already exists in the central `V1__initial_schema.sql`. DP1
creates the mechanism; DP2 onwards adds SQL only when a canonical DBML change
justifies it. An applied migration is immutable — change it with a later
migration, never in place.

## Open item: `storage` schema ownership is contradictory

`docs/backend-implementation-master-checklist.md` lists `storage` among D's
primary schemas, but
`backend/db-migration/src/main/java/com/idea2strategy/backend/migration/DatabaseAccessPolicy.java:36`
registers `storage` to `MigrationOwner.SHARED`. Under the central rule, a
`pipeline`-owned migration that touches `storage` is rejected:

> `Migration owner pipeline cannot mutate storage.<table>; registered owner is shared`

Consequences held to deliberately, until A resolves the contradiction:

1. `schemas=` declares `storage`, because the pipeline genuinely writes rows to
   `storage.objects` — `market_data.dataset_objects.object_id` is a NOT NULL
   foreign key to it, so the contribution cannot honestly claim otherwise.
   Declaring it is not claiming ownership of it: `mutable_schemas` excludes it.
2. This repository authors **no** `storage` DDL, and
   `check_migration_content` fails the build if anyone tries. It uses the
   existing V1 definition of `storage.objects` as-is.
3. Runtime access is unaffected: `DatabaseAccessPolicy.allowsPipeline` already
   grants the pipeline role `READ` and `INSERT` on `storage.objects`, which is
   all the object-registration path needs. In application code the same choice
   is named explicitly by
   `market_pipeline_lib.catalog.StorageObjectsPolicy`.

A separate ownership-correction issue is required against `backend/db-migration`
to make the checklist and `DatabaseAccessPolicy` agree. Do not work around it by
claiming the schema here.

Related, unresolved: `DatabaseAccessPolicy` is a compile-time/unit-test helper.
`V1__initial_schema.sql` contains no role `GRANT`s, so nothing enforces these
rules at runtime today.

## Local validation

`validate_contribution.py` mirrors `MigrationContribution`, `MigrationOwner` and
`DatabaseAccessPolicy` so a defect fails here rather than at central assembly.

```bash
python db/migration-contributions/validate_contribution.py
```

It is covered by `tests/test_migration_contribution.py` and runs as its own CI
gate (`migration-contribution`).
