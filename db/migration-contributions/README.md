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
| `schemas` | `market_data,storage` | Every schema this contribution touches. Root #139 assigns both to D. |
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

For this repository both declared schemas are D-owned and mutable:
`{market_data, storage}`.

## What may go in `migrations/`

Only DDL that mutates D-owned `market_data` or `storage`. Nothing else. In particular
`market_data.pipeline_partitions` must never appear: it is absent from the
canonical `db/schema.dbml` and spec section 2.4 forbids adding it.

The directory contains additive migrations accepted by the central assembler. An
applied migration is immutable; change it with a later migration, never in place.

## `storage` schema ownership

Root #139 resolved the contradiction in favor of the implementation checklist: D owns
`storage`. D produces the immutable objects and their registrations; other bundles read
their references. The validator therefore accepts D-owned migrations for both declared
schemas. Runtime roles and GRANTs are assembled centrally as a separate reviewed unit.

## Local validation

`validate_contribution.py` mirrors `MigrationContribution`, `MigrationOwner` and
`DatabaseAccessPolicy` so a defect fails here rather than at central assembly.

```bash
python db/migration-contributions/validate_contribution.py
```

It is covered by `tests/test_migration_contribution.py` and runs as its own CI
gate (`migration-contribution`).
