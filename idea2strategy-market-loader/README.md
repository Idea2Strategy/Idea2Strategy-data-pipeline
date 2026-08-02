# Idea2Strategy historical market loader

This directory is a new, standalone Python 3.12 project. It does not import or copy
the legacy code in the parent `market_hist_script` repository.

It collects Alpaca SIP 30-minute bars for explicit `[start, end)` date ranges,
filters them to XNYS regular sessions, derives 1-hour, 4-hour, and daily bars, writes
fixed-schema Parquet shards, uploads immutable versioned S3 objects, and registers
only verified manifests in PostgreSQL. RDS `AVAILABLE` manifests—not S3 listings—
are the authoritative data catalog.

## Install

Install `uv`, then create the locked environment:

```powershell
uv sync --frozen --all-groups
uv run market-loader --help
```

Copy `.env.example` to `.env` and set credentials locally. Copy
`config.example.yaml` to `config.yaml`. Neither file should contain long-lived AWS
access keys. AWS access uses the named CLI profile; private RDS access uses the SSM
port-forwarding script and `sslmode=verify-full`.

### Schema

This project owns **no** database migrations. The private Flyway history that used
to live in `db/migration/` was a second, competing history over `storage` and
`market_data` and has been removed (COM07). Its `db/test-init/` role bootstrap and
`docker-compose.test.yaml` Flyway container were removed with it.

Schema changes go through the central `backend/db-migration` module: add a
contribution under `db/migration-contributions/` at the repository root
(`V<YYYYMMDDHHMMSS>__<owner>_<slug>.sql`) and let the central assembly apply it.
The loader itself never applies DDL, and the canonical column definitions are
`db/schema.dbml`.

The database must already be provisioned with that central schema, and the login
role used by the loader must already exist with runtime privileges only.

## Safe workflow

All write commands are dry-run unless `--execute` is present.

```powershell
uv run market-loader doctor --config .\config.yaml

uv run market-loader plan `
  --config .\config.yaml `
  --universe .\universe.csv `
  --start 2024-01-01 --end 2025-01-01

uv run market-loader seed-catalog `
  --config .\config.yaml `
  --universe .\universe.csv

uv run market-loader seed-catalog `
  --config .\config.yaml `
  --universe .\universe.csv `
  --execute
```

The first write must be a sample of no more than five instruments and one year:

```powershell
uv run market-loader backfill `
  --config .\config.yaml `
  --universe .\universe.csv `
  --start 2024-01-01 --end 2025-01-01 `
  --max-symbols 5 `
  --execute
```

A larger write is blocked until that successful sample is recorded in RDS.
`PROVIDER_RIGHTS_APPROVED=true` and a non-empty
`PROVIDER_RIGHTS_VERSION` are also mandatory.

```powershell
uv run market-loader resume <run-uuid> --config .\config.yaml --execute
uv run market-loader validate --manifest-id <manifest-uuid>
uv run market-loader reconcile <manifest-uuid>
uv run market-loader status --run-id <run-uuid>
```

`validate` and `reconcile` address a single dataset manifest. The former
run-scoped variants resolved a run to its manifests through
`market_data.pipeline_partitions`, a table that does not exist in the canonical
`db/schema.dbml`; that dependency has been removed. `status` reports the
`market_data.pipeline_runs` row only — there is no canonical per-partition
progress table yet.

The application processes one calendar year at a time and splits API calls into
at most 180-day chunks and configured symbol batches. It never loads all ten years
into one table or DataFrame. Missing bars are reported; they are not synthesized,
interpolated, or forward-filled.

## Tests and static checks

```powershell
uv run pytest --cov=market_loader --cov-report=term-missing
uv run ruff check .
uv run ruff format --check .
uv run mypy
```

There is currently no database-backed test target in this project: the local
Flyway/PostgreSQL compose stack was removed together with the private migration.
A Testcontainers (PostgreSQL 16) integration test against the centrally assembled
schema is still to be added. Conditional-put, Version ID, encryption, and checksum
verification must be run against a dedicated development S3 bucket.

Operational recovery is documented in [RUNBOOK.md](RUNBOOK.md).
