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

Apply `db/migration/V001__market_data_initial_schema.sql` through the deployment
Flyway process before executing any write command. The `market_loader` login role
must already exist; V001 grants it only the runtime schema and table privileges it
needs. Run Flyway with the deployment or master account, never with
`market_loader`. The loader never applies DDL.

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
uv run market-loader validate --run-id <run-uuid>
uv run market-loader reconcile <run-uuid>
uv run market-loader status --run-id <run-uuid>
```

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

`docker-compose.test.yaml` provides PostgreSQL and a fast S3-compatible test
service. Final conditional-put, Version ID, encryption, and checksum verification
must still be run against a dedicated development S3 bucket.

Operational recovery is documented in [RUNBOOK.md](RUNBOOK.md). The Spring/worker
consumer query is in `db/queries/available_manifest.sql`.
