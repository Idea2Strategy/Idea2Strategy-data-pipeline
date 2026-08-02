# Failure recovery runbook

The relational catalog is authoritative. Never infer official datasets from an S3
prefix listing and never delete an uploaded object merely because the following
database transaction failed.

## Schema ownership

This project applies no DDL and holds no Flyway history of its own. Schema changes
are contributed to the central `backend/db-migration` module through
`db/migration-contributions/` at the repository root. If a table appears to be
missing, the fix is a central migration — never a local `CREATE TABLE`.

## Interrupted run

1. Run `market-loader status --run-id <UUID>`.
2. Run `market-loader reconcile <manifest-UUID>` without `--repair` for each
   manifest the run produced.
3. Resolve credential, network, quota, or quality failures.
4. Run `market-loader resume <UUID> --config config.yaml --execute`.
5. Run `market-loader validate --manifest-id <manifest-UUID>`.

`resume` reuses the original idempotency key and accepts an existing object only
when its immutable key and SHA-256 match.

Partition-level skip-on-resume is **not** currently available: it was backed by
`market_data.pipeline_partitions`, which is not part of the canonical
`db/schema.dbml` and has been removed. A resumed run therefore re-derives and
re-publishes every partition of the run; publication remains idempotent on the
immutable object key plus SHA-256.

## S3 success followed by RDS failure

Keep the S3 version. It is an orphan candidate, not garbage. Reconciliation may
register it only after the exact key, Version ID, byte size, metadata SHA-256,
Parquet footer, schema, row count, period, shard, and owning `BUILDING` manifest
have all been verified. Current automated repair intentionally refuses ambiguous
or hash-mismatched objects.

## Hash, version, or missing-object incident

Do not select another revision as a fallback. Mark the manifest `QUARANTINED`,
preserve evidence, and create a new revision only after the cause is understood.
Never overwrite the existing key.

## Long-running states

Investigate `RUNNING` runs and `BUILDING` manifests against process logs, staging
files, and exact S3 object versions. A process exit leaves the run resumable. It
does not make partially published data `AVAILABLE`.
