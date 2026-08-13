# Pipeline migration contributions

This directory contributes pipeline-owned schema changes to the central Flyway bundle. It is not an independently executed Flyway project.

The launch schema was rebased on 2026-08-13. All existing `market_data` and `storage` structure and seed data are now part of the immutable central `V1__initial_schema.sql`, so `migrations/` intentionally starts empty. The vendored central fixture contains the same V1 for standalone integration tests.

Future changes use `V<UTC timestamp>__pipeline_<slug>.sql`, may mutate only the schemas declared in `contribution.properties`, and are assembled and executed centrally. Runtime Flyway in this service remains disabled.

The root `db/schema.dbml` is authoritative and must change in the same reviewed change as any schema migration.
