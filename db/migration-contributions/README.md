# Pipeline migration contributions

This directory contributes pipeline-owned schema changes to the central Flyway bundle. It is not an independently executed Flyway project.

The launch schema was rebased on 2026-08-13. All existing `market_data` and `storage` structure and seed data are part of the immutable central `V1__initial_schema.sql`, so `migrations/` intentionally starts empty. The vendored central fixture contains the complete current central bundle byte-for-byte for standalone integration tests; its checksum file must be refreshed whenever the central bundle advances.

Future changes use `V<UTC timestamp>__pipeline_<slug>.sql`, may mutate only the schemas declared in `contribution.properties`, and are assembled and executed centrally. Runtime Flyway in this service remains disabled.

The root `db/schema.dbml` is authoritative and must change in the same reviewed change as any schema migration.
