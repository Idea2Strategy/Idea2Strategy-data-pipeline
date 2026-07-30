from __future__ import annotations

import os
from datetime import date

import psycopg
import pytest

from market_loader.database.repositories import MarketRepository
from market_loader.model.catalog import UniverseInstrument

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.getenv("MARKET_LOADER_INTEGRATION") != "1",
    reason="set MARKET_LOADER_INTEGRATION=1 with docker-compose.test.yaml running",
)
def test_flyway_schema_catalog_seed_and_run_idempotency() -> None:
    connection = psycopg.connect(
        "host=127.0.0.1 port=55432 dbname=idea2strategy_test "
        "user=market_loader password=market-loader-test"
    )
    try:
        with connection.transaction():
            repository = MarketRepository(connection)
            repository.assert_schema()
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT version FROM public.flyway_schema_history WHERE success = true"
                )
                assert cursor.fetchone() == ("001",)
            repository.seed_provider_and_feeds("test-rights-v1")
            instrument = UniverseInstrument(
                provider_symbol="AAPL",
                asset_type="STOCK",
                primary_exchange_mic="XNAS",
                effective_from=date(2020, 1, 1),
                effective_to=None,
                support_status="ACTIVE",
                instrument_id="11111111-1111-1111-1111-111111111111",
            )
            assert repository.seed_instrument(instrument) == repository.seed_instrument(instrument)
            run_id, reused = repository.create_run(
                idempotency_key="a" * 64,
                processing_version="test/1",
                input_config={
                    "symbol_count": 1,
                    "start": "2024-01-01",
                    "end": "2025-01-01",
                },
                partition_keys=["adjustment=raw/resolution=30m/year=2024/shard=00"],
            )
            assert not reused
            repository.complete_run(run_id, succeeded=True, summary={"rows": 0})
        with connection.transaction():
            same_run_id, reused = MarketRepository(connection).create_run(
                idempotency_key="a" * 64,
                processing_version="test/1",
                input_config={},
                partition_keys=[],
            )
            assert reused
            assert same_run_id == run_id
    finally:
        connection.close()

    restricted = psycopg.connect(
        "host=127.0.0.1 port=55432 dbname=idea2strategy_test "
        "user=market_loader password=market-loader-test"
    )
    try:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            with restricted.transaction(), restricted.cursor() as cursor:
                cursor.execute("CREATE TABLE market_data.forbidden_ddl (id integer)")
    finally:
        restricted.close()
