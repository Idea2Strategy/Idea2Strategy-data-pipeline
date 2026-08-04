from __future__ import annotations

import io
import json
import os
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_pipeline_lib.cli import build_parser
from market_pipeline_lib.runtime_export import (
    HistoricalSelection,
    RuntimeExportError,
    export_trading_instruments,
)

AAPL = "10000000-0000-4000-8000-000000000001"
MSFT = "10000000-0000-4000-8000-000000000002"
TSLA = "10000000-0000-4000-8000-000000000003"
FEED = "20000000-0000-4000-8000-000000000001"
M30 = "30000000-0000-4000-8000-000000000030"
H1 = "30000000-0000-4000-8000-000000000001"


class FakeCatalog:
    def __init__(self, rows: dict[str, list[dict[str, Any]]]) -> None:
        self.rows = deepcopy(rows)

    def records(self, table: str, *, where: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        rows = deepcopy(self.rows.get(table, []))
        if where:
            rows = [row for row in rows if all(row.get(key) == value for key, value in where.items())]
        return rows


class FakeS3:
    def __init__(self, payloads: dict[tuple[str, str], bytes]) -> None:
        self.payloads = payloads
        self.head_calls: list[dict[str, Any]] = []
        self.get_calls: list[dict[str, Any]] = []

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        self.head_calls.append(kwargs)
        payload = self.payloads[(kwargs["Key"], kwargs["VersionId"])]
        import hashlib

        return {
            "VersionId": kwargs["VersionId"],
            "ContentLength": len(payload),
            "ServerSideEncryption": "AES256",
            "Metadata": {"content-sha256": hashlib.sha256(payload).hexdigest()},
        }

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.get_calls.append(kwargs)
        return {
            "VersionId": kwargs["VersionId"],
            "Body": io.BytesIO(self.payloads[(kwargs["Key"], kwargs["VersionId"])]),
        }


def parquet_bytes(instruments: list[str]) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(
        pa.table(
            {
                "instrument_id": instruments,
                "session_date_et": pa.array([date(2024, 2, index + 1) for index in range(len(instruments))]),
            }
        ),
        sink,
    )
    return sink.getvalue().to_pybytes()


def fixture() -> tuple[FakeCatalog, FakeS3]:
    payloads = {
        ("30m.parquet", "v30"): parquet_bytes([AAPL, MSFT]),
        ("1h.parquet", "v1h"): parquet_bytes([MSFT, TSLA]),
    }
    import hashlib

    storage = []
    relations = []
    for index, (manifest_id, key, version) in enumerate(
        ((M30, "30m.parquet", "v30"), (H1, "1h.parquet", "v1h")), 1
    ):
        payload = payloads[(key, version)]
        object_id = f"40000000-0000-4000-8000-{index:012d}"
        storage.append(
            {
                "id": object_id,
                "status": "AVAILABLE",
                "storage_provider": "S3_COMPATIBLE",
                "bucket_name": "market-data",
                "object_key": key,
                "provider_version_id": version,
                "content_hash": hashlib.sha256(payload).hexdigest(),
                "byte_size": len(payload),
            }
        )
        relations.append(
            {
                "id": f"50000000-0000-4000-8000-{index:012d}",
                "dataset_manifest_id": manifest_id,
                "object_id": object_id,
            }
        )
    manifests = [
        {
            "id": M30,
            "feed_id": FEED,
            "data_layer": "RAW",
            "resolution": "30m",
            "revision_number": 1,
            "status": "AVAILABLE",
            "period_start": "2024-01-01T05:00:00Z",
            "period_end": "2025-01-01T05:00:00Z",
            "dataset_hash": "a" * 64,
        },
        {
            "id": H1,
            "feed_id": FEED,
            "data_layer": "DERIVED",
            "resolution": "1h",
            "revision_number": 1,
            "status": "AVAILABLE",
            "period_start": "2024-01-01T05:00:00Z",
            "period_end": "2025-01-01T05:00:00Z",
            "dataset_hash": "b" * 64,
        },
    ]
    instruments = [
        {
            "id": instrument_id,
            "primary_exchange_mic": "XNAS",
            "listed_at": "2000-01-01",
            "delisted_at": None,
        }
        for instrument_id in (AAPL, MSFT, TSLA)
    ]
    symbols = [
        {
            "id": f"60000000-0000-4000-8000-{index:012d}",
            "instrument_id": instrument_id,
            "exchange_mic": "XNAS",
            "symbol": symbol,
            "effective_from": "2000-01-01T00:00:00Z",
            "effective_to": None,
        }
        for index, (instrument_id, symbol) in enumerate(
            ((AAPL, "AAPL"), (MSFT, "MSFT"), (TSLA, "TSLA")), 1
        )
    ]
    catalog = FakeCatalog(
        {
            "market_data.feeds": [
                {"id": FEED, "code": "ALPACA_SIP_RAW_30M", "resolution": "30m"}
            ],
            "market_data.dataset_manifests": manifests,
            "market_data.dataset_objects": relations,
            "storage.objects": storage,
            "market_data.instruments": instruments,
            "market_data.instrument_symbols": symbols,
        }
    )
    return catalog, FakeS3(payloads)


def selection() -> HistoricalSelection:
    return HistoricalSelection(
        layer_by_resolution={"30m": "RAW", "1h": "DERIVED"},
        adjustment="raw",
        start=date(2024, 1, 1),
        end_exclusive=date(2025, 1, 1),
        latest_revision_policy="latest-per-period",
        symbol_effective_cutoff=datetime(2024, 12, 31, tzinfo=UTC),
    )


def test_export_scans_exact_versions_and_writes_sorted_canonical_mapping(tmp_path: Path) -> None:
    catalog, s3 = fixture()
    output = tmp_path / "runtime" / "trading" / "instruments.json"
    evidence = tmp_path / "runtime" / "trading" / "instruments.evidence.json"

    report = export_trading_instruments(
        catalog,
        s3,
        selection(),
        output=output,
        evidence_output=evidence,
        expected_bucket="market-data",
        execute=True,
    )

    assert output.read_bytes() == ('{"MSFT":"' + MSFT + '"}\n').encode()
    assert report["instrument_count"] == 1
    assert report["mapping_sha256"]
    assert json.loads(evidence.read_text())["source_objects"][0]["provider_version_id"]
    assert all(call.get("VersionId") for call in s3.head_calls + s3.get_calls)
    assert {call["VersionId"] for call in s3.get_calls} == {"v30", "v1h"}


def test_export_dry_run_does_not_write_but_returns_the_exact_hash(tmp_path: Path) -> None:
    catalog, s3 = fixture()
    output = tmp_path / "instruments.json"
    evidence = tmp_path / "evidence.json"

    report = export_trading_instruments(
        catalog,
        s3,
        selection(),
        output=output,
        evidence_output=evidence,
        expected_bucket="market-data",
        execute=False,
    )

    assert report["status"] == "DRY_RUN"
    assert not output.exists()
    assert not evidence.exists()


def test_export_fails_closed_when_a_catalog_receipt_has_no_version(tmp_path: Path) -> None:
    catalog, s3 = fixture()
    catalog.rows["storage.objects"][0]["provider_version_id"] = ""

    with pytest.raises(RuntimeExportError, match="version"):
        export_trading_instruments(
            catalog,
            s3,
            selection(),
            output=tmp_path / "instruments.json",
            evidence_output=tmp_path / "evidence.json",
            expected_bucket="market-data",
            execute=True,
        )

    assert not s3.get_calls


def test_export_fails_closed_when_get_does_not_return_the_pinned_version(
    tmp_path: Path,
) -> None:
    catalog, s3 = fixture()
    original = s3.get_object

    def wrong_version(**kwargs: Any) -> dict[str, Any]:
        response = original(**kwargs)
        response["VersionId"] = "different"
        return response

    s3.get_object = wrong_version  # type: ignore[method-assign]

    with pytest.raises(RuntimeExportError, match="different version"):
        export_trading_instruments(
            catalog,
            s3,
            selection(),
            output=tmp_path / "instruments.json",
            evidence_output=tmp_path / "evidence.json",
            expected_bucket="market-data",
            execute=True,
        )


def test_export_rejects_an_existing_different_runtime_mapping(tmp_path: Path) -> None:
    catalog, s3 = fixture()
    output = tmp_path / "instruments.json"
    output.write_text('{"AAPL":"wrong"}\n')

    with pytest.raises(RuntimeExportError, match="already exists"):
        export_trading_instruments(
            catalog,
            s3,
            selection(),
            output=output,
            evidence_output=tmp_path / "evidence.json",
            expected_bucket="market-data",
            execute=True,
        )


def test_instrument_cli_has_no_semantic_selection_defaults() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["export-trading-instruments", "--artifact-root", "evidence"])

    args = parser.parse_args(
        [
            "export-trading-instruments",
            "--artifact-root",
            "evidence",
            "--bucket",
            "market-data",
            "--adjustment",
            "raw",
            "--resolution",
            "30m",
            "--layer",
            "30m=RAW",
            "--start-year",
            "2024",
            "--end-year",
            "2024",
            "--latest-revision-policy",
            "latest-per-period",
            "--symbol-effective-cutoff",
            "2024-12-31T21:00:00Z",
            "--output",
            "instruments.json",
            "--evidence-output",
            "instruments.evidence.json",
        ]
    )
    assert args.adjustment == "raw"
    assert args.latest_revision_policy == "latest-per-period"


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("LOCALSTACK_ENDPOINT_URL"),
    reason="set LOCALSTACK_ENDPOINT_URL to run the LocalStack runtime export integration test",
)
def test_localstack_export_reads_the_catalog_version_not_the_latest_object(
    tmp_path: Path,
) -> None:
    import hashlib
    import uuid

    import boto3

    endpoint = os.environ["LOCALSTACK_ENDPOINT_URL"]
    client = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")
    bucket = f"i2s-runtime-export-{uuid.uuid4()}"
    client.create_bucket(Bucket=bucket)
    client.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )
    catalog, fake = fixture()
    for receipt in catalog.rows["storage.objects"]:
        key = receipt["object_key"]
        payload = next(value for (candidate, _), value in fake.payloads.items() if candidate == key)
        digest = hashlib.sha256(payload).hexdigest()
        response = client.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            ServerSideEncryption="AES256",
            Metadata={"content-sha256": digest},
        )
        receipt.update(
            {
                "bucket_name": bucket,
                "provider_version_id": response["VersionId"],
                "content_hash": digest,
                "byte_size": len(payload),
            }
        )
        # The unpinned latest object is deliberately corrupt. The exporter must
        # still read the exact immutable version named by the canonical receipt.
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=b"not-parquet",
            ServerSideEncryption="AES256",
            Metadata={"content-sha256": hashlib.sha256(b"not-parquet").hexdigest()},
        )

    report = export_trading_instruments(
        catalog,
        client,
        selection(),
        output=tmp_path / "instruments.json",
        evidence_output=tmp_path / "evidence.json",
        expected_bucket=bucket,
        execute=False,
    )

    assert report["instrument_count"] == 1
