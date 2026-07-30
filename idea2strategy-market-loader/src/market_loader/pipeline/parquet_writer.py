from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq

from market_loader.errors import QualityError
from market_loader.model.bar import Bar


def bar_schema(*, derived: bool) -> pa.Schema:
    fields = [
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("provider_symbol", pa.string(), nullable=False),
        pa.field("bar_start_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("session_date_et", pa.date32(), nullable=False),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("volume", pa.int64(), nullable=False),
        pa.field("trade_count", pa.int64(), nullable=True),
        pa.field("vwap", pa.float64(), nullable=True),
    ]
    if derived:
        fields.extend(
            [
                pa.field("source_bar_count", pa.int16(), nullable=False),
                pa.field("source_minutes", pa.int16(), nullable=False),
            ]
        )
    return pa.schema(fields)


@dataclass(frozen=True, slots=True)
class WrittenParquet:
    path: Path
    row_count: int
    byte_size: int
    content_sha256: str
    min_bar_start_at: datetime | None
    max_bar_start_at: datetime | None


def _columns(bars: list[Bar], derived: bool) -> dict[str, list[object]]:
    names = [field.name for field in bar_schema(derived=derived)]
    result: dict[str, list[object]] = {name: [] for name in names}
    for bar in bars:
        for name in names:
            result[name].append(getattr(bar, name))
    return result


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_parquet(
    *,
    bars: list[Bar],
    output_path: Path,
    derived: bool,
    schema_version: str,
    processing_version: str,
    adjustment: str,
    resolution: str,
    period_start: date,
    period_end: date,
    revision: int,
    manifest_id: UUID,
    compression: str = "zstd",
    compression_level: int = 3,
    row_group_size: int = 131_072,
) -> WrittenParquet:
    ordered = sorted(bars, key=lambda item: (item.instrument_id, item.bar_start_at))
    schema = bar_schema(derived=derived)
    metadata = {
        "schema_version": schema_version,
        "processing_version": processing_version,
        "provider": "alpaca",
        "feed": "sip",
        "adjustment": adjustment,
        "session_scope": "regular",
        "resolution": resolution,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "revision": str(revision),
        "manifest_id": str(manifest_id),
        "created_at": datetime.now(UTC).isoformat(),
    }
    schema = schema.with_metadata({key.encode(): value.encode() for key, value in metadata.items()})
    table = pa.Table.from_pydict(_columns(ordered, derived), schema=schema)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    pq.write_table(
        table,
        temporary,
        compression=compression,
        compression_level=compression_level,
        row_group_size=row_group_size,
        use_dictionary=["provider_symbol"],
        version="2.6",
    )
    parquet_file = pq.ParquetFile(temporary)
    if parquet_file.metadata.num_rows != len(ordered):
        temporary.unlink(missing_ok=True)
        raise QualityError("Parquet footer row count mismatch")
    actual_schema = parquet_file.schema_arrow.remove_metadata()
    if not actual_schema.equals(bar_schema(derived=derived), check_metadata=False):
        parquet_file.close()
        temporary.unlink(missing_ok=True)
        raise QualityError("Parquet footer schema mismatch")
    parquet_file.close()
    os.replace(temporary, output_path)
    timestamps = [bar.bar_start_at for bar in ordered]
    return WrittenParquet(
        path=output_path,
        row_count=len(ordered),
        byte_size=output_path.stat().st_size,
        content_sha256=sha256_file(output_path),
        min_bar_start_at=min(timestamps) if timestamps else None,
        max_bar_start_at=max(timestamps) if timestamps else None,
    )


def validate_parquet(path: Path, *, derived: bool, expected_sha256: str | None = None) -> int:
    try:
        parquet_file = pq.ParquetFile(path)
    except Exception as exc:
        raise QualityError("cannot read Parquet footer") from exc
    if not parquet_file.schema_arrow.remove_metadata().equals(
        bar_schema(derived=derived), check_metadata=False
    ):
        raise QualityError("Parquet schema mismatch")
    if expected_sha256 and sha256_file(path) != expected_sha256:
        raise QualityError("Parquet SHA-256 mismatch")
    return int(parquet_file.metadata.num_rows)
