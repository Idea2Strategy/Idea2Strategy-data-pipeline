"""D11 -- stream watermark tracking over `market_data.stream_watermarks`.

Until DP5 the table existed in the canonical schema and in
:mod:`market_pipeline_lib.db.tables` and nothing read or wrote it.  This module
is the reader and the writer.

What a watermark means here
---------------------------
A realtime feed is consumed as several **shards** (``stable_shard_key`` puts an
instrument on exactly one of them, deterministically).  Each shard has its own
head, and an arriving message is judged against *its own* shard head:

======================  ==============================================================
outcome                 meaning
======================  ==============================================================
``ADVANCED``            strictly newer than this shard's head; process it
``DUPLICATE``           exactly the head; an at-least-once redelivery, skip it
``STALE``               older than the head; an out-of-order or replayed message
======================  ==============================================================

The head never moves backwards, so a redelivery storm or a provider replaying an
hour of history cannot rewind ingestion.

What the persisted row means
----------------------------
``market_data.stream_watermarks`` is keyed by ``feed_id`` alone -- there is no
shard column -- so a per-shard row is not expressible against the applied
baseline, and this repository does not author DDL (see `db/tables.py`).  The one
row per feed therefore records the **completion floor**: the position every
declared shard has passed.

That is the value a resume needs.  Seeding every shard from the floor after a
crash replays the window that was in flight (absorbed by idempotent processing)
and skips nothing that came after it -- whereas seeding from the newest position
seen on the fastest shard would silently drop whatever the slowest shard had not
reached.  It is also the value the pre-evaluation gate described in the table's
own ``COMMENT`` actually wants: "market data is complete up to T", not "one shard
happened to be fresh".  ``last_ingested_at`` carries process liveness, which is
the other half of that question.

The floor is only defined once every declared shard has a head, and the declared
shard set is required rather than discovered: a shard that first appears *after*
a resume would otherwise be seeded from a floor it never contributed to, and its
earlier messages would be discarded as ``STALE``.  Consequently, an undeclared
shard key is an error, never an implicit registration.

DP-a note
---------
Splitting the row per shard needs ``(feed_id, shard_key)`` as the primary key,
which is a central migration.  Until that exists the floor is the honest
projection of per-shard state onto a per-feed row.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import Engine, and_, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .db.codec import from_db_row, to_db_params
from .db.tables import MARKET_DATA_SCHEMA, stream_watermarks

__all__ = [
    "InMemoryWatermarkRepository",
    "SqlWatermarkRepository",
    "StreamPosition",
    "StreamWatermark",
    "UnknownShardError",
    "WatermarkDecision",
    "WatermarkError",
    "WatermarkLedger",
    "WatermarkOutcome",
    "WatermarkRepository",
]

STREAM_WATERMARKS_TABLE = f"{MARKET_DATA_SCHEMA}.{stream_watermarks.name}"

#: Sorts before every real sequence, so "no sequence" is the earliest position at
#: an instant rather than an incomparable one.
_NO_SEQUENCE = -(2**63)


class WatermarkError(ValueError):
    """A watermark could not be interpreted or applied."""


class UnknownShardError(WatermarkError):
    """A message arrived for a shard the ledger was not told about."""


@dataclass(frozen=True, order=False)
class StreamPosition:
    """Where a shard has got to: the source clock, tie-broken by sequence."""

    source_event_at: datetime
    sequence: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.source_event_at, datetime):
            raise WatermarkError(f"source_event_at must be a datetime, got {type(self.source_event_at).__name__}")
        if self.source_event_at.tzinfo is None or self.source_event_at.utcoffset() is None:
            raise WatermarkError(
                "source_event_at has no timezone; this pipeline works in ET and UTC at "
                "once and a naive instant is never assumed to be UTC"
            )
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, (int, type(None))):
            raise WatermarkError(f"sequence must be an int or None, got {type(self.sequence).__name__}")
        object.__setattr__(self, "source_event_at", self.source_event_at.astimezone(UTC))

    @property
    def order_key(self) -> tuple[datetime, int]:
        return self.source_event_at, _NO_SEQUENCE if self.sequence is None else self.sequence

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, StreamPosition):
            return NotImplemented
        return self.order_key < other.order_key

    def __le__(self, other: object) -> bool:
        if not isinstance(other, StreamPosition):
            return NotImplemented
        return self.order_key <= other.order_key

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, StreamPosition):
            return NotImplemented
        return self.order_key > other.order_key

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, StreamPosition):
            return NotImplemented
        return self.order_key >= other.order_key

    def isoformat(self) -> str:
        return self.source_event_at.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class StreamWatermark:
    """One durable `market_data.stream_watermarks` row, in domain terms."""

    feed_id: str
    position: StreamPosition
    ingested_at: datetime
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.feed_id:
            raise WatermarkError("feed_id must not be empty")
        for name in ("ingested_at", "updated_at"):
            value = getattr(self, name)
            if value is None:
                continue
            if value.tzinfo is None or value.utcoffset() is None:
                raise WatermarkError(f"{name} must be timezone-aware")
            object.__setattr__(self, name, value.astimezone(UTC))

    def as_record(self) -> dict[str, Any]:
        """The canonical `market_data.stream_watermarks` record."""

        updated_at = self.updated_at or self.ingested_at
        return {
            "feed_id": self.feed_id,
            "last_source_event_at": self.position.source_event_at,
            "last_ingested_at": self.ingested_at,
            "last_sequence": self.position.sequence,
            "updated_at": updated_at,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> StreamWatermark:
        return cls(
            feed_id=str(record["feed_id"]),
            position=StreamPosition(
                source_event_at=_as_datetime(record["last_source_event_at"], "last_source_event_at"),
                sequence=None if record["last_sequence"] is None else int(record["last_sequence"]),
            ),
            ingested_at=_as_datetime(record["last_ingested_at"], "last_ingested_at"),
            updated_at=_as_datetime(record["updated_at"], "updated_at"),
        )


def _as_datetime(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = f"{text[:-1]}+00:00"
        moment = datetime.fromisoformat(text)
    else:
        raise WatermarkError(f"{label} must be a datetime or ISO-8601 string, got {type(value).__name__}")
    if moment.tzinfo is None:
        raise WatermarkError(f"{label} must be timezone-aware")
    return moment.astimezone(UTC)


class WatermarkOutcome(StrEnum):
    ADVANCED = "ADVANCED"
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"


@dataclass(frozen=True)
class WatermarkDecision:
    """What the ledger decided about one arriving message."""

    shard_key: str
    outcome: WatermarkOutcome
    position: StreamPosition
    head: StreamPosition

    @property
    def should_process(self) -> bool:
        return self.outcome is WatermarkOutcome.ADVANCED


@runtime_checkable
class WatermarkRepository(Protocol):
    """Durable, advance-only storage for one watermark per feed."""

    def load(self, feed_id: str) -> StreamWatermark | None:
        """The stored watermark, or `None` when the feed has never been ingested."""

    def advance(self, watermark: StreamWatermark) -> StreamWatermark:
        """Store `watermark` if it is newer, then return whatever is now stored.

        Never regresses: an older or equal write leaves the stored value alone and
        is reported by returning it.
        """


class InMemoryWatermarkRepository:
    """Process-local repository with exactly the SQL repository's semantics."""

    def __init__(self, initial: Iterable[StreamWatermark] = ()) -> None:
        self._rows: dict[str, StreamWatermark] = {}
        for watermark in initial:
            self._rows[watermark.feed_id] = watermark

    def load(self, feed_id: str) -> StreamWatermark | None:
        return self._rows.get(feed_id)

    def advance(self, watermark: StreamWatermark) -> StreamWatermark:
        stored = self._rows.get(watermark.feed_id)
        if stored is not None and watermark.position <= stored.position:
            return stored
        effective = replace(watermark, updated_at=watermark.updated_at or watermark.ingested_at)
        self._rows[watermark.feed_id] = effective
        return effective


class SqlWatermarkRepository:
    """`market_data.stream_watermarks` over SQLAlchemy Core.

    The advance-only rule lives in the SQL, not in a read-then-write in Python:
    the `ON CONFLICT DO UPDATE` carries a `WHERE` that only fires for a strictly
    newer position, so two workers racing on the same feed cannot interleave into
    a regression.  A skipped update returns no row, and the current row is read
    back and returned unchanged.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @property
    def engine(self) -> Engine:
        return self._engine

    def load(self, feed_id: str) -> StreamWatermark | None:
        statement = select(stream_watermarks).where(
            stream_watermarks.c.feed_id == to_db_params(STREAM_WATERMARKS_TABLE, {"feed_id": feed_id})["feed_id"]
        )
        with self._engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
        if row is None:
            return None
        return StreamWatermark.from_record(from_db_row(STREAM_WATERMARKS_TABLE, row))

    def advance(self, watermark: StreamWatermark) -> StreamWatermark:
        params = to_db_params(STREAM_WATERMARKS_TABLE, watermark.as_record())
        statement = pg_insert(stream_watermarks).values(**params)
        excluded = statement.excluded
        current_sequence = func.coalesce(stream_watermarks.c.last_sequence, _NO_SEQUENCE)
        incoming_sequence = func.coalesce(excluded.last_sequence, _NO_SEQUENCE)
        statement = statement.on_conflict_do_update(
            index_elements=[stream_watermarks.c.feed_id],
            set_={
                "last_source_event_at": excluded.last_source_event_at,
                "last_ingested_at": excluded.last_ingested_at,
                "last_sequence": excluded.last_sequence,
                "updated_at": excluded.updated_at,
            },
            where=or_(
                stream_watermarks.c.last_source_event_at < excluded.last_source_event_at,
                and_(
                    stream_watermarks.c.last_source_event_at == excluded.last_source_event_at,
                    current_sequence < incoming_sequence,
                ),
            ),
        ).returning(*stream_watermarks.c)

        with self._engine.begin() as connection:
            row = connection.execute(statement).mappings().first()
            if row is None:
                # The advance-only guard rejected it; report what is actually stored.
                row = (
                    connection.execute(
                        select(stream_watermarks).where(
                            stream_watermarks.c.feed_id == params["feed_id"]
                        )
                    )
                    .mappings()
                    .first()
                )
            if row is None:  # pragma: no cover - only reachable if the row vanished mid-transaction
                raise WatermarkError(
                    f"stream_watermarks row for feed {watermark.feed_id} was neither written nor found"
                )
            record = from_db_row(STREAM_WATERMARKS_TABLE, row)
        return StreamWatermark.from_record(record)


@dataclass
class WatermarkLedger:
    """Per-shard heads for one feed, projected onto one durable row.

    `repository` is optional so the classification logic can be exercised without
    storage; `checkpoint` is a no-op without one, and says so by returning `None`.
    """

    feed_id: str
    shard_keys: Sequence[str]
    repository: WatermarkRepository | None = None
    resumed_from: StreamPosition | None = None
    _heads: dict[str, StreamPosition | None] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        keys = tuple(self.shard_keys)
        if not keys:
            raise WatermarkError("at least one shard key must be declared")
        if len(set(keys)) != len(keys):
            raise WatermarkError(f"shard keys must be unique, got {list(keys)}")
        self.shard_keys = keys
        self._heads = dict.fromkeys(keys, self.resumed_from)

    @classmethod
    def resume(
        cls,
        repository: WatermarkRepository,
        *,
        feed_id: str,
        shard_keys: Sequence[str],
    ) -> WatermarkLedger:
        """Rebuild a ledger from the persisted row alone -- the crash-restart path."""

        stored = repository.load(feed_id)
        return cls(
            feed_id=feed_id,
            shard_keys=shard_keys,
            repository=repository,
            resumed_from=None if stored is None else stored.position,
        )

    # -- classification ---------------------------------------------------------
    def observe(self, shard_key: str, position: StreamPosition) -> WatermarkDecision:
        """Classify one arriving message and, when it is new, move the shard head."""

        if shard_key not in self._heads:
            raise UnknownShardError(
                f"{shard_key!r} is not a declared shard of feed {self.feed_id}; "
                f"declared shards are {list(self.shard_keys)}. Registering it now would "
                "seed it from a floor it never contributed to and discard its history."
            )
        head = self._heads[shard_key]
        if head is None:
            outcome = WatermarkOutcome.ADVANCED
        elif position > head:
            outcome = WatermarkOutcome.ADVANCED
        elif position == head:
            outcome = WatermarkOutcome.DUPLICATE
        else:
            outcome = WatermarkOutcome.STALE
        if outcome is WatermarkOutcome.ADVANCED:
            self._heads[shard_key] = position
        return WatermarkDecision(
            shard_key=shard_key,
            outcome=outcome,
            position=position,
            head=self._heads[shard_key] or position,
        )

    def head(self, shard_key: str) -> StreamPosition | None:
        if shard_key not in self._heads:
            raise UnknownShardError(f"{shard_key!r} is not a declared shard of feed {self.feed_id}")
        return self._heads[shard_key]

    def heads(self) -> dict[str, StreamPosition | None]:
        return dict(self._heads)

    def completion_floor(self) -> StreamPosition | None:
        """The position every declared shard has passed, or `None` if not all have."""

        positions = [self._heads[key] for key in self.shard_keys]
        if any(value is None for value in positions):
            return self.resumed_from
        return min(value for value in positions if value is not None)

    # -- persistence ------------------------------------------------------------
    def checkpoint(self, *, ingested_at: datetime) -> StreamWatermark | None:
        """Persist the completion floor.  Returns the stored row, or `None`.

        `None` means there was nothing durable to write -- either no repository, or
        at least one declared shard has not produced a position yet.  It never means
        "written successfully".
        """

        floor = self.completion_floor()
        if self.repository is None or floor is None:
            return None
        return self.repository.advance(
            StreamWatermark(feed_id=self.feed_id, position=floor, ingested_at=ingested_at)
        )
