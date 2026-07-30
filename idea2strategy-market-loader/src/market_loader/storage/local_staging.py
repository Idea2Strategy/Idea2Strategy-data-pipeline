from __future__ import annotations

from pathlib import Path
from uuid import UUID

from market_loader.errors import InputError


class LocalStaging:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def path_for(
        self, run_id: UUID, adjustment: str, resolution: str, year: int, shard: int
    ) -> Path:
        relative = Path(
            str(run_id),
            f"adjustment={adjustment}",
            f"resolution={resolution}",
            f"year={year}",
            f"shard={shard:02d}",
            "part-00001.parquet",
        )
        target = (self.root / relative).resolve()
        if self.root not in target.parents:
            raise InputError("staging path escapes staging root")
        return target
