"""Filesystem path helpers that keep local IO inside the Windows MAX_PATH limit.

Two independent problems are solved here.

`short_temp_path` bounds the *transient* name used for atomic writes. The old
pattern was ``.<full destination name>.<pid>.tmp``, which roughly doubles an
already long file name (``source=<uuid>-batch=000001.parquet`` alone is 65
characters) and pushes deep partition paths past 260 characters on Windows.
The replacement is a fixed-width hidden token, so the temp name never grows
with the destination name.

`long_path` renders an absolute path in Windows extended-length form
(``\\\\?\\C:\\...``) for the duration of a single OS call. Canonical object keys
are deep by contract (``provider=``/``feed=``/``dataset=``/``revision=``/
``layer=``/``resolution=``/``granularity=``/``partition_start=``/
``partition_end=``/``shard=``), so a store rooted under a normal user profile
already exceeds the limit before the file name is appended. Shortening the key
is not an option — it is the published object identity.
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path

__all__ = [
    "MAX_TEMP_SUFFIX_LENGTH",
    "TEMP_TOKEN_BYTES",
    "long_path",
    "short_temp_path",
]


# ".<32 hex chars>.tmp" would be 37; 8 random bytes gives ".<16>.tmp" = 21.
TEMP_TOKEN_BYTES = 8
MAX_TEMP_SUFFIX_LENGTH = 2 + 2 * TEMP_TOKEN_BYTES + 4  # "." + token + ".tmp"

_WINDOWS = os.name == "nt"
_EXTENDED_PREFIX = "\\\\?\\"
_DEVICE_PREFIX = "\\\\.\\"


def short_temp_path(destination: Path) -> Path:
    """Return a sibling temp path whose name length is fixed and bounded.

    The name is unique per call, hidden (leading dot) and always ends in
    ``.tmp`` so cleanup and ignore rules can match it. It intentionally does
    **not** embed the destination name: that is what made the old scheme
    overflow MAX_PATH on deep partitions.
    """
    token = secrets.token_hex(TEMP_TOKEN_BYTES)
    return destination.parent / f".{token}.tmp"


def long_path(path: Path | str) -> str:
    """Return a path string safe to hand to an OS call on Windows.

    On Windows the absolute path is returned in extended-length form, which
    lifts the 260-character `MAX_PATH` restriction for that call. Everywhere
    else, and for paths that already carry a ``\\\\?\\`` or ``\\\\.\\`` prefix,
    the input is returned unchanged.
    """
    text = os.fspath(path)
    if not _WINDOWS:
        return text
    if text.startswith(_EXTENDED_PREFIX) or text.startswith(_DEVICE_PREFIX):
        return text
    absolute = os.path.abspath(text)
    if absolute.startswith("\\\\"):
        # \\server\share -> \\?\UNC\server\share
        return f"{_EXTENDED_PREFIX}UNC{absolute[1:]}"
    return _EXTENDED_PREFIX + absolute
