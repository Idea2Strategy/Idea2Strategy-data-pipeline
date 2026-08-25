from __future__ import annotations

from types import TracebackType

from tests.conftest import _execute_script


class RecordingCopy:
    def __init__(self, statement: str, writes: list[str]) -> None:
        self.statement = statement
        self.writes = writes

    def __enter__(self) -> RecordingCopy:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def write(self, payload: str) -> None:
        self.writes.append(payload)


class RecordingCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.copy_statements: list[str] = []
        self.copy_writes: list[str] = []

    def execute(self, statement: str) -> None:
        self.executed.append(statement)

    def copy(self, statement: str) -> RecordingCopy:
        self.copy_statements.append(statement)
        return RecordingCopy(statement, self.copy_writes)


def test_pg_dump_copy_blocks_use_the_copy_protocol_and_leave_sql_executable() -> None:
    cursor = RecordingCursor()

    _execute_script(
        cursor,
        "CREATE TABLE sample (id integer, label text);\n"
        "COPY sample (id, label) FROM stdin;\n"
        "1\tone\n2\ttwo\n"
        "\\.\n"
        "ALTER TABLE sample ADD PRIMARY KEY (id);\n",
    )

    assert cursor.copy_statements == ["COPY sample (id, label) FROM stdin;"]
    assert cursor.copy_writes == ["1\tone\n2\ttwo\n"]
    assert cursor.executed == [
        "CREATE TABLE sample (id integer, label text);\n",
        "ALTER TABLE sample ADD PRIMARY KEY (id);\n",
    ]


def test_empty_pg_dump_copy_block_does_not_write_a_phantom_row() -> None:
    cursor = RecordingCursor()

    _execute_script(cursor, "COPY sample (id) FROM stdin;\r\n\\.\r\nSELECT 1;\r\n")

    assert cursor.copy_statements == ["COPY sample (id) FROM stdin;"]
    assert cursor.copy_writes == []
    assert cursor.executed == ["SELECT 1;\r\n"]
