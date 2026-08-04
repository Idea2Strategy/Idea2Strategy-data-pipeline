from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_pipeline_worker_image_is_arm64_portable_and_runs_without_root() -> None:
    dockerfile = ROOT / "Dockerfile"
    assert dockerfile.is_file(), "Fargate cannot build the pipeline worker without a Dockerfile"

    document = dockerfile.read_text(encoding="utf-8")
    assert "FROM python:3.12.13-slim-bookworm" in document
    assert "--platform=linux/amd64" not in document
    assert "USER 10001:10001" in document
    assert 'ENTRYPOINT ["pipeline-worker"]' in document
    assert "pip install --no-cache-dir ." in document


def test_container_build_context_excludes_local_state_and_credentials() -> None:
    ignored = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    for required in (".git", ".venv", ".env", "__pycache__", "*.parquet"):
        assert required in ignored
