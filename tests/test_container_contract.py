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


def _instructions() -> list[str]:
    """Dockerfile instructions with continuations joined and whitespace collapsed."""

    document = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    lines = document.replace("\\\n", " ").splitlines()
    return [" ".join(line.split()) for line in lines if line.strip() and not line.lstrip().startswith("#")]


def test_pipeline_worker_image_installs_third_party_packages_only_from_the_hash_lock() -> None:
    pip_installs = [line for line in _instructions() if "pip install" in line]

    assert pip_installs[0] == (
        "RUN python -m pip install --no-cache-dir --require-hashes --only-binary=:all: "
        "-r requirements/runtime.txt"
    )
    assert pip_installs[1].startswith(
        "RUN python -m pip install --no-cache-dir --no-deps . && python -m pip check && "
    )
    assert len(pip_installs) == 2


def test_pipeline_worker_image_removes_runtime_unneeded_perl() -> None:
    """Keep Debian's vulnerable Perl runtime out of the shipped worker image."""

    document = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "dpkg --purge --force-remove-essential perl-base" in document


def test_container_build_context_excludes_local_state_and_credentials() -> None:
    ignored = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    for required in (".git", ".venv", ".env", "__pycache__", "*.parquet"):
        assert required in ignored


def test_ci_executes_the_arm64_image_and_checks_readiness() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert (
        "docker/setup-qemu-action@96fe6ef7f33517b61c61be40b68a1882f3264fb8 # v4.2.0"
        in workflow
    )
    assert (
        "docker/setup-buildx-action@bb05f3f5519dd87d3ba754cc423b652a5edd6d2c # v4.2.0"
        in workflow
    )
    assert "docker buildx build --platform linux/arm64 --load" in workflow
    assert "curl --fail --silent http://127.0.0.1:18080/ready" in workflow
    assert "dpkg-query -W perl-base" in workflow
