from __future__ import annotations

import re
from pathlib import Path

EXPECTED_PINS = {
    "actions/checkout": ("3d3c42e5aac5ba805825da76410c181273ba90b1", "v7.0.1"),
    "actions/setup-python": ("5fda3b95a4ea91299a34e894583c3862153e4b97", "v7.0.0"),
    "actions/upload-artifact": ("043fb46d1a93c77aae656e7c1c64a875d1fc6a0a", "v7.0.1"),
    "docker/setup-buildx-action": ("bb05f3f5519dd87d3ba754cc423b652a5edd6d2c", "v4.2.0"),
    "docker/setup-qemu-action": ("96fe6ef7f33517b61c61be40b68a1882f3264fb8", "v4.2.0"),
    "trufflesecurity/trufflehog": ("6f3c981e7b77f235fd2702dd74af25fc4b72bf11", "v3.96.0"),
}
USES = re.compile(
    r"^\s*(?:-\s+)?uses:\s+([^\s@]+)@([^\s#]+)(?:\s+#\s+(\S+))?\s*$"
)


def test_workflow_actions_use_reviewed_immutable_pins() -> None:
    root = Path(__file__).resolve().parents[1]
    observed = 0
    for workflow in sorted((root / ".github" / "workflows").glob("*.yml")):
        for line_number, line in enumerate(workflow.read_text(encoding="utf-8").splitlines(), 1):
            if "uses:" not in line:
                continue
            match = USES.match(line)
            assert match is not None, f"{workflow.name}:{line_number} has an unparsable action reference"
            action, revision, tag = match.groups()
            assert action in EXPECTED_PINS, (
                f"{workflow.name}:{line_number} uses an unreviewed action: {action}"
            )
            expected_revision, expected_tag = EXPECTED_PINS[action]
            assert revision == expected_revision, (
                f"{workflow.name}:{line_number} must pin {action}@{expected_revision}, found {revision}"
            )
            assert tag == expected_tag, (
                f"{workflow.name}:{line_number} must annotate {action} with # {expected_tag}, found # {tag}"
            )
            observed += 1
    assert observed > 0
