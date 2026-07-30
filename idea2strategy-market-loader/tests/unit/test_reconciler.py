from market_loader.pipeline.reconciler import compare_object


def test_reconciler_detects_missing_version_and_hash_mismatch() -> None:
    missing = compare_object(
        object_key="key",
        expected_version_id="v1",
        expected_sha256="a",
        actual_version_id=None,
        actual_sha256=None,
    )
    assert [item.finding_type for item in missing] == ["S3_OBJECT_MISSING"]
    mismatch = compare_object(
        object_key="key",
        expected_version_id="v1",
        expected_sha256="a",
        actual_version_id="v2",
        actual_sha256="b",
    )
    assert {item.finding_type for item in mismatch} == {
        "VERSION_MISMATCH",
        "SHA256_MISMATCH",
    }
