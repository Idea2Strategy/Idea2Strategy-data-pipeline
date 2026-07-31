import copy
import json
import unittest
from pathlib import Path

from market_pipeline_lib.compatibility import (
    ContractValidationError,
    validate_backtest_request,
    validate_backtest_result,
    validate_dataset_manifest,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures/contracts/com06-d-fixtures.v1.json"


class CompatibilityContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixtures = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    def test_d_owned_fixture_set_is_self_consistent(self) -> None:
        manifest = self.fixtures["dataset_manifest"]
        request = self.fixtures["backtest_request"]

        validate_dataset_manifest(manifest)
        validate_backtest_request(request)
        for result in self.fixtures["backtest_results"]:
            validate_backtest_result(result)

        self.assertEqual(request["dataset_manifest_id"], manifest["manifest_id"])
        self.assertEqual(request["dataset_hash"], manifest["dataset_hash"])
        self.assertEqual(
            {result["status"] for result in self.fixtures["backtest_results"]},
            {"QUEUED", "RUNNING", "COMPLETE", "FAILED", "UNAVAILABLE"},
        )

    def test_available_manifest_rejects_corrupted_dataset_hash(self) -> None:
        manifest = copy.deepcopy(self.fixtures["dataset_manifest"])
        manifest["dataset_hash"] = "0" * 64

        with self.assertRaisesRegex(ContractValidationError, "dataset_hash"):
            validate_dataset_manifest(manifest)

    def test_contract_rejects_unknown_schema_version(self) -> None:
        request = copy.deepcopy(self.fixtures["backtest_request"])
        request["schema_version"] = 2

        with self.assertRaisesRegex(ContractValidationError, "schema_version"):
            validate_backtest_request(request)


if __name__ == "__main__":
    unittest.main()
