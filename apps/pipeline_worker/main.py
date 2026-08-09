"""Console entry point for `pipeline-worker`.

    pipeline-worker                 # run the consumer loop
    pipeline-worker --check-config  # validate configuration and exit
    pipeline-worker --print-env     # list every environment variable it reads

Exit codes:
    0  clean shutdown, or a successful --check-config / --print-env
    1  the worker started and then failed
    2  configuration error (the process never started)
    3  a required port has no adapter (for example SQS before DP5)
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence

from apps.common.errors import ConfigurationError, PipelineAppError, PortNotConfiguredError
from apps.common.logging import configure_logging
from apps.pipeline_worker.config import WorkerConfig, environment_variable_help
from apps.pipeline_worker.worker import PipelineWorker

EXIT_OK = 0
EXIT_RUNTIME_FAILURE = 1
EXIT_CONFIGURATION_ERROR = 2
EXIT_PORT_NOT_CONFIGURED = 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeline-worker",
        description="Idea2Strategy market-data pipeline worker.",
    )
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="Validate configuration, print a log-safe description, and exit.",
    )
    parser.add_argument(
        "--print-env",
        action="store_true",
        help="Print every environment variable this app reads, then exit.",
    )
    parser.add_argument(
        "--publish-manifest-watermarks",
        action="store_true",
        help=(
            "Advance active-feed watermarks from AVAILABLE manifests and exit. "
            "Requires PIPELINE_WORKER_DATABASE_URL."
        ),
    )
    parser.add_argument(
        "--sync-market-history",
        action="store_true",
        help=(
            "Publish missing adjusted daily history, project recent AVAILABLE "
            "bars to Redis, advance watermarks, and exit."
        ),
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    environment: Mapping[str, str] | None = None,
) -> int:
    arguments = build_parser().parse_args(list(argv) if argv is not None else None)

    if arguments.print_env:
        print(environment_variable_help())
        return EXIT_OK

    if arguments.publish_manifest_watermarks:
        from apps.pipeline_worker.publish_manifest_watermarks import (
            execute as publish_manifest_watermarks,
        )

        try:
            result = publish_manifest_watermarks([], environment)
        except (OSError, RuntimeError, ValueError) as error:
            print(f"publish-manifest-watermarks failed: {error}", file=sys.stderr)
            return EXIT_RUNTIME_FAILURE
        print(json.dumps(result, indent=2, sort_keys=True))
        return EXIT_OK

    if arguments.sync_market_history:
        from apps.pipeline_worker.sync_market_history import execute as sync_market_history

        try:
            result = sync_market_history(environment)
        except (OSError, RuntimeError, ValueError) as error:
            print(f"sync-market-history failed: {error}", file=sys.stderr)
            return EXIT_RUNTIME_FAILURE
        print(json.dumps(result, indent=2, sort_keys=True))
        return EXIT_OK

    try:
        config = WorkerConfig.from_environment(environment)
    except ConfigurationError as error:
        # Logging is not configured yet — and must not be, because the log level
        # itself comes from the configuration that just failed to load.
        print(f"pipeline-worker configuration error: {error}", file=sys.stderr)
        print("Run `pipeline-worker --print-env` for the full variable list.", file=sys.stderr)
        return EXIT_CONFIGURATION_ERROR

    if arguments.check_config:
        print(json.dumps(config.describe(), indent=2, sort_keys=True))
        return EXIT_OK

    configure_logging(config.log_level)
    try:
        return PipelineWorker(config).run()
    except PortNotConfiguredError as error:
        print(f"pipeline-worker port not configured: {error}", file=sys.stderr)
        return EXIT_PORT_NOT_CONFIGURED
    except PipelineAppError as error:
        print(f"pipeline-worker failed: {error}", file=sys.stderr)
        return EXIT_RUNTIME_FAILURE


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
