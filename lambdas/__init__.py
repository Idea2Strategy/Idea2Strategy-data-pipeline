"""AWS Lambda handlers for the D bundle.

The `data-pipeline` repository exposes three Lambdas:

* ``lambdas.pipeline_trigger``           - schedules pipeline commands onto the worker queue
* ``lambdas.lightweight_validation``     - cheap contract validation of published documents
* ``lambdas.corporate_action_research``  - twice-daily corporate-action evidence collection

Each package exposes ``handler(event, context)``.  Shared runtime support lives
in :mod:`apps.common` (typed errors, structured logging, idempotency) and
:mod:`lambdas.common` (result envelope).
"""
