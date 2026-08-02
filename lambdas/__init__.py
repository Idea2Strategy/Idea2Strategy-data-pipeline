"""AWS Lambda handlers for the D bundle.

`docs/backend-implementation-master-checklist.md` names three Lambdas for the
`data-pipeline` repository:

* ``lambdas.pipeline_trigger``           - schedules pipeline commands onto the worker queue
* ``lambdas.lightweight_validation``     - cheap contract validation of published documents
* ``lambdas.corporate_action_research``  - twice-daily corporate-action evidence collection

Each package exposes ``handler(event, context)``.  Shared runtime support lives
in :mod:`apps.common` (typed errors, structured logging, idempotency) and
:mod:`lambdas.common` (result envelope).
"""
