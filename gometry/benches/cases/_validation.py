"""Validation-only output for focused benchmark cases.

The pytest owner invokes this path with small fixtures.  It proves that a case
still imports and emits its declared schema without running its measurement
workload.
"""

from __future__ import annotations

import json
from typing import Any


def emit_validation(case_id: str, *, useful_output: dict[str, Any]) -> None:
    print(json.dumps({
        'schema_version': 1,
        'artifact_type': 'gometry-focused-case-validation',
        'case_id': case_id,
        'timed': False,
        'useful_output': useful_output,
    }, sort_keys=True))
