"""Filter changed-file paths to those that affect builds or tests.

Reads paths on stdin and prints build-relevant paths on stdout. Docs-only
changes are skipped for every package.
"""

import re
import sys

IGNORE = re.compile(
    r'^(?:'
    r'[^/]+/(?:docs|overrides|_snippets|assets)/'  # per-package docs and theme
    r'|[^/]+/properdocs\.yml$'  # per-package docs site config
    r'|[^/]+\.md$'  # repo-root markdown
    r')'
)

for line in sys.stdin:
    path = line.strip()
    if path and not IGNORE.search(path):
        print(path)
