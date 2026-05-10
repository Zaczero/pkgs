"""Filter changed-file paths to those that affect builds or tests.

Reads paths on stdin, prints relevant paths on stdout. Lets docs-only
changes skip CI universally for any package using the docs/ convention.
"""

import re
import sys

IGNORE = re.compile(
    r"^[^/]+(?:"
    r"/(docs|overrides|_snippets|assets)/"  # per-package docs and theme
    r"|/properdocs\.yml$"                   # per-package docs site config
    r"|\.md$"                               # repo-root markdown
    r")"
)

for line in sys.stdin:
    path = line.strip()
    if path and not IGNORE.search(path):
        print(path)
