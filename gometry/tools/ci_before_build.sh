#!/usr/bin/env bash
set -euo pipefail

if [[ "$RUNNER_OS" == Windows ]]; then
  choco install sqlite --yes --no-progress
fi
