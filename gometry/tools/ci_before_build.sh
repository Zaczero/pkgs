#!/usr/bin/env bash
set -euo pipefail

if command -v sqlite3 >/dev/null; then
  exit 0
fi

case "${RUNNER_OS:-Linux}" in
  Windows) choco install sqlite --yes --no-progress ;;
  macOS) brew install sqlite ;;
  Linux)
    if command -v dnf >/dev/null; then
      dnf install -y sqlite
    elif command -v apk >/dev/null; then
      apk add sqlite
    else
      sudo apt-get update
      sudo apt-get install --yes sqlite3
    fi
    ;;
esac
