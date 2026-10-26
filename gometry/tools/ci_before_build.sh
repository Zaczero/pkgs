#!/usr/bin/env bash
set -euo pipefail

if command -v sqlite3 >/dev/null; then
  exit 0
fi

case "${RUNNER_OS:-Linux}" in
  Windows)
    # The official SQLite tools, pinned to sqlite.org's published release.
    case "$RUNNER_ARCH" in
      X64) arch=x64 sha256=fb20997ade5583a5fbf1e36db7bc3c82c9416b3f314f16961e1572aa4e49487d ;;
      ARM64) arch=arm64 sha256=b5f77486ce53f3458b8443eda928ddf4312eea5d4dd3adb68851ff39028eee05 ;;
      *) echo "no SQLite tools for Windows runner architecture $RUNNER_ARCH" >&2; exit 1 ;;
    esac
    dir="$(cygpath -u "$RUNNER_TEMP")/sqlite"
    mkdir -p "$dir"
    curl --proto '=https' --tlsv1.2 -fsSL --retry 3 -o "$dir/tools.zip" \
      "https://www.sqlite.org/2026/sqlite-tools-win-$arch-3540100.zip"
    echo "$sha256  $dir/tools.zip" | sha256sum --check --quiet
    python -m zipfile -e "$dir/tools.zip" "$dir"
    PATH="$dir:$PATH"
    cygpath -w "$dir" >> "$GITHUB_PATH"
    ;;
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

command -v sqlite3 >/dev/null || {
  echo "sqlite3 is required to build PROJ's database, but installation did not provide it" >&2
  exit 1
}
