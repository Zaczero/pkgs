#!/usr/bin/env bash
# CRS cache invalidation tests copy the bundled PROJ database before mutating it.
set -euo pipefail

cd "$(dirname "$0")/.."
cargo build
