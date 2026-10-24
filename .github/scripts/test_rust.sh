#!/usr/bin/env bash
set -euo pipefail

for profile in test release; do
  args=(--locked -p "$CARGO_PACKAGE" --cargo-profile "$profile")
  if [[ "$PACKAGE_TYPE" != rust ]]; then
    args+=(--no-tests=pass)
  fi
  cargo nextest run "${args[@]}"
done
