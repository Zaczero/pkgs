#!/usr/bin/env bash
set -euo pipefail

for profile in test release; do
  args=(--locked -p "$CARGO_PACKAGE" --cargo-profile "$profile")
  status=0
  cargo nextest run "${args[@]}" || status=$?
  if [[ "$status" == 4 && "$PACKAGE_TYPE" != rust ]]; then
    cargo nextest list "${args[@]}" --message-format json >"$RUNNER_TEMP/rust-tests.json"
    count="$(jq -er '."test-count" | numbers' "$RUNNER_TEMP/rust-tests.json")"
    if [[ "$count" == 0 ]]; then
      echo "::notice::The $profile package targets contain no Rust tests."
      continue
    fi
  fi
  if [[ "$status" != 0 ]]; then
    exit "$status"
  fi
done
