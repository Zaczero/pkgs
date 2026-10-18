#!/usr/bin/env bash
set -euo pipefail

for profile in test release; do
  args=(--locked -p "$CARGO_PACKAGE" --cargo-profile "$profile")
  cargo nextest list "${args[@]}" --message-format json > "$RUNNER_TEMP/rust-tests.json"
  count="$(jq -er '."test-count" | numbers' "$RUNNER_TEMP/rust-tests.json")"
  if [[ "$count" == 0 && "$PACKAGE_TYPE" != rust ]]; then
    echo "::notice::The $profile package targets contain no Rust tests."
  else
    cargo nextest run "${args[@]}"
  fi
done
