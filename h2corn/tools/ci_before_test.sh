#!/usr/bin/env bash
# Optional monorepo CI hook: install test-only system deps before pytest.
set -euo pipefail

# h2spec is Linux x64 only (protocol conformance tests).
if [[ "${RUNNER_OS:-}" != "Linux" || "${RUNNER_ARCH:-}" != "X64" ]]; then
  exit 0
fi

H2SPEC_VERSION=2.6.0
H2SPEC_SHA256=157ee0de702e01ad40e752dbf074b366027e550c8e7504f9450da2809e279318
archive="$(mktemp)"
trap 'rm -f "$archive"' EXIT
curl --fail --location --silent --show-error \
  "https://github.com/summerwind/h2spec/releases/download/v${H2SPEC_VERSION}/h2spec_linux_amd64.tar.gz" \
  -o "$archive"
echo "$H2SPEC_SHA256  $archive" | sha256sum --check --status
tar -xzf "$archive" h2spec
sudo mv h2spec /usr/local/bin/
