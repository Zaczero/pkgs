#!/usr/bin/env bash
# Optional monorepo CI hook: install test-only system deps before pytest.
set -euo pipefail

# h2spec is Linux x64 only (protocol conformance tests).
if [[ "${RUNNER_OS:-}" != "Linux" || "${RUNNER_ARCH:-}" != "X64" ]]; then
  exit 0
fi

H2SPEC_VERSION="${H2SPEC_VERSION:-2.6.0}"
curl -sSL \
  "https://github.com/summerwind/h2spec/releases/download/v${H2SPEC_VERSION}/h2spec_linux_amd64.tar.gz" \
  | tar -xzf - h2spec
sudo mv h2spec /usr/local/bin/
