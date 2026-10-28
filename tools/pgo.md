# Profile-guided distribution wheels

`pgo.py` prepares a baseline, an instrumented training wheel, and a profile-use
wheel with the same `release-dist` Cargo profile. Packages opt in through
`pyproject.toml`:

```toml
[tool.ci.pgo]
build = "cp315-manylinux_x86_64"
target = "x86_64-unknown-linux-gnu"
adapter = "bench.pgo.adapter"
test-groups = ["freethreaded", "grpc-interop"]
```

`wheels.py` owns distribution builds, source identities, build receipts, and
installed-wheel byte checks for both PGO and benchmark consumers.

The shared build matrix routes that identifier to PGO and excludes it from the
ordinary wheel build. Other identifiers use their ordinary build. The release
command runs all three phases through cibuildwheel, tests the installed baseline
and final wheels, and copies only the tested profile-use wheel into `dist`.
Actions retains the profiles, collection receipts, wheel identities, and build
and test logs in a separate `pgo-*` artifact with the repository's default
retention. Download that artifact while it is retained if the profile is needed
for later reproduction.

## Package adapter

The adapter owns the production entry point, collection lifecycle, workloads,
and installed-wheel tests:

- `GENERATE_FLAGS`: additional target flags enabling collection-only hooks.
- `stage_generator()`: a context manager staging collection-only Python changes
  and restoring the source on exit.
- `verify_runtime(generator=...)`: verify the supported interpreter and the
  presence or absence of collection interfaces.
- `collect(output)`: execute the finite corpus and return its collection receipt.
- `test(output)`: exercise the installed wheel and retain test evidence.
- `known_profile_diagnostic(line)`: recognize narrowly justified diagnostics.
  Other profile-use warnings fail the build.

Declare training dependencies in the `pgo` dependency group. Collection receipts
list the planned leaves, rational weights, verified useful-work denominators,
expected native function families, generator image hashes, and exact worker raw
files with their hashes. `pgo_profile.py` checks these inputs, rejects empty or
incomplete contributions, and merges each leaf before applying integer
coefficients proportional to `weight / units`. It retains zero-count functions.

For a single-process workload, `pgo_process.py` runs the adapter's `train` entry
point in a fresh interpreter and collects its exit-flushed profile. The entry
point reports completed operations and the loaded extension's digest through
`completed`; `collect` verifies both before producing the receipt.

The accepted profile has an immutable content-addressed path. Its manifest binds
it to the generator wheel, compiler, target, Python build, source and dependency
inputs, corpus, and merge coefficients. A changed compiler, source, target, or
Python build requires fresh collection. An environment `RUSTFLAGS` or
`CARGO_ENCODED_RUSTFLAGS` is rejected because it would override the target
configuration carrying PGO.

## Local qualification

Use a supported interpreter with the package's training and testing dependencies.
Run these commands from the repository root; install each resulting wheel into
the interpreter used for its collection or tests.

```sh
python tools/pgo.py h2corn build baseline --output /path/to/baseline
python tools/pgo.py h2corn build generate --output /path/to/generate
uv pip install --python /path/to/training-python /path/to/generate/*.whl
/path/to/training-python tools/pgo.py h2corn collect \
  --wheel /path/to/generate/h2corn-….whl \
  --output /path/to/collection --profiles h2corn/.pgo
python tools/pgo.py h2corn build use --output /path/to/use \
  --profile h2corn/.pgo/DIGEST/profile.profdata
uv pip install --python /path/to/testing-python /path/to/use/*.whl
/path/to/testing-python tools/pgo.py h2corn test \
  --wheel /path/to/use/h2corn-….whl --output /path/to/tests
```

Each build and collection needs a fresh output directory. Keep indexed profiles
inside the package for managed Cargo offload to transfer them as build inputs.
Local wheels use the Linux compatibility tag; the release command owns manylinux
repair and validation.

Compare the exact baseline and profile-use wheels with held-out workloads under
matched runtime settings. Preserve raw paired observations and same-wheel
controls; profile coverage and lower instruction counts alone do not establish
throughput or tail-latency improvement.
