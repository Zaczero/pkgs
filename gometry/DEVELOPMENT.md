# Development

This page is for contributors diagnosing gometry's Rust and native-library boundaries. The
sanitizer command is deliberate and local; it is not part of the ordinary pytest gate or CI.

## AddressSanitizer

AddressSanitizer (ASan) checks the instrumented gometry extension and the bundled native archives
while the focused public-path tests execute. This diagnostic is deliberately scoped to Linux
x86_64; it rejects other operating systems and architectures. Use a Linux checkout with the
nightly Rust toolchain, Clang, CMake, binutils, the initialized libtiff submodule, and a CPython
environment containing NumPy, PyArrow, pytest, `pytest-timeout`, `pytest-xdist`, and maturin 1.14.1.

From the `gometry` directory, create a disposable test environment and invoke the repository-owned
diagnostic directly:

```bash
ENV_DIR=/tmp/gometry-asan-env
uv venv --python 3.14 "$ENV_DIR"
uv pip install --python "$ENV_DIR/bin/python" \
  'numpy>=1.26' 'pyarrow>=24' 'pytest>=9.1' \
  'pytest-timeout>=2.4' 'pytest-xdist[psutil]>=3.8' 'maturin==1.14.1'
"$ENV_DIR/bin/python" tools/sanitizers.py --python "$ENV_DIR/bin/python"
```

The command builds into `target-sanitizer-address` and its private Cargo build-script directory,
passes `-Z sanitizer=address` only to the gometry library target, and runs
`tests/test_unsafe_boundaries.py` and
`tests/test_free_threading_soundness.py`. A GIL-enabled interpreter runs the deterministic cases;
the concurrent free-threading cases must report their explicit skip reason. A free-threaded
interpreter runs those cases only when its Python installation provides a working `sysconfig`.

The selected command instruments:

- Rust code compiled into the gometry library target;
- C and C++ code in bundled PROJ, libtiff, SQLite, and zlib static archives through `CFLAGS` and
  `CXXFLAGS`; and
- the ASan runtime loaded into the test process with `LD_PRELOAD`.

After the build, the command checks the extension and every located PROJ, libtiff, SQLite, and zlib
static archive for ASan references. Missing archives or archives without references fail the
diagnostic.

CPython, NumPy, PyArrow, Rust dependencies, and test tooling are not instrumented. A clean run is
evidence only for the public paths exercised by these two test modules. Before building, the
diagnostic saves the selected ABI extension. It restores its exact prior bytes and path on success,
test failure, or build failure; if no extension existed, it removes the generated artifact.

The free-threaded CPython 3.14t binary distributed by the local uv installation may fail before
the build with `ModuleNotFoundError: No module named '_sysconfigdata__linux_x86_64-linux-gnu'`.
That is an unsupported interpreter-installation signature, not a sanitizer result; use a
free-threaded Python with complete build configuration data rather than treating the command as
green.

## ThreadSanitizer

ThreadSanitizer (TSan) is not retained as a gometry diagnostic. Applying `-Z sanitizer=thread` only
to the extension is ABI-incompatible with uninstrumented `std`, `core`, and other Rust dependencies.
A coherent full-graph `-Z build-std=std` build would also require instrumenting Cargo's host and
procedural-macro crates, and is not a supported extension build here. Suppressing the ABI mismatch
would leave the dependency graph uninstrumented and produce false evidence.

## Ordinary builds

The diagnostic uses a separate Cargo target and build-script directory and changes no CI, pytest,
nextest, product runtime, benchmark, or distribution configuration. Its C and Rust flags exist only
in the launched process environment. The prior extension is restored automatically, so a failed or
successful diagnostic does not require a rebuild before ordinary development.
