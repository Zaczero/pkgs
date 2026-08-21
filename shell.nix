{ }:

let
  pkgs =
    import
      (fetchTarball "https://github.com/NixOS/nixpkgs/archive/6b5e5b7a6631f065bf6908986990b37d845f847f.tar.gz")
      { };

  makeScript =
    with pkgs;
    name: text:
    writeTextFile {
      inherit name;
      executable = true;
      destination = "/bin/${name}";
      text = ''
        #!${runtimeShell} -e
        shopt -s extglob nullglob globstar
        ${text}
      '';
      checkPhase = ''
        ${stdenv.shellDryRun} "$target"
        ${shellcheck}/bin/shellcheck -e SC1091 "$target"
      '';
      meta.mainProgram = name;
    };

  packages' = with pkgs; [
    cargo-about
    coreutils
    curl
    gnugrep
    h2spec
    hatch
    jq
    k6
    lychee
    cmake
    openssh
    oha
    pyright
    rsync
    ruff
    uv

    (makeScript "nixpkgs-update" ''
      hash=$(
        curl -fsSL \
          https://prometheus.nixos.org/api/v1/query \
          -d 'query=channel_revision{channel="nixpkgs-unstable"}' \
        | jq -r ".data.result[0].metric.revision")
      sed -i "s|nixpkgs/archive/[0-9a-f]\{40\}|nixpkgs/archive/$hash|" shell.nix
      echo "Nixpkgs updated to $hash"
    '')

    # docs-{serve,build,deploy} act on the package in CWD. Each requires a
    # properdocs.yml and docs tooling in the default dev dependency group.
    # Convention: site_url in properdocs.yml is the canonical
    # hostname; deploy rsyncs the build into edge:/var/www/<that-host>/. The
    # Caddyfile entry for the host is maintained by hand on edge.
    (makeScript "docs-serve" ''
      export MATURIN_IMPORT_HOOK_ENABLED=0
      export CARGO_TARGET_DIR="''${CARGO_TARGET_DIR:-$(pwd)/target}"
      unset SOURCE_DATE_EPOCH
      if [ ! -f properdocs.yml ]; then
        echo "no properdocs.yml in $(pwd)" >&2
        exit 1
      fi
      uv sync --quiet --all-groups
      exec uv run --no-sync properdocs serve --no-strict --dev-addr 127.0.0.1:8765 "$@"
    '')

    (makeScript "docs-build" ''
      export MATURIN_IMPORT_HOOK_ENABLED=0
      export CARGO_TARGET_DIR="''${CARGO_TARGET_DIR:-$(pwd)/target}"
      unset SOURCE_DATE_EPOCH
      if [ ! -f properdocs.yml ]; then
        echo "no properdocs.yml in $(pwd)" >&2
        exit 1
      fi
      uv sync --quiet --all-groups
      uv run --no-sync properdocs build --strict --clean "$@"
      if [ -f tools/docs/check.py ]; then
        uv run --no-sync python tools/docs/check.py
      fi
    '')

    (makeScript "docs-deploy" ''
      export MATURIN_IMPORT_HOOK_ENABLED=0
      export CARGO_TARGET_DIR="''${CARGO_TARGET_DIR:-$(pwd)/target}"
      unset SOURCE_DATE_EPOCH
      if [ ! -f properdocs.yml ]; then
        echo "no properdocs.yml in $(pwd)" >&2
        exit 1
      fi
      host=$(${gnugrep}/bin/grep -m1 '^site_url:' properdocs.yml \
        | sed -E 's|.*://||; s|/.*||')
      if [ -z "$host" ]; then
        echo "could not derive host from properdocs.yml site_url" >&2
        exit 1
      fi

      dry_run=false
      if [ "''${1:-}" = "--dry-run" ] || [ "''${1:-}" = "-n" ]; then
        dry_run=true
        echo "DRY RUN: build and validation will run; nothing will contact edge."
      fi

      docs-build

      echo "Target: edge:/var/www/$host/"
      if [ "$dry_run" = true ]; then
        echo "DRY RUN: rsync skipped."
      else
        rsync -avL --mkpath --delete-after \
          site/ "edge:/var/www/$host/"
      fi
      echo "Done. The Caddyfile entry for $host is maintained by hand on edge."
    '')
  ];

  shell' = ''
    export TZ=UTC
    export NIX_ENFORCE_NO_NATIVE=0
    export NIX_ENFORCE_PURITY=0
    export PYTHONPATH=""
    export COVERAGE_CORE=sysmon
    export PYTEST_ADDOPTS="--quiet --import-mode=importlib --strict-markers --strict-config"

    # `uv sync --all-groups` builds through maturin's PEP 517 backend, which defaults to
    # plain `--release` (thin LTO and default codegen-units), measured at 37 s for a one-line
    # change against 2 s for dev. The import hook is separately unset above, so
    # both paths agree on dev. Dev also turns on debug_assertions and
    # overflow-checks, which release omits and which nothing else exercises.
    #
    # Two consequences worth knowing: `uv build` run *in this shell* produces a
    # dev-profile wheel (publishing happens in CI via cibuildwheel, which does
    # not source this shell), and benchmarks must still build release -- see
    # h2corn/bench/README.md.
    export MATURIN_PEP517_ARGS="--profile dev"
  '';
in
pkgs.mkShell {
  buildInputs = packages';
  shellHook = shell';
}
