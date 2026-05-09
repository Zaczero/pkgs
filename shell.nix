{ }:

let
  pkgs =
    import
      (fetchTarball "https://github.com/NixOS/nixpkgs/archive/8d8c1fa5b412c223ffa47410867813290cdedfef.tar.gz")
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
    coreutils
    curl
    gnugrep
    h2spec
    hatch
    jq
    k6
    openssh
    pyright
    rsync
    ruff
    uv

    (makeScript "activate" ''
      if [ -f pyproject.toml ]; then
        uv sync
        if ${gnugrep}/bin/grep -q 'build-backend = "maturin"' pyproject.toml; then
          if ! .venv/bin/python - <<'PY'
      import sys
      import sysconfig
      from pathlib import Path

      sitecustomize = Path(sysconfig.get_paths()["purelib"]) / "sitecustomize.py"
      if sitecustomize.exists() and "maturin_import_hook" in sitecustomize.read_text():
          raise SystemExit(0)
      raise SystemExit(1)
      PY
          then
            uv run python -m maturin_import_hook site install --args="--release"
          fi
        fi
      fi
    '')

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
    # properdocs.yml and a [dependency-groups] docs = [...] section in
    # pyproject.toml. Convention: site_url in properdocs.yml is the canonical
    # hostname; deploy rsyncs the build into edge:/var/www/<that-host>/. The
    # Caddyfile entry for the host is maintained by hand on edge.
    (makeScript "docs-serve" ''
      export MATURIN_IMPORT_HOOK_ENABLED=0
      unset SOURCE_DATE_EPOCH
      if [ ! -f properdocs.yml ]; then
        echo "no properdocs.yml in $(pwd)" >&2
        exit 1
      fi
      uv sync --group docs --quiet
      exec uv run --no-sync properdocs serve --no-strict --dev-addr 127.0.0.1:8765 "$@"
    '')

    (makeScript "docs-build" ''
      export MATURIN_IMPORT_HOOK_ENABLED=0
      unset SOURCE_DATE_EPOCH
      if [ ! -f properdocs.yml ]; then
        echo "no properdocs.yml in $(pwd)" >&2
        exit 1
      fi
      uv sync --group docs --quiet
      exec uv run --no-sync properdocs build --strict --clean "$@"
    '')

    (makeScript "docs-deploy" ''
      export MATURIN_IMPORT_HOOK_ENABLED=0
      unset SOURCE_DATE_EPOCH
      if [ ! -f properdocs.yml ]; then
        echo "no properdocs.yml in $(pwd)" >&2
        exit 1
      fi
      host=$(${gnugrep}/bin/grep -m1 '^site_url:' properdocs.yml \
        | sed -E 's|.*://||; s|/.*||')
      if [ -z "$host" ]; then
        echo "could not derive host from mkdocs.yml site_url" >&2
        exit 1
      fi

      dry=()
      if [ "''${1:-}" = "--dry-run" ] || [ "''${1:-}" = "-n" ]; then
        dry=(--dry-run)
        echo "DRY RUN: nothing will be written to edge."
      fi

      uv sync --group docs --quiet
      uv run --no-sync properdocs build --strict --clean

      echo "Target: edge:/var/www/$host/"
      rsync -avL --mkpath --delete-after "''${dry[@]}" \
        site/ "edge:/var/www/$host/"
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
  '';
in
pkgs.mkShell {
  buildInputs = packages';
  shellHook = shell';
}
