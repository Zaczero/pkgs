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
    pyright
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
