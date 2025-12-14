{ }:

let
  pkgs =
    import
      (fetchTarball "https://github.com/NixOS/nixpkgs/archive/5d6bdbddb4695a62f0d00a3620b37a15275a5093.tar.gz")
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

  python' = pkgs.python314;

  packages' = with pkgs; [
    coreutils
    curl
    jq
    python'
    uv
    hatch
    ruff
    pyright

    (makeScript "activate" ''
      if [ -f pyproject.toml ]; then
        current_python=$(readlink -e .venv/bin/python || echo "")
        current_python=''${current_python%/bin/*}
        [ "$current_python" != "${python'}" ] && rm -rf .venv/

        uv sync
        source .venv/bin/activate
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
    export PYTHONNOUSERSITE=1
    export PYTHONPATH=""
    export COVERAGE_CORE=sysmon
    export UV_DOWNLOADS=never
    export UV_NATIVE_TLS=true
    export UV_PYTHON_PREFERENCE=only-system
    export UV_PYTHON="${python'}/bin/python"
    export PYTEST_ADDOPTS="--quiet --import-mode=importlib --strict-markers --strict-config"
  '';
in
pkgs.mkShell {
  buildInputs = packages';
  shellHook = shell';
}
