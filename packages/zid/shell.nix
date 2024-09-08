{ isDevelopment ? true }:

let
  # Update packages with `nixpkgs-update` command
  pkgs = import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/45508c1098a3fb7140ae3d86414cee8f5ee7511c.tar.gz") { };

  packages' = with pkgs; [
    coreutils
    python313
    uv
    ruff
    gcc14
    rustc
    cargo
    maturin

    (writeShellScriptBin "run-tests" ''
      python -m pytest . \
        --verbose \
        --no-header \
        --cov zid \
        --cov-report "''${1:-xml}"
    '')
    (writeShellScriptBin "nixpkgs-update" ''
      hash=$(
        curl --silent --location \
        https://prometheus.nixos.org/api/v1/query \
        -d "query=channel_revision{channel=\"nixpkgs-unstable\"}" | \
        grep --only-matching --extended-regexp "[0-9a-f]{40}")
      sed -i -E "s|/nixpkgs/archive/[0-9a-f]{40}\.tar\.gz|/nixpkgs/archive/$hash.tar.gz|" shell.nix
      echo "Nixpkgs updated to $hash"
    '')
  ];

  shell' = with pkgs; lib.optionalString isDevelopment ''
    current_python=$(readlink -e .venv/bin/python || echo "")
    current_python=''${current_python%/bin/*}
    [ "$current_python" != "${python313}" ] && rm -rf .venv/

    export SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt

    echo "Installing Python dependencies"
    echo "${python313}/bin/python" > .python-version
    NIX_ENFORCE_PURITY=0 uv sync --frozen

    echo "Activating Python virtual environment"
    source .venv/bin/activate

    # Development environment variables
    export PYTHONNOUSERSITE=1
    export TZ=UTC
  '';
in
pkgs.mkShellNoCC {
  buildInputs = packages';
  shellHook = shell';
}
