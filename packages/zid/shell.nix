{ isDevelopment ? true }:

let
  # Update packages with `nixpkgs-update` command
  pkgs = import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/0aeab749216e4c073cece5d34bc01b79e717c3e0.tar.gz") { };

  packages' = with pkgs; [
    python313
    poetry
    ruff

    (writeShellScriptBin "run-tests" ''
      set -e
      python -m pytest . \
        --verbose \
        --no-header \
        --cov zid \
        --cov-report "''${1:-xml}"
    '')
    (writeShellScriptBin "nixpkgs-update" ''
      set -e
      hash=$(git ls-remote https://github.com/NixOS/nixpkgs nixpkgs-unstable | cut -f 1)
      sed -i -E "s|/nixpkgs/archive/[0-9a-f]{40}\.tar\.gz|/nixpkgs/archive/$hash.tar.gz|" shell.nix
      echo "Nixpkgs updated to $hash"
    '')
  ];

  shell' = with pkgs; lib.optionalString isDevelopment ''
    current_python=$(readlink -e .venv/bin/python || echo "")
    current_python=''${current_python%/bin/*}
    [ "$current_python" != "${python313}" ] && rm -rf .venv/

    echo "Installing Python dependencies"
    poetry env use ${python313}/bin/python
    poetry install --compile

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
