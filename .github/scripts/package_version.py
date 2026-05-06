import argparse
import os
import re
from pathlib import Path

import tomllib

VERSION_RE = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+\Z")


def fail(message: str) -> None:
    raise SystemExit(message)


def validate_version(version: str) -> None:
    if not VERSION_RE.fullmatch(version):
        fail(f"invalid version {version!r}; expected X.Y.Z")


def package_path(package: str) -> Path:
    path = Path(package)
    if path.name != package or not (path / "pyproject.toml").is_file():
        valid = ", ".join(
            sorted(
                path.parent.name
                for path in Path(".").glob("*/pyproject.toml")
            )
        )
        fail(f"unknown package {package!r}; valid packages: {valid}")
    return path


def load_toml(path: Path) -> dict:
    with path.open("rb") as file:
        return tomllib.load(file)


def package_info(package: str) -> dict[str, str]:
    path = package_path(package)
    pyproject = load_toml(path / "pyproject.toml")
    requires_python = pyproject["project"].get("requires-python", "")
    cargo_toml = path / "Cargo.toml"

    if cargo_toml.is_file():
        cargo = load_toml(cargo_toml)
        module_name = pyproject["tool"]["maturin"]["module-name"]
        return {
            "import-name": module_name.split(".", maxsplit=1)[0],
            "is-rusty": "true",
            "package": package,
            "requires-python": requires_python,
            "type": "python-rust",
            "version": cargo["package"]["version"],
            "version_path": cargo_toml.as_posix(),
            "crate": cargo["package"]["name"],
        }

    version_path = path / pyproject["tool"]["hatch"]["version"]["path"]
    return {
        "import-name": version_path.relative_to(path).parts[0],
        "is-rusty": "false",
        "package": package,
        "requires-python": requires_python,
        "type": "python",
        "version": read_python_version(version_path),
        "version_path": version_path.as_posix(),
    }


def read_python_version(path: Path) -> str:
    match = re.search(
        r"(?m)^__version__\s*=\s*(['\"])([^'\"]+)\1[ \t]*$",
        path.read_text(),
    )
    if match is None:
        fail(f"could not find __version__ in {path}")
    return match.group(2)


def set_rust_version(path: Path, version: str) -> None:
    text = path.read_text()
    pattern = re.compile(
        r"(?ms)^(\[package\]\s+.*?^version\s*=\s*\")([^\"]+)(\")"
    )
    text, count = pattern.subn(rf"\g<1>{version}\3", text, count=1)
    if count != 1:
        fail(f"could not update [package].version in {path}")
    path.write_text(text)


def set_python_version(path: Path, version: str) -> None:
    text = path.read_text()
    pattern = re.compile(
        r"(?m)^(__version__\s*=\s*)(['\"])([^'\"]+)(\2)([ \t]*)$"
    )
    text, count = pattern.subn(rf"\g<1>\g<2>{version}\g<4>\g<5>", text, count=1)
    if count != 1:
        fail(f"could not update __version__ in {path}")
    path.write_text(text)


def write_outputs(outputs: dict[str, str]) -> None:
    for key, value in outputs.items():
        print(f"{key}={value}")

    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path is not None:
        with Path(output_path).open("a") as file:
            for key, value in outputs.items():
                file.write(f"{key}={value}\n")


def command_info(args: argparse.Namespace) -> None:
    write_outputs(package_info(args.package))


def command_set(args: argparse.Namespace) -> None:
    validate_version(args.version)
    info = package_info(args.package)

    if info["type"] == "python-rust":
        set_rust_version(Path(info["version_path"]), args.version)
    else:
        set_python_version(Path(info["version_path"]), args.version)

    info = package_info(args.package)
    if info["version"] != args.version:
        fail(
            f"version update failed for {args.package}: "
            f"expected {args.version}, got {info['version']}"
        )
    write_outputs(info)


def command_check(args: argparse.Namespace) -> None:
    info = package_info(args.package)
    if info["version"] != args.version:
        fail(
            f"version mismatch for {args.package}: "
            f"expected {args.version}, got {info['version']}"
        )
    write_outputs(info)


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)

    info_parser = subparsers.add_parser("info")
    info_parser.add_argument("package")
    info_parser.set_defaults(func=command_info)

    set_parser = subparsers.add_parser("set")
    set_parser.add_argument("package")
    set_parser.add_argument("version")
    set_parser.set_defaults(func=command_set)

    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("package")
    check_parser.add_argument("version")
    check_parser.set_defaults(func=command_check)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    try:
        main()
    except KeyError as error:
        raise SystemExit(f"missing expected metadata key: {error}") from error
