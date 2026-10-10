"""Native image, source digest and wheel-content inspection."""

from __future__ import annotations

import base64
import binascii
import csv
import hashlib
import importlib.metadata
import importlib.util
import io
import re
import struct
import tarfile
import zipfile
from itertools import chain
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TYPE_CHECKING, Any

from _artifact_common import (
    ABSOLUTE_BUILD_PATH,
    HEX_DIGEST,
    IDENTIFIER,
    LIBTIFF,
    LICENSE_SCRIPT,
    ROOT,
    SOURCE_TOOL_MEMBERS,
    _sha256_bytes,
    _target_architecture,
    release_target_policy,
    sha256_file,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence
    from zipfile import ZipFile
X86_64_V2_TARGET_FEATURES = frozenset({
    'cmpxchg16b',
    'lahfsahf',
    'popcnt',
    'sse3',
    'sse4.1',
    'sse4.2',
    'ssse3',
})
X86_64_V3_PLUS_TARGET_FEATURES = frozenset({
    'avx',
    'avx2',
    'bmi1',
    'bmi2',
    'fma',
    'f16c',
    'lzcnt',
    'movbe',
    'xsave',
})
_TARGET_FEATURE_LIST = re.compile(rb'[a-z0-9_.]+(?:,[a-z0-9_.]+)+')
TEXT_SUFFIXES = frozenset({
    '.json',
    '.lock',
    '.md',
    '.py',
    '.pyi',
    '.rs',
    '.toml',
    '.txt',
    '.yaml',
    '.yml',
})
PRINTABLE_STRING = re.compile(rb'[\x20-\x7e]{4,}')
NON_BUILD_PATH_PREFIXES = ('/tmp/gometry-crs-',)  # noqa: S108


def build_identifier_policy(identifier: str) -> dict[str, Any]:
    """Resolve one supported CIBW identifier to its raw Rust release target."""
    match = IDENTIFIER.fullmatch(identifier)
    if match is None:
        raise AssertionError(f'invalid cibuildwheel identifier: {identifier!r}')
    python_tag = match.group('python')
    python_match = re.fullmatch(r'cp(?P<version>[0-9]{2,3})(?P<gil>t?)', python_tag)
    if python_match is None:
        raise AssertionError(f'unsupported gometry wheel Python tag: {python_tag!r}')
    python_version = python_match.group('version')
    if python_version not in {'311', '312', '313', '314'}:
        raise AssertionError(f'unsupported gometry wheel Python tag: {python_tag!r}')
    if python_match.group('gil') and python_version != '314':
        raise AssertionError(f'free-threaded Python tag is unsupported: {python_tag!r}')

    platform_tag = match.group('platform').lower()
    if platform_tag.startswith(('manylinux_', 'musllinux_')):
        family, _, architecture = platform_tag.partition('_')
        raw_arch = {'x86_64': 'x86_64', 'aarch64': 'aarch64'}.get(architecture)
        if raw_arch is None:
            raise AssertionError(
                f'unsupported Linux wheel architecture: {platform_tag!r}'
            )
        libc = {'manylinux': 'gnu', 'musllinux': 'musl'}[family]
        target = f'{raw_arch}-unknown-linux-{libc}'
        platform_family = f'linux-{libc}'
    elif platform_tag.startswith('macosx_'):
        architecture = platform_tag.removeprefix('macosx_')
        raw_arch = {
            'x86_64': 'x86_64',
            'arm64': 'aarch64',
        }.get(architecture)
        if raw_arch is None:
            raise AssertionError(
                f'unsupported macOS wheel architecture: {platform_tag!r}'
            )
        target = f'{raw_arch}-apple-darwin'
        platform_family = 'darwin'
    elif platform_tag.startswith('win_'):
        architecture = platform_tag.removeprefix('win_')
        raw_arch = {
            'amd64': 'x86_64',
            'arm64': 'aarch64',
        }.get(architecture)
        if raw_arch is None:
            raise AssertionError(
                f'unsupported Windows wheel architecture: {platform_tag!r}'
            )
        target = f'{raw_arch}-pc-windows-msvc'
        platform_family = 'windows-msvc'
    else:
        raise AssertionError(f'unsupported gometry wheel platform: {platform_tag!r}')
    return {
        **release_target_policy(target),
        'raw_target_arch': raw_arch,
        'platform_family': platform_family,
        'python_tag': python_tag,
    }


def is_hidden_native_path(relative: str) -> bool:
    parts = PurePosixPath(relative).parts
    native_index = next(
        (
            index
            for index in range(len(parts) - 1)
            if parts[index : index + 2] == ('gometry', 'native')
        ),
        None,
    )
    return native_index is not None and any(
        part.startswith('.') for part in parts[native_index + 2 :]
    )


def has_hidden_component(relative: str) -> bool:
    return any(part.startswith('.') for part in PurePosixPath(relative).parts)


def _source_input_records(
    root: Path,
) -> tuple[list[tuple[str, Path]], list[tuple[str, Path]]]:
    fixed = [
        ('gometry/build.rs', root / 'build.rs'),
    ]
    source_files: list[tuple[str, Path]] = []
    _collect_source_files(root / 'src', 'gometry/src', source_files)
    native_files: list[tuple[str, Path]] = []
    _collect_source_files(
        root / 'native',
        'gometry/native',
        native_files,
        skip_hidden_native=True,
    )
    return [*fixed, *source_files, *native_files], native_files


def _collect_source_files(
    root: Path,
    logical_root: str,
    files: list[tuple[str, Path]],
    *,
    skip_hidden_native: bool = False,
) -> None:
    try:
        entries = sorted(root.iterdir(), key=lambda path: path.name)
    except OSError as error:
        raise AssertionError(
            f'cannot read build input directory {root}: {error}'
        ) from error
    for path in entries:
        if path.name == 'Cargo.toml':
            continue
        logical = f'{logical_root}/{path.name}'
        if skip_hidden_native and is_hidden_native_path(logical):
            continue
        if path.is_dir():
            _collect_source_files(
                path,
                logical,
                files,
                skip_hidden_native=skip_hidden_native,
            )
        elif path.is_file():
            files.append((logical, path))


def _digest_source_files(files: Sequence[tuple[str, Path]]) -> str:
    digest = hashlib.sha256()
    for logical, path in sorted(files, key=lambda item: item[0]):
        try:
            payload = path.read_bytes()
        except OSError as error:
            raise AssertionError(f'cannot read build input {path}: {error}') from error
        digest.update(logical.encode('utf-8'))
        digest.update(b'\0')
        digest.update(payload)
        digest.update(b'\0')
    return digest.hexdigest()


def source_input_digests(root: Path = ROOT) -> dict[str, str]:
    """Match the source and native digests emitted by ``gometry/build.rs``."""
    files, native_files = _source_input_records(root)
    return {
        'source_inputs_sha256': _digest_source_files(files),
        'native_source_inputs_sha256': _digest_source_files(native_files),
    }


def python_member_hashes(package_dir: Path) -> dict[str, str]:
    """Hash the actual Python package members admitted to a wheel."""
    package_dir = package_dir.resolve()
    if not package_dir.is_dir():
        raise AssertionError(f'Python package directory is missing: {package_dir}')
    hashes: dict[str, str] = {}
    for path in sorted(package_dir.rglob('*')):
        if not path.is_file():
            continue
        relative = path.relative_to(package_dir)
        if not is_python_package_member(relative.as_posix()):
            continue
        logical = (
            PurePosixPath('gometry') / PurePosixPath(relative.as_posix())
        ).as_posix()
        hashes[logical] = sha256_file(path)
    if not hashes:
        raise AssertionError(f'Python package directory has no members: {package_dir}')
    return dict(sorted(hashes.items()))


def is_python_package_member(relative: str) -> bool:
    path = PurePosixPath(relative)
    return (
        '__pycache__' not in path.parts
        and path.suffix not in {'.pyc', '.pyo'}
        and not _is_native_library_member(path.name)
        and not path.name.endswith(('~', '.swp', '.swo', '.orig'))
        and path.name != '.DS_Store'
    )


def libtiff_source_members() -> set[str]:
    return {
        path.relative_to(LIBTIFF).as_posix()
        for path in LIBTIFF.rglob('*')
        if path.is_file()
        and not has_hidden_component(path.relative_to(LIBTIFF).as_posix())
    }


def inspect_libtiff_sdist(path: Path, expected: set[str] | None = None) -> None:
    with tarfile.open(path) as archive:
        inspect_libtiff_sdist_members(archive.getnames(), path, expected)


def inspect_libtiff_sdist_members(
    names: list[str],
    path: Path,
    expected: set[str] | None = None,
    *,
    allow_hidden_native: bool = False,
) -> None:
    root = f'gometry-{path.name.removeprefix("gometry-").removesuffix(".tar.gz")}'
    prefix = f'{root}/gometry/native/libtiff-sys/libtiff/'
    members = [
        name.removeprefix(prefix)
        for name in names
        if name.startswith(prefix) and not name.endswith('/')
    ]
    if len(members) != len(set(members)):
        raise AssertionError('sdist contains duplicate libtiff paths')
    for member in members:
        member_path = Path(member)
        if not member or member_path.is_absolute() or '..' in member_path.parts:
            raise AssertionError(f'sdist contains invalid libtiff path: {member!r}')
    actual = set(members)
    if allow_hidden_native:
        actual = {member for member in actual if not has_hidden_component(member)}
    expected = libtiff_source_members() if expected is None else expected
    if actual != expected:
        raise AssertionError(
            'sdist libtiff file set differs from initialized submodule: '
            f'missing={sorted(expected - actual)[:20]}, '
            f'extra={sorted(actual - expected)[:20]}'
        )


def is_text_member(name: str) -> bool:
    path = Path(name)
    return path.suffix.lower() in TEXT_SUFFIXES or path.name in {
        'METADATA',
        'PKG-INFO',
        'WHEEL',
    }


def _is_native_library_member(name: str) -> bool:
    lower = name.lower()
    return lower.endswith(('.so', '.pyd', '.dylib', '.dll')) or '.so.' in lower


def _printable_strings(value: bytes) -> Iterator[str]:
    for match in PRINTABLE_STRING.finditer(value):
        yield match.group().decode('ascii')


def content_leaks(
    members: dict[str, str],
    binary_members: dict[str, bytes] | None = None,
) -> list[str]:
    leaks: list[str] = []
    binary_values = (
        (name, value)
        for name, payload in (binary_members or {}).items()
        for value in _printable_strings(payload)
    )
    for name, value in chain(members.items(), binary_values):
        if any(name.endswith(f'/{member}') for member in SOURCE_TOOL_MEMBERS):
            continue
        for match in ABSOLUTE_BUILD_PATH.finditer(value):
            if any(
                value.startswith(prefix, match.start())
                for prefix in NON_BUILD_PATH_PREFIXES
            ):
                continue
            leaks.append(f'{name}: absolute build path {match.group(0)!r}')
    return leaks


def native_notice_markers() -> list[tuple[str, str, str]]:
    """Component name, license ID, and full pinned notice text."""
    spec = importlib.util.spec_from_file_location(
        'gometry_license_generator', LICENSE_SCRIPT
    )
    if spec is None or spec.loader is None:
        raise AssertionError(f'could not load license generator {LICENSE_SCRIPT}')
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    try:
        entries = generator._native_notice_texts('gometry')
    except SystemExit as error:
        raise AssertionError(str(error)) from error
    return [
        (component['name'], component['license'], text) for component, text in entries
    ]


def verify_native_notice_texts(text: str, artifact: Path) -> None:
    for component, license_id, notice in native_notice_markers():
        if (
            component not in text
            or f'Licensed under {license_id}.' not in text
            or notice not in text
        ):
            raise AssertionError(
                f'{artifact} is missing the full native notice for {component!r}'
            )


def _unpack_native(
    fmt: str, payload: bytes, offset: int, artifact: Path
) -> tuple[Any, ...]:
    size = struct.calcsize(fmt)
    if offset < 0 or offset + size > len(payload):
        raise AssertionError(
            f'{artifact} native extension has a truncated inspectable header'
        )
    return struct.unpack_from(fmt, payload, offset)


def _native_name(payload: bytes) -> str:
    return payload.split(b'\0', 1)[0].decode('ascii', errors='replace')


def _elf_virtual_file_offset(
    address: int,
    size: int,
    loads: Sequence[tuple[int, int, int]],
    payload: bytes,
    artifact: Path,
    label: str,
) -> int:
    if address < 0 or size <= 0:
        raise AssertionError(f'{artifact} native ELF {label} has an invalid range')
    file_offsets = {
        file_start + address - virtual_start
        for virtual_start, file_start, file_size in loads
        if virtual_start <= address
        and address + size <= virtual_start + file_size
        and file_start + address - virtual_start + size <= len(payload)
    }
    if len(file_offsets) != 1:
        raise AssertionError(
            f'{artifact} native ELF {label} is not in one unambiguous file-backed PT_LOAD'
        )
    return next(iter(file_offsets))


def _elf_dynamic_string(
    payload: bytes,
    string_offset: int,
    string_size: int,
    index: int,
    artifact: Path,
    label: str,
) -> str:
    if index < 0 or index >= string_size:
        raise AssertionError(f'{artifact} native ELF {label} string offset is invalid')
    start = string_offset + index
    end = payload.find(b'\0', start, string_offset + string_size)
    if end < 0:
        raise AssertionError(f'{artifact} native ELF {label} string is truncated')
    try:
        value = payload[start:end].decode('ascii')
    except UnicodeDecodeError as error:
        raise AssertionError(
            f'{artifact} native ELF {label} string is not ASCII'
        ) from error
    if not value:
        raise AssertionError(f'{artifact} native ELF {label} string is empty')
    return value


def _elf_dynamic_facts(payload: bytes, artifact: Path) -> dict[str, Any]:
    if len(payload) < 64:
        raise AssertionError(f'{artifact} native ELF header is truncated')
    elf_class = payload[4]
    data_encoding = payload[5]
    endian = {(1, 1): '<', (1, 2): '>', (2, 1): '<', (2, 2): '>'}.get((
        elf_class,
        data_encoding,
    ))
    if endian is None:
        raise AssertionError(f'{artifact} native ELF encoding is unsupported')
    if elf_class == 2:
        machine = _unpack_native(f'{endian}H', payload, 18, artifact)[0]
        phoff = _unpack_native(f'{endian}Q', payload, 32, artifact)[0]
        phentsize = _unpack_native(f'{endian}H', payload, 54, artifact)[0]
        phnum = _unpack_native(f'{endian}H', payload, 56, artifact)[0]
        phfmt = f'{endian}IIQQQQQQ'
        dynfmt = f'{endian}qQ'
    else:
        machine = _unpack_native(f'{endian}H', payload, 18, artifact)[0]
        phoff = _unpack_native(f'{endian}I', payload, 28, artifact)[0]
        phentsize = _unpack_native(f'{endian}H', payload, 42, artifact)[0]
        phnum = _unpack_native(f'{endian}H', payload, 44, artifact)[0]
        phfmt = f'{endian}IIIIIIII'
        dynfmt = f'{endian}iI'
    minimum_ph = struct.calcsize(phfmt)
    if phentsize < minimum_ph or not phnum:
        raise AssertionError(f'{artifact} native ELF has no program-header table')
    loads: list[tuple[int, int, int]] = []
    dynamic: list[tuple[int, int]] = []
    for index in range(phnum):
        header = _unpack_native(
            phfmt,
            payload,
            phoff + index * phentsize,
            artifact,
        )
        p_type = header[0]
        if elf_class == 2:
            p_offset, p_vaddr, p_filesz = header[2], header[3], header[5]
        else:
            p_offset, p_vaddr, p_filesz = header[1], header[2], header[4]
        if p_type == 1:
            if p_offset + p_filesz > len(payload):
                raise AssertionError(f'{artifact} native ELF PT_LOAD is truncated')
            loads.append((p_vaddr, p_offset, p_filesz))
        elif p_type == 2:
            dynamic.append((p_offset, p_filesz))
    if not dynamic or not loads:
        raise AssertionError(f'{artifact} native ELF has no dynamic dependency table')
    entry_size = struct.calcsize(dynfmt)
    tags: list[tuple[int, int]] = []
    for offset, size in dynamic:
        if offset + size > len(payload) or size % entry_size:
            raise AssertionError(f'{artifact} native ELF dynamic table is truncated')
        for cursor in range(offset, offset + size, entry_size):
            tag, value = _unpack_native(dynfmt, payload, cursor, artifact)
            if tag == 0:
                break
            tags.append((tag, value))
    string_table = next((value for tag, value in tags if tag == 5), None)
    string_size = next((value for tag, value in tags if tag == 10), None)
    if string_table is None or string_size is None:
        raise AssertionError(f'{artifact} native ELF lacks its dynamic string table')
    string_offset = _elf_virtual_file_offset(
        string_table,
        string_size,
        loads,
        payload,
        artifact,
        'dynamic string table',
    )
    needed: list[str] = []
    for tag, value in tags:
        if tag != 1:
            continue
        needed.append(
            _elf_dynamic_string(
                payload,
                string_offset,
                string_size,
                value,
                artifact,
                'dependency name',
            )
        )
    libc_names = [
        name for name in needed if name == 'libc.so.6' or name.startswith('libc.musl-')
    ]
    if len(libc_names) > 1:
        raise AssertionError(
            f'{artifact} native ELF has multiple dynamic libc dependencies: {needed}'
        )
    libc = None if not libc_names else 'gnu' if libc_names[0] == 'libc.so.6' else 'musl'
    version_need_addresses = [value for tag, value in tags if tag == 0x6FFFFFFE]
    version_need_counts = [value for tag, value in tags if tag == 0x6FFFFFFF]
    if bool(version_need_addresses) != bool(version_need_counts):
        raise AssertionError(f'{artifact} native ELF version-need table is incomplete')
    if len(version_need_addresses) > 1 or len(version_need_counts) > 1:
        raise AssertionError(f'{artifact} native ELF has duplicate version-need tags')
    versions: set[str] = set()
    if version_need_addresses:
        current_need_address = version_need_addresses[0]
        need_count = version_need_counts[0]
        if need_count == 0:
            raise AssertionError(f'{artifact} native ELF version-need table is empty')
        for need_index in range(need_count):
            need_offset = _elf_virtual_file_offset(
                current_need_address,
                16,
                loads,
                payload,
                artifact,
                'version-need record',
            )
            version, auxiliary_count, file_index, auxiliary_offset, next_offset = (
                _unpack_native(f'{endian}HHIII', payload, need_offset, artifact)
            )
            if version != 1 or auxiliary_count == 0 or auxiliary_offset < 16:
                raise AssertionError(
                    f'{artifact} native ELF version-need record is malformed'
                )
            dependency = _elf_dynamic_string(
                payload,
                string_offset,
                string_size,
                file_index,
                artifact,
                'version-need dependency',
            )
            if dependency not in needed:
                raise AssertionError(
                    f'{artifact} native ELF version-need record names a non-needed library'
                )
            auxiliary_address = current_need_address + auxiliary_offset
            for auxiliary_index in range(auxiliary_count):
                auxiliary_file_offset = _elf_virtual_file_offset(
                    auxiliary_address,
                    16,
                    loads,
                    payload,
                    artifact,
                    'version-need auxiliary record',
                )
                (
                    _hash,
                    _flags,
                    _other,
                    name_index,
                    auxiliary_next,
                ) = _unpack_native(
                    f'{endian}IHHII',
                    payload,
                    auxiliary_file_offset,
                    artifact,
                )
                version_name = _elf_dynamic_string(
                    payload,
                    string_offset,
                    string_size,
                    name_index,
                    artifact,
                    'version-need name',
                )
                if re.fullmatch(r'GLIBC_[0-9]+(?:\.[0-9]+)+', version_name):
                    versions.add(version_name)
                if auxiliary_index + 1 < auxiliary_count:
                    if auxiliary_next < 16:
                        raise AssertionError(
                            f'{artifact} native ELF version-need auxiliary chain is truncated'
                        )
                    auxiliary_address += auxiliary_next
                elif auxiliary_next != 0:
                    raise AssertionError(
                        f'{artifact} native ELF version-need auxiliary chain has trailing data'
                    )
            if need_index + 1 < need_count:
                if next_offset < 16:
                    raise AssertionError(
                        f'{artifact} native ELF version-need chain is truncated'
                    )
                current_need_address += next_offset
            elif next_offset != 0:
                raise AssertionError(
                    f'{artifact} native ELF version-need chain has trailing data'
                )
    versions = sorted(versions)
    if libc == 'musl' and versions:
        raise AssertionError(
            f'{artifact} musl ELF contains GNU libc version references'
        )
    architecture = {
        62: 'x86_64',
        183: 'aarch64',
    }.get(machine)
    if architecture is None:
        raise AssertionError(f'{artifact} native ELF machine is unsupported: {machine}')
    return {
        'format': 'ELF',
        'machine': machine,
        'architecture': architecture,
        'libc': libc,
        'needed': sorted(needed),
        'glibc_versions': versions,
    }


def _macho_load_facts(payload: bytes, artifact: Path) -> dict[str, Any]:
    magic = payload[:4]
    thin = {
        b'\xce\xfa\xed\xfe': ('<', 32),
        b'\xfe\xed\xfa\xce': ('>', 32),
        b'\xcf\xfa\xed\xfe': ('<', 64),
        b'\xfe\xed\xfa\xcf': ('>', 64),
    }
    if magic in thin:
        endian, bits = thin[magic]
        header_size = 32 if bits == 64 else 28
        machine = _unpack_native(f'{endian}I', payload, 4, artifact)[0]
        command_count = _unpack_native(f'{endian}I', payload, 16, artifact)[0]
        command_bytes = _unpack_native(f'{endian}I', payload, 20, artifact)[0]
        command_end = header_size + command_bytes
        if command_end > len(payload):
            raise AssertionError(f'{artifact} Mach-O load commands are truncated')
        dependencies: list[str] = []
        rpaths: list[str] = []
        deployments: list[str] = []
        cursor = header_size
        for _ in range(command_count):
            command, size = _unpack_native(f'{endian}II', payload, cursor, artifact)
            if size < 8 or cursor + size > command_end:
                raise AssertionError(f'{artifact} Mach-O load command is invalid')
            if command in {
                0xC,
                0x80000018,
                0x8000001F,
                0x80000020,
                0x80000023,
            }:
                name_offset = _unpack_native(
                    f'{endian}I', payload, cursor + 8, artifact
                )[0]
                if name_offset >= size:
                    raise AssertionError(f'{artifact} Mach-O dylib name is invalid')
                start = cursor + name_offset
                end = payload.find(b'\0', start, cursor + size)
                if end < 0:
                    raise AssertionError(f'{artifact} Mach-O dylib name is truncated')
                dependencies.append(payload[start:end].decode('utf-8', errors='strict'))
            elif command == 0x8000001C:
                path_offset = _unpack_native(
                    f'{endian}I', payload, cursor + 8, artifact
                )[0]
                if path_offset >= size:
                    raise AssertionError(f'{artifact} Mach-O rpath is invalid')
                start = cursor + path_offset
                end = payload.find(b'\0', start, cursor + size)
                if end < 0:
                    raise AssertionError(f'{artifact} Mach-O rpath is truncated')
                rpaths.append(payload[start:end].decode('utf-8', errors='strict'))
            elif command == 0x32:
                platform_id, minos, _sdk, _tools = _unpack_native(
                    f'{endian}IIII', payload, cursor + 8, artifact
                )
                if platform_id == 1:
                    deployments.append(_packed_version(minos))
            elif command == 0x24:
                minimum = _unpack_native(f'{endian}I', payload, cursor + 8, artifact)[0]
                deployments.append(_packed_version(minimum))
            cursor += size
        architecture = {
            0x01000007: 'x86_64',
            0x0100000C: 'arm64',
        }.get(machine)
        if architecture is None:
            raise AssertionError(f'{artifact} Mach-O machine is unsupported: {machine}')
        return {
            'machine': machine,
            'architecture': architecture,
            'needed': sorted(dependencies),
            'rpaths': sorted(rpaths),
            'deployment_targets': sorted(set(deployments)),
        }

    fat = {
        b'\xca\xfe\xba\xbe': ('>', 20),
        b'\xbe\xba\xfe\xca': ('<', 20),
        b'\xca\xfe\xba\xbf': ('>', 32),
        b'\xbf\xba\xfe\xca': ('<', 32),
    }
    if magic not in fat:
        raise AssertionError(f'{artifact} Mach-O magic is invalid')
    endian, entry_size = fat[magic]
    count = _unpack_native(f'{endian}I', payload, 4, artifact)[0]
    slices = []
    for index in range(count):
        entry = 8 + index * entry_size
        if entry_size == 20:
            offset, size = _unpack_native(f'{endian}IIIII', payload, entry, artifact)[
                2:4
            ]
        else:
            offset, size = _unpack_native(f'{endian}IIQQII', payload, entry, artifact)[
                2:4
            ]
        if offset + size > len(payload):
            raise AssertionError(f'{artifact} fat Mach-O slice is truncated')
        slices.append(_macho_load_facts(payload[offset : offset + size], artifact))
    return {
        'machines': sorted({image['machine'] for image in slices}),
        'architectures': sorted({image['architecture'] for image in slices}),
        'needed': sorted({item for image in slices for item in image['needed']}),
        'rpaths': sorted({item for image in slices for item in image['rpaths']}),
        'deployment_targets': sorted({
            item for image in slices for item in image['deployment_targets']
        }),
    }


def _packed_version(value: int) -> str:
    return f'{(value >> 16) & 0xFFFF}.{(value >> 8) & 0xFF}.{value & 0xFF}'


def native_image_facts(payload: bytes, artifact: Path) -> dict[str, Any]:
    if payload.startswith(b'\x7fELF'):
        return _elf_dynamic_facts(payload, artifact)
    if payload.startswith(b'MZ'):
        signature_offset = _unpack_native('<I', payload, 0x3C, artifact)[0]
        signature = _unpack_native('4s', payload, signature_offset, artifact)[0]
        if signature != b'PE\0\0':
            raise AssertionError(f'{artifact} native PE signature is invalid')
        machine = _unpack_native('<H', payload, signature_offset + 4, artifact)[0]
        architecture = {
            0x8664: 'x86_64',
            0xAA64: 'aarch64',
        }.get(machine)
        if architecture is None:
            raise AssertionError(
                f'{artifact} native PE machine is unsupported: {machine}'
            )
        return {'format': 'PE', 'machine': machine, 'architecture': architecture}
    return {'format': 'Mach-O', **_macho_load_facts(payload, artifact)}


def _elf_sections(payload: bytes, artifact: Path) -> list[str]:
    if len(payload) < 16:
        raise AssertionError(f'{artifact} native ELF header is truncated')
    elf_class = payload[4]
    data_encoding = payload[5]
    byte_order = {
        (1, 1): '<',
        (1, 2): '>',
        (2, 1): '<',
        (2, 2): '>',
    }.get((elf_class, data_encoding))
    if byte_order is None:
        raise AssertionError(f'{artifact} native ELF header is unsupported')
    if elf_class == 2:
        section_offset = _unpack_native(f'{byte_order}Q', payload, 40, artifact)[0]
        section_entry_size = _unpack_native(f'{byte_order}H', payload, 58, artifact)[0]
        section_count = _unpack_native(f'{byte_order}H', payload, 60, artifact)[0]
        section_strings = _unpack_native(f'{byte_order}H', payload, 62, artifact)[0]
        section_format = f'{byte_order}IIQQQQIIQQ'
    else:
        section_offset = _unpack_native(f'{byte_order}I', payload, 32, artifact)[0]
        section_entry_size = _unpack_native(f'{byte_order}H', payload, 46, artifact)[0]
        section_count = _unpack_native(f'{byte_order}H', payload, 48, artifact)[0]
        section_strings = _unpack_native(f'{byte_order}H', payload, 50, artifact)[0]
        section_format = f'{byte_order}IIIIIIIIII'
    minimum_entry_size = struct.calcsize(section_format)
    if section_offset == 0 or section_entry_size < minimum_entry_size:
        raise AssertionError(f'{artifact} native ELF has no inspectable section table')
    first = _unpack_native(
        section_format,
        payload,
        section_offset,
        artifact,
    )
    if section_count == 0:
        section_count = first[5]
    if section_strings == 0xFFFF:
        section_strings = first[6]
    if section_strings >= section_count:
        raise AssertionError(f'{artifact} native ELF section strings are invalid')
    section_headers = [
        _unpack_native(
            section_format,
            payload,
            section_offset + index * section_entry_size,
            artifact,
        )
        for index in range(section_count)
    ]
    string_header = section_headers[section_strings]
    string_offset = string_header[4]
    string_size = string_header[5]
    if string_offset + string_size > len(payload):
        raise AssertionError(f'{artifact} native ELF strings are truncated')
    strings = payload[string_offset : string_offset + string_size]
    names: list[str] = []
    for header in section_headers:
        name_offset = header[0]
        if name_offset >= len(strings):
            raise AssertionError(f'{artifact} native ELF section name is invalid')
        names.append(_native_name(strings[name_offset:]))
    return names


def _macho_sections(payload: bytes, artifact: Path) -> list[str]:
    magic = payload[:4]
    thin = {
        b'\xce\xfa\xed\xfe': ('<', 32),
        b'\xfe\xed\xfa\xce': ('>', 32),
        b'\xcf\xfa\xed\xfe': ('<', 64),
        b'\xfe\xed\xfa\xcf': ('>', 64),
    }
    if magic in thin:
        endian, bits = thin[magic]
        header_size = 32 if bits == 64 else 28
        command_count = _unpack_native(f'{endian}I', payload, 16, artifact)[0]
        command_size = _unpack_native(f'{endian}I', payload, 20, artifact)[0]
        command_end = header_size + command_size
        if command_end > len(payload):
            raise AssertionError(f'{artifact} native Mach-O commands are truncated')
        sections: list[str] = []
        cursor = header_size
        segment_command = 0x19 if bits == 64 else 0x1
        integer = f'{endian}I'
        for _ in range(command_count):
            command, size = _unpack_native(f'{endian}II', payload, cursor, artifact)
            if size < 8 or cursor + size > command_end:
                raise AssertionError(f'{artifact} native Mach-O command is invalid')
            if command == segment_command:
                segment_name = _native_name(payload[cursor + 8 : cursor + 24])
                section_count_offset = cursor + (64 if bits == 64 else 48)
                section_start = cursor + (72 if bits == 64 else 56)
                section_size = 80 if bits == 64 else 68
                section_count = _unpack_native(
                    integer, payload, section_count_offset, artifact
                )[0]
                if segment_name == '__DWARF':
                    sections.append('__DWARF')
                for index in range(section_count):
                    section_offset = section_start + index * section_size
                    if section_offset + section_size > cursor + size:
                        raise AssertionError(
                            f'{artifact} native Mach-O sections are truncated'
                        )
                    sections.append(
                        _native_name(payload[section_offset : section_offset + 16])
                    )
            cursor += size
        return sections
    fat = {
        b'\xca\xfe\xba\xbe': ('>', 20),
        b'\xbe\xba\xfe\xca': ('<', 20),
        b'\xca\xfe\xba\xbf': ('>', 32),
        b'\xbf\xba\xfe\xca': ('<', 32),
    }
    if magic not in fat:
        raise AssertionError(f'{artifact} native Mach-O header is unsupported')
    endian, entry_size = fat[magic]
    integer = f'{endian}I'
    archive_count = _unpack_native(integer, payload, 4, artifact)[0]
    sections = []
    for index in range(archive_count):
        entry_offset = 8 + index * entry_size
        if entry_size == 20:
            slice_offset, slice_size = _unpack_native(
                f'{endian}IIIII', payload, entry_offset, artifact
            )[2:4]
        else:
            slice_offset, slice_size = _unpack_native(
                f'{endian}IIQQII', payload, entry_offset, artifact
            )[2:4]
        if slice_offset + slice_size > len(payload):
            raise AssertionError(f'{artifact} native Mach-O slice is truncated')
        sections.extend(
            _macho_sections(payload[slice_offset : slice_offset + slice_size], artifact)
        )
    return sections


def _pe_sections(payload: bytes, artifact: Path) -> list[str]:
    if len(payload) < 64:
        raise AssertionError(f'{artifact} native PE header is truncated')
    signature_offset = _unpack_native('<I', payload, 0x3C, artifact)[0]
    signature = _unpack_native('4s', payload, signature_offset, artifact)[0]
    if signature != b'PE\0\0':
        raise AssertionError(f'{artifact} native PE signature is invalid')
    coff = signature_offset + 4
    section_count = _unpack_native('<H', payload, coff + 2, artifact)[0]
    optional_size = _unpack_native('<H', payload, coff + 16, artifact)[0]
    optional = coff + 20
    optional_magic = _unpack_native('<H', payload, optional, artifact)[0]
    directory_offset = {0x10B: 96, 0x20B: 112}.get(optional_magic)
    if directory_offset is None:
        raise AssertionError(f'{artifact} native PE optional header is unsupported')
    debug_directory = _unpack_native(
        '<II',
        payload,
        optional + directory_offset + 6 * 8,
        artifact,
    )
    if debug_directory[1]:
        raise AssertionError(f'{artifact} native extension contains PE debug data')
    sections_start = optional + optional_size
    return [
        _native_name(
            _unpack_native('8s', payload, sections_start + index * 40, artifact)[0]
        )
        for index in range(section_count)
    ]


def _verify_native_debug_sections(payload: bytes, artifact: Path) -> None:
    if payload.startswith(b'\x7fELF'):
        sections = _elf_sections(payload, artifact)
        format_name = 'ELF'
        debug = lambda name: (
            name.startswith(('.debug', '.zdebug'))
            or name in {'.gnu_debugdata', '.gnu_debuglink', '.gdb_index'}
        )
    elif payload[:4] in {
        b'\xce\xfa\xed\xfe',
        b'\xfe\xed\xfa\xce',
        b'\xcf\xfa\xed\xfe',
        b'\xfe\xed\xfa\xcf',
        b'\xca\xfe\xba\xbe',
        b'\xbe\xba\xfe\xca',
        b'\xca\xfe\xba\xbf',
        b'\xbf\xba\xfe\xca',
    }:
        sections = _macho_sections(payload, artifact)
        format_name = 'Mach-O'
        debug = lambda name: (
            name == '__DWARF' or name.startswith(('__debug', '__zdebug'))
        )
    elif payload.startswith(b'MZ'):
        sections = _pe_sections(payload, artifact)
        format_name = 'PE'
        debug = lambda name: name.startswith('.debug')
    else:
        raise AssertionError(
            f'{artifact} native extension format has no inspectable debug sections'
        )
    if forbidden := sorted(name for name in sections if debug(name)):
        raise AssertionError(
            f'{artifact} native {format_name} extension contains debug sections: '
            f'{forbidden[:20]}'
        )


def _record_hash(algorithm: str, encoded: str, member: str) -> Any:
    algorithms = {
        'sha256',
        'sha384',
        'sha512',
        'sha3_256',
        'sha3_384',
        'sha3_512',
        'blake2b',
        'blake2s',
    }
    if algorithm not in algorithms:
        raise AssertionError(f'wheel RECORD uses a weak or unsupported hash: {member}')
    try:
        digest = hashlib.new(algorithm)
    except ValueError as error:
        raise AssertionError(
            f'wheel RECORD hash is unavailable in this Python: {algorithm}'
        ) from error
    if digest.digest_size < 32:
        raise AssertionError(f'wheel RECORD hash is weaker than SHA-256: {member}')
    if not encoded or '=' in encoded:
        raise AssertionError(f'wheel RECORD digest is not canonical: {member}')
    try:
        decoded = base64.b64decode(
            encoded + '=' * (-len(encoded) % 4),
            altchars=b'-_',
            validate=True,
        )
    except (ValueError, binascii.Error) as error:
        raise AssertionError(f'wheel RECORD digest is malformed: {member}') from error
    if len(decoded) != digest.digest_size:
        raise AssertionError(f'wheel RECORD digest has the wrong length: {member}')
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b'=').decode('ascii')
    if canonical != encoded:
        raise AssertionError(f'wheel RECORD digest is not canonical: {member}')
    return digest


def _wheel_member_name(name: str) -> tuple[str, bool]:
    if (
        not name
        or name.startswith('/')
        or '\\' in name
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in name)
    ):
        raise AssertionError(f'wheel contains an unsafe archive path: {name!r}')
    directory = name.endswith('/')
    literal = name[:-1] if directory else name
    path = PurePosixPath(literal)
    if (
        not literal
        or path.is_absolute()
        or PureWindowsPath(literal).drive
        or PureWindowsPath(literal).root
        or str(path) != literal
        or any(part in {'', '.', '..'} for part in path.parts)
    ):
        raise AssertionError(f'wheel contains a non-canonical archive path: {name!r}')
    return literal, directory


def inspect_wheel_members(wheel: ZipFile) -> dict[str, Any]:
    """Validate one wheel RECORD while hashing and reading each member once."""
    infos = wheel.infolist()
    names: set[str] = set()
    file_names: dict[str, Any] = {}
    directory_names: set[str] = set()
    for info in infos:
        literal, directory = _wheel_member_name(info.filename)
        if (
            info.filename in names
            or literal in file_names
            or literal in directory_names
        ):
            raise AssertionError(f'wheel contains duplicate members: {info.filename}')
        names.add(info.filename)
        if directory or info.is_dir():
            directory_names.add(literal)
        else:
            file_names[literal] = info
    if set(file_names) & directory_names:
        raise AssertionError('wheel has a file and directory with the same path')

    record_names = [
        name
        for name in file_names
        if len(PurePosixPath(name).parts) >= 2
        and PurePosixPath(name).parts[-2].endswith('.dist-info')
        and PurePosixPath(name).name == 'RECORD'
    ]
    dist_infos = {
        part
        for name in file_names
        for part in PurePosixPath(name).parts
        if part.endswith('.dist-info')
    }
    if len(record_names) != 1 or len(dist_infos) != 1:
        raise AssertionError(f'wheel must contain one canonical RECORD: {record_names}')
    record_name = record_names[0]
    dist_info = PurePosixPath(record_name).parts[-2]
    record_payload = wheel.read(file_names[record_name])
    try:
        rows = list(
            csv.reader(
                io.StringIO(record_payload.decode('utf-8'), newline=''),
                strict=True,
            )
        )
    except (UnicodeDecodeError, csv.Error) as error:
        raise AssertionError(f'wheel RECORD is malformed: {error}') from error
    record_rows: dict[str, tuple[str, str]] = {}
    for row in rows:
        if len(row) != 3:
            raise AssertionError('wheel RECORD rows must have exactly three columns')
        name, digest, size = row
        _wheel_member_name(name)
        if name in record_rows:
            raise AssertionError(f'wheel RECORD contains a duplicate path: {name}')
        record_rows[name] = (digest, size)

    signatures = {
        f'{dist_info}/RECORD.jws',
        f'{dist_info}/RECORD.p7s',
    } & set(file_names)
    expected_rows = set(file_names) - signatures
    if set(record_rows) != expected_rows:
        raise AssertionError(
            'wheel RECORD membership mismatch: '
            f'missing={sorted(expected_rows - set(record_rows))}, '
            f'extra={sorted(set(record_rows) - expected_rows)}'
        )
    self_hash, self_size = record_rows[record_name]
    if self_hash or self_size:
        raise AssertionError('wheel RECORD self-row must have blank hash and size')

    member_records: dict[str, dict[str, Any]] = {}
    retained: dict[str, bytes] = {}
    for name, info in file_names.items():
        if name == record_name:
            payload = record_payload
            if len(payload) != info.file_size:
                raise AssertionError('wheel RECORD size differs from ZIP directory')
            member_records[name] = {
                'size': len(payload),
                'sha256': _sha256_bytes(payload),
            }
            continue
        expected_digest = expected_size = ''
        algorithm = ''
        record_hasher = None
        if name not in signatures and name != record_name:
            encoded_hash, expected_size = record_rows[name]
            if (
                not isinstance(expected_size, str)
                or re.fullmatch(r'[0-9]+', expected_size) is None
            ):
                raise AssertionError(
                    f'wheel RECORD size is not a decimal integer: {name}'
                )
            algorithm, separator, encoded_digest = encoded_hash.partition('=')
            if not separator or not encoded_digest or '=' in encoded_digest:
                raise AssertionError(f'wheel RECORD hash is malformed: {name}')
            record_hasher = _record_hash(algorithm, encoded_digest, name)
            expected_digest = encoded_digest
        sha256 = record_hasher if algorithm == 'sha256' else hashlib.sha256()
        should_retain = is_text_member(name) or _is_native_library_member(name)
        chunks: list[bytes] | None = [] if should_retain else None
        actual_size = 0
        try:
            with wheel.open(info) as stream:
                while chunk := stream.read(1024 * 1024):
                    actual_size += len(chunk)
                    sha256.update(chunk)
                    if record_hasher is not None and record_hasher is not sha256:
                        record_hasher.update(chunk)
                    if chunks is not None:
                        chunks.append(chunk)
        except (OSError, zipfile.BadZipFile, RuntimeError) as error:
            raise AssertionError(f'cannot read wheel member {name}: {error}') from error
        if actual_size != info.file_size:
            raise AssertionError(
                f'wheel member size differs from ZIP directory: {name}'
            )
        if expected_size and actual_size != int(expected_size):
            raise AssertionError(f'wheel RECORD size mismatch: {name}')
        if record_hasher is not None:
            actual_record_digest = (
                base64
                .urlsafe_b64encode(record_hasher.digest())
                .rstrip(b'=')
                .decode('ascii')
            )
            if actual_record_digest != expected_digest:
                raise AssertionError(f'wheel RECORD digest mismatch: {name}')
        member_records[name] = {'size': actual_size, 'sha256': sha256.hexdigest()}
        if chunks is not None:
            retained[name] = b''.join(chunks)

    return {
        'dist_info': dist_info,
        'record': record_name,
        'members': member_records,
        'payloads': retained,
        'signatures': sorted(signatures),
    }


def inspect_wheel(
    path: Path,
    *,
    release: bool = True,
    members: dict[str, Any] | None = None,
    expected_python_members: Mapping[str, str] | None = None,
    expected_prepared_inputs: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    extension_suffixes = ('.so', '.pyd', '.dylib')
    with zipfile.ZipFile(path) as wheel:
        inspected = members or inspect_wheel_members(wheel)
    names = set(inspected['members'])
    payloads: dict[str, bytes] = inspected['payloads']
    text_members = {
        name: payload.decode('utf-8', errors='ignore')
        for name, payload in payloads.items()
        if is_text_member(name)
    }
    binary_members = {
        name: payload
        for name, payload in payloads.items()
        if name.startswith('gometry/_lib') and name.endswith(extension_suffixes)
    }
    native_members = {
        name: payload
        for name, payload in payloads.items()
        if _is_native_library_member(name)
    }
    package = {name for name in names if name.startswith('gometry/')}
    dist_info = {
        name for name in names if name.startswith(f'{inspected["dist_info"]}/')
    }
    has_extension = bool(binary_members)
    required = {'gometry/_lib.pyi', 'gometry/py.typed'}
    missing = sorted(required - names)
    required_licenses = {
        'LICENSE-APACHE.md',
        'LICENSE-MIT.md',
        'LICENSE-THIRD-PARTY.md',
    }
    bundled_licenses = {Path(name).name for name in dist_info if '/licenses/' in name}
    pth_files = [name for name in names if name.endswith('.pth')]
    generated_members = sorted(
        name
        for name in names
        if '__pycache__' in Path(name).parts or name.endswith(('.pyc', '.pyo'))
    )
    if not has_extension:
        raise AssertionError('wheel does not contain gometry/_lib native extension')
    if len(binary_members) != 1:
        raise AssertionError(
            f'wheel must contain one gometry extension, found {sorted(binary_members)}'
        )
    # Cargo cannot identify release-dist from PROFILE; the native artifact's
    # absence of debug sections is the inspectable publication property.
    for name, payload in native_members.items():
        if release:
            _verify_native_debug_sections(payload, Path(f'{path}!{name}'))
    if missing:
        raise AssertionError(f'wheel missing required files: {missing}')
    if missing_licenses := sorted(required_licenses - bundled_licenses):
        raise AssertionError(f'wheel missing dist-info licenses: {missing_licenses}')
    third_party = [
        name for name in dist_info if name.endswith('/licenses/LICENSE-THIRD-PARTY.md')
    ]
    if len(third_party) != 1:
        raise AssertionError(f'expected one third-party license file: {third_party!r}')
    try:
        third_party_text = payloads[third_party[0]].decode('utf-8')
    except UnicodeDecodeError as error:
        raise AssertionError(
            f'wheel third-party license is not UTF-8: {path}'
        ) from error
    if expected_prepared_inputs is None:
        verify_native_notice_texts(third_party_text, path)
    else:
        for license_name in required_licenses:
            expected_hash = expected_prepared_inputs.get(license_name)
            members_for_license = [
                name for name in dist_info if name.endswith(f'/licenses/{license_name}')
            ]
            if (
                not isinstance(expected_hash, str)
                or HEX_DIGEST.fullmatch(expected_hash) is None
                or len(members_for_license) != 1
                or inspected['members'][members_for_license[0]]['sha256']
                != expected_hash
            ):
                raise AssertionError(
                    f'wheel license identity differs from staged source: {license_name}'
                )
            root_copies = [name for name in names if name == license_name]
            if (
                root_copies
                and inspected['members'][root_copies[0]]['sha256'] != expected_hash
            ):
                raise AssertionError(
                    f'wheel root license differs from staged source: {license_name}'
                )
    if generated_members:
        raise AssertionError(
            f'wheel contains generated Python bytecode: {generated_members[:20]}'
        )
    if release and (leaks := content_leaks(text_members, binary_members)):
        raise AssertionError(f'wheel contents leak absolute build paths: {leaks}')
    if pth_files and not package:
        raise AssertionError(f'wheel is .pth-only: {pth_files}')
    metadata_name = f'{inspected["dist_info"]}/METADATA'
    wheel_name = f'{inspected["dist_info"]}/WHEEL'
    if metadata_name not in names or wheel_name not in names:
        raise AssertionError('wheel dist-info is missing METADATA or WHEEL')
    try:
        metadata = payloads[metadata_name].decode('utf-8')
        wheel_text = payloads[wheel_name].decode('utf-8')
    except UnicodeDecodeError as error:
        raise AssertionError(
            f'wheel dist-info contains invalid UTF-8: {path}'
        ) from error
    name_lines = [
        line.removeprefix('Name: ').strip()
        for line in metadata.splitlines()
        if line.startswith('Name: ')
    ]
    version_lines = [
        line.removeprefix('Version: ').strip()
        for line in metadata.splitlines()
        if line.startswith('Version: ')
    ]
    if len(name_lines) != 1 or name_lines[0].lower() != 'gometry':
        raise AssertionError(f'wheel METADATA does not identify gometry: {path}')
    if len(version_lines) != 1:
        raise AssertionError(f'wheel METADATA has no unique Version: {path}')
    metadata_version = version_lines[0]
    filename_parts = path.name.split('-', 2)
    filename_version = filename_parts[1] if len(filename_parts) == 3 else ''
    if not metadata_version or metadata_version != filename_version:
        raise AssertionError(
            'wheel filename/metadata version mismatch: '
            f'{filename_version!r} != {metadata_version!r}'
        )
    expected_dist_info = f'gometry-{metadata_version}.dist-info'
    if inspected['dist_info'] != expected_dist_info:
        raise AssertionError(
            f'wheel dist-info does not match distribution identity: '
            f'{inspected["dist_info"]!r} != {expected_dist_info!r}'
        )
    generator_values = [
        line.removeprefix('Generator: ').strip()
        for line in wheel_text.splitlines()
        if line.startswith('Generator: ')
    ]
    if len(generator_values) != 1 or not generator_values[0]:
        raise AssertionError(f'wheel has no unique WHEEL Generator: {path}')
    tags = [
        line.removeprefix('Tag: ').strip()
        for line in wheel_text.splitlines()
        if line.startswith('Tag: ')
    ]
    if not tags or any(len(tag.split('-', 2)) != 3 for tag in tags):
        raise AssertionError(f'wheel has invalid or missing WHEEL tags: {path}')
    inspected['tags'] = tags
    actual_python_members = wheel_python_members(inspected['members'])
    if expected_python_members is not None:
        _verify_python_member_identity(
            actual_python_members,
            expected_python_members,
            label=str(path),
        )
    inspected['python_members'] = actual_python_members
    extension_name, extension_payload = next(iter(binary_members.items()))
    if release and wheel_architecture(path, tags) == 'x86_64':
        _verify_x86_64_v2_markers(binary_members, path)
    native = {
        'extension': native_image_facts(
            extension_payload,
            Path(f'{path}!{extension_name}'),
        ),
        'libraries': {
            name: native_image_facts(payload, Path(f'{path}!{name}'))
            for name, payload in native_members.items()
            if name != extension_name
        },
    }
    return {
        **inspected,
        'version': metadata_version,
        'generator': generator_values[0],
        'tags': tags,
        'extension_members': binary_members,
        'native': native,
    }


def wheel_python_members(members: Mapping[str, Mapping[str, Any]]) -> dict[str, str]:
    return {
        name: record['sha256']
        for name, record in sorted(members.items())
        if not _is_native_library_member(name)
        and not name.endswith(('.pyc', '.pyo'))
        and '__pycache__' not in PurePosixPath(name).parts
        and (
            name.startswith('gometry/')
            or name.endswith(('.py', '.pyi'))
            or PurePosixPath(name).name == 'py.typed'
        )
    }


def _verify_python_member_identity(
    actual: Mapping[str, str],
    expected: Mapping[str, str],
    *,
    label: str,
) -> None:
    if dict(actual) == dict(expected):
        return
    missing_members = sorted(set(expected) - set(actual))
    extra_members = sorted(set(actual) - set(expected))
    changed_members = sorted(
        name for name in set(expected) & set(actual) if expected[name] != actual[name]
    )
    raise AssertionError(
        f'{label} Python members do not match staged source: '
        f'missing={missing_members}, extra={extra_members}, changed={changed_members}'
    )


def _verify_x86_64_v2_markers(binary_members: Mapping[str, bytes], path: Path) -> None:
    if not binary_members:
        raise AssertionError(f'x86_64 wheel has no native extension to verify: {path}')
    missing = sorted(
        name
        for name, payload in binary_members.items()
        if not _has_x86_64_v2_target_features(payload)
    )
    if missing:
        raise AssertionError(
            f'x86_64 wheel native extension is missing x86-64-v2 target features: {missing}'
        )


def _has_x86_64_v2_target_features(payload: bytes) -> bool:
    if b'target_features' not in payload:
        return False
    for field in re.finditer(b'target_features', payload):
        start = field.end()
        while start < len(payload) and payload[start] in b'\x00:= ':
            start += 1
        marker = _TARGET_FEATURE_LIST.match(payload, start)
        if marker is None:
            continue
        features = frozenset(marker.group().decode('ascii').split(','))
        if features >= X86_64_V2_TARGET_FEATURES:
            return True
    for marker in _TARGET_FEATURE_LIST.finditer(payload):
        features = frozenset(marker.group().decode('ascii').split(','))
        if features >= X86_64_V2_TARGET_FEATURES:
            return True
    return False


def _wheel_tags(path: Path, inspection: Mapping[str, Any] | None = None) -> list[str]:
    if inspection is not None:
        return list(inspection['tags'])
    with zipfile.ZipFile(path) as wheel:
        tags = [
            line.removeprefix('Tag: ').strip()
            for name in wheel.namelist()
            if name.endswith('/WHEEL')
            for line in wheel.read(name).decode('utf-8').splitlines()
            if line.startswith('Tag: ')
        ]
    if not tags:
        raise AssertionError(f'wheel has no WHEEL Tag records: {path}')
    return tags


def _platform_architecture(platform_tag: str) -> str:
    normalized = platform_tag.replace('-', '_').lower()
    if normalized.endswith(('_x86_64', '_amd64')):
        return 'x86_64'
    if normalized.endswith(('_aarch64', '_arm64')):
        return 'arm64'
    if normalized.endswith(('_i686', '_x86')):
        return 'x86'
    return normalized.rsplit('_', 1)[-1]


def wheel_architecture(path: Path, tags: Sequence[str] | None = None) -> str:
    architectures = {
        _platform_architecture(tag.split('-', 2)[2])
        for tag in (_wheel_tags(path) if tags is None else tags)
        if len(tag.split('-', 2)) == 3
    }
    if len(architectures) != 1:
        raise AssertionError(
            f'wheel has no unique architecture identity: {path}: {architectures}'
        )
    return next(iter(architectures))


def wheel_target_compatible(
    path: Path, target: str, tags: Sequence[str] | None = None
) -> bool:
    target = target.lower()
    recognized = False
    for tag in _wheel_tags(path) if tags is None else tags:
        parts = tag.split('-', 2)
        if len(parts) != 3:
            return False
        platform_tag = parts[2].replace('-', '_').lower()
        if platform_tag.startswith(('manylinux_', 'musllinux_')):
            recognized = True
            if '-linux-' not in target:
                return False
            if platform_tag.startswith('musllinux_') != ('-linux-musl' in target):
                return False
        elif platform_tag.startswith('macosx_'):
            recognized = True
            if not target.endswith('-apple-darwin'):
                return False
        elif platform_tag.startswith('win_'):
            recognized = True
            if '-windows-' not in target:
                return False
        else:
            return False
    return recognized


def validate_wheel_native_target(
    inspection: Mapping[str, Any],
    policy: Mapping[str, Any],
    *,
    label: str,
    tags: Sequence[str] | None = None,
    validate_tags: bool = True,
) -> None:
    native = inspection.get('native')
    if not isinstance(native, dict):
        raise TypeError(f'{label} has no native image identity')
    family = policy['platform_family']
    extension = native.get('extension')
    libraries = native.get('libraries')
    if not isinstance(extension, dict) or not isinstance(libraries, dict):
        extension = native
        libraries = {}

    def check_image(image: Mapping[str, Any], name: str, *, require_libc: bool) -> None:
        architectures = image.get('architectures')
        if architectures is None:
            architecture = image.get('architecture')
            architectures = [architecture] if isinstance(architecture, str) else []
        if not any(
            isinstance(architecture, str)
            and _target_architecture(architecture) == policy['architecture']
            for architecture in architectures
        ):
            raise AssertionError(f'{label} native image architecture differs: {name}')
        if family.startswith('linux-'):
            expected_libc = family.removeprefix('linux-')
            if image.get('format') != 'ELF':
                raise AssertionError(f'{label} native image is not ELF: {name}')
            libc = image.get('libc')
            if require_libc and libc != expected_libc:
                raise AssertionError(
                    f'{label} native image libc differs from target: '
                    f'{name}: {libc!r} != {expected_libc!r}'
                )
            if libc is not None and libc != expected_libc:
                raise AssertionError(
                    f'{label} native library libc differs from target: '
                    f'{name}: {libc!r} != {expected_libc!r}'
                )
            needed = image.get('needed')
            versions = image.get('glibc_versions')
            if not isinstance(needed, list) or not isinstance(versions, list):
                raise AssertionError(f'{label} lacks Linux dependency evidence: {name}')
            if expected_libc == 'musl' and versions:
                raise AssertionError(
                    f'{label} musl image contains GNU libc versions: {name}'
                )
            if require_libc and expected_libc == 'gnu' and not versions:
                raise AssertionError(f'{label} GNU image has no GLIBC versions: {name}')
        elif family == 'darwin' and image.get('format') != 'Mach-O':
            raise AssertionError(f'{label} native image is not Mach-O: {name}')
        elif family == 'windows-msvc' and image.get('format') != 'PE':
            raise AssertionError(f'{label} native image is not PE: {name}')

    check_image(extension, 'extension', require_libc=True)
    for name, image in libraries.items():
        if not isinstance(image, dict):
            raise TypeError(f'{label} native library identity is malformed: {name}')
        check_image(image, name, require_libc=False)
    if family == 'darwin':
        deployment_targets = extension.get('deployment_targets')
        if (
            not isinstance(deployment_targets, list)
            or '11.0.0' not in deployment_targets
        ):
            raise AssertionError(f'{label} Mach-O minimum macOS version is not 11.0')
    if not validate_tags:
        return
    if tags is None:
        tags = inspection.get('tags')
    if not isinstance(tags, (list, tuple)) or not tags:
        raise AssertionError(f'{label} has no platform tags for native policy')
    platform_tags: list[str] = []
    for tag in tags:
        parts = tag.split('-', 2) if isinstance(tag, str) else []
        if len(parts) != 3:
            raise AssertionError(f'{label} has a malformed wheel tag')
        platform_tags.extend(parts[2].lower().split('.'))
    if family == 'darwin':
        deployment_targets = extension['deployment_targets']
        macos_floors = []
        for platform_tag in platform_tags:
            match = re.match(r'^macosx_(\d+)_(\d+)_', platform_tag)
            if match is None:
                raise AssertionError(f'{label} has an unknown macOS platform tag')
            macos_floors.append((int(match.group(1)), int(match.group(2))))
        if not macos_floors or min(macos_floors) != (11, 0):
            raise AssertionError(f'{label} wheel tag changes the macOS 11.0 floor')
        higher = [
            version
            for image in (extension, *libraries.values())
            if isinstance(image, dict)
            for version in image.get('deployment_targets', [])
            if isinstance(version, str)
            and tuple(int(part) for part in version.split('.')) > (11, 0, 0)
        ]
        if higher:
            raise AssertionError(
                f'{label} native Mach-O image raises the macOS deployment floor: '
                f'{sorted(higher)}'
            )
    elif family == 'linux-gnu':
        floors = []
        legacy_floors = {
            'manylinux1': (2, 5),
            'manylinux2010': (2, 12),
            'manylinux2014': (2, 17),
        }
        for platform_tag in platform_tags:
            if platform_tag.startswith('manylinux_'):
                match = re.match(r'^manylinux_(\d+)_(\d+)_', platform_tag)
                if match is None:
                    raise AssertionError(f'{label} has an unknown manylinux floor')
                floors.append((int(match.group(1)), int(match.group(2))))
            else:
                legacy, separator, architecture = platform_tag.partition('_')
                if not separator or legacy not in legacy_floors:
                    raise AssertionError(
                        f'{label} has an unknown manylinux platform tag'
                    )
                if (
                    architecture not in {'x86_64', 'aarch64'}
                    or _target_architecture(architecture) != policy['architecture']
                ):
                    raise AssertionError(
                        f'{label} legacy manylinux tag architecture differs from target'
                    )
                floors.append(legacy_floors[legacy])
        if not floors:
            raise AssertionError(f'{label} has no manylinux platform floor')
        required_versions = [
            tuple(int(part) for part in version.removeprefix('GLIBC_').split('.'))
            for image in (extension, *libraries.values())
            for version in image.get('glibc_versions', [])
            if isinstance(version, str) and version.startswith('GLIBC_')
        ]
        if required_versions and max(required_versions) > min(floors):
            raise AssertionError(
                f'{label} wheel tag understates its GLIBC requirement: '
                f'{max(required_versions)} > {min(floors)}'
            )
    elif family == 'linux-musl':
        if any(
            not platform_tag.startswith(('musllinux_1_1_', 'musllinux_1_2_'))
            for platform_tag in platform_tags
        ):
            raise AssertionError(f'{label} has an unknown musllinux floor')


def _platform_matches(identifier: str, wheel_platform: str) -> bool:
    identifier = identifier.replace('-', '_')
    wheel_platform = wheel_platform.replace('-', '_')
    if identifier == wheel_platform:
        return True
    family, separator, architecture = identifier.partition('_')
    return bool(
        separator
        and family in {'manylinux', 'musllinux', 'macosx'}
        and wheel_platform.startswith(f'{family}_')
        and wheel_platform.endswith(f'_{architecture}')
    )


def wheel_matches_identifier(
    path: Path, identifier: str, tags: Sequence[str] | None = None
) -> bool:
    match = IDENTIFIER.fullmatch(identifier)
    if match is None:
        raise AssertionError(f'invalid cibuildwheel identifier: {identifier!r}')
    expected_python = match.group('python')
    expected_platform = match.group('platform')
    for tag in _wheel_tags(path) if tags is None else tags:
        parts = tag.split('-', 2)
        if (
            len(parts) == 3
            and parts[0] == expected_python
            and _platform_matches(expected_platform, parts[2])
        ):
            return True
    return False
