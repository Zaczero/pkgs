"""Profile admission rejects mismatched build inputs before compilation."""

import hashlib
import json
import subprocess
import sys
from fractions import Fraction
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tools'))

from pgo import check_profile
from pgo_process import collect as collect_process
from pgo_profile import integer_weights, merge
from wheels import compilation_identity

PACKAGE = Path(__file__).resolve().parents[1]
CONFIG = {'target': 'x86_64-unknown-linux-gnu'}


@pytest.fixture
def profile(tmp_path, monkeypatch):
    # The interpreter already loaded ASan; child compiler tools must not preload it.
    monkeypatch.delenv('LD_PRELOAD', raising=False)
    monkeypatch.delenv('RUSTFLAGS', raising=False)
    monkeypatch.delenv('CARGO_ENCODED_RUSTFLAGS', raising=False)
    path = tmp_path / 'profile.profdata'
    path.write_bytes(b'profile admission fixture')
    manifest = {
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
        'compiler': subprocess.check_output(['rustc', '-vV'], text=True, timeout=15),
        'compilation': compilation_identity(PACKAGE),
        'target': CONFIG['target'],
        'python': sys.version,
    }
    path.with_suffix('.json').write_text(json.dumps(manifest))
    return path, manifest


@pytest.mark.parametrize(
    'rustflags',
    [
        pytest.param(None, id='default'),
        pytest.param(['-Cllvm-args=-disable-preinline'], id='pipeline-adjustment'),
    ],
)
def test_matching_profile_is_admitted(profile, rustflags):
    path, manifest = profile
    config = dict(CONFIG)
    if rustflags is not None:
        config['rustflags'] = manifest['rustflags'] = rustflags
        path.with_suffix('.json').write_text(json.dumps(manifest))
    assert check_profile(path, PACKAGE, config) == manifest


@pytest.mark.parametrize(
    ('field', 'replacement', 'message'),
    [
        ('sha256', 'changed', 'digest changed'),
        ('compiler', 'different compiler', 'different Rust compiler'),
        ('compilation', {}, 'different source or dependencies'),
        ('rustflags', ['-Cllvm-args=-disable-preinline'], 'different target flags'),
        ('target', 'aarch64-unknown-linux-gnu', 'different target or Python'),
        ('python', 'different Python', 'different target or Python'),
    ],
)
def test_profile_build_mismatch_is_rejected(profile, field, replacement, message):
    path, manifest = profile
    manifest[field] = replacement
    path.with_suffix('.json').write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError, match=message):
        check_profile(path, PACKAGE, CONFIG)


@pytest.mark.parametrize('name', ['RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS'])
def test_even_empty_environment_flags_cannot_disable_pgo(profile, monkeypatch, name):
    path, _ = profile
    monkeypatch.setenv(name, '')
    with pytest.raises(RuntimeError, match='overrides the target PGO configuration'):
        check_profile(path, PACKAGE, CONFIG)


def test_weights_normalize_by_useful_work():
    assert integer_weights([Fraction(36, 50000), Fraction(24, 40000)]) == [6, 5]


@pytest.mark.parametrize('shares', [[], [Fraction(0)], [Fraction(-1)]])
def test_nonpositive_profile_shares_are_rejected(shares):
    with pytest.raises(ValueError, match='must be positive'):
        integer_weights(shares)


@pytest.mark.parametrize('defect', ['missing', 'duplicate', 'work', 'plan-digest'])
def test_changed_collection_plan_is_rejected(tmp_path, defect):
    leaf = {
        'name': 'requests',
        'weight': '100',
        'units': 12,
        'expected_functions': ['serve'],
    }
    plan = tmp_path / 'plan.json'
    plan.write_text(json.dumps({'leaves': [leaf]}))
    collection = {
        'plan_sha256': hashlib.sha256(plan.read_bytes()).hexdigest(),
        'leaves': [dict(leaf)],
    }
    if defect == 'missing':
        collection['leaves'].clear()
    elif defect == 'duplicate':
        collection['leaves'].append(dict(leaf))
    elif defect == 'work':
        collection['leaves'][0]['units'] = 13
    else:
        collection['plan_sha256'] = 'changed'
    receipt = tmp_path / 'collection.json'
    receipt.write_text(json.dumps(collection))
    with pytest.raises(RuntimeError, match=r'plan|planned work'):
        merge(receipt, tmp_path / 'accepted', build={})
    assert not (tmp_path / 'accepted').exists()


@pytest.mark.parametrize('defect', ['image', 'units'])
def test_process_collection_rejects_wrong_worker(tmp_path, monkeypatch, defect):
    monkeypatch.delenv('LD_PRELOAD', raising=False)
    image = tmp_path / 'extension.so'
    image.write_bytes(b'extension image')
    expected_image = hashlib.sha256(image.read_bytes()).hexdigest()
    worker = tmp_path / 'worker.py'
    worker.write_text(
        'import json, os\n'
        'from pathlib import Path\n'
        "Path(os.environ['LLVM_PROFILE_FILE']).write_bytes(b'profile')\n"
        "print(json.dumps({'units': int(os.environ['UNITS']), "
        "'image_sha256': os.environ['IMAGE']}))\n"
    )
    monkeypatch.setenv('IMAGE', 'different' if defect == 'image' else expected_image)
    monkeypatch.setenv('UNITS', '2' if defect == 'units' else '1')
    output = tmp_path / 'collection'
    with pytest.raises(RuntimeError, match='wrong image or completed work'):
        collect_process(output, worker, image, units=1, functions=['entry'])
    assert not (output / 'collection.json').exists()


def test_process_collection_retains_worker_failure(tmp_path, monkeypatch):
    monkeypatch.delenv('LD_PRELOAD', raising=False)
    image = tmp_path / 'extension.so'
    image.write_bytes(b'extension image')
    worker = tmp_path / 'worker.py'
    worker.write_text(
        "import sys\nprint('training failed', file=sys.stderr)\nsys.exit(1)\n"
    )
    output = tmp_path / 'collection'
    with pytest.raises(RuntimeError, match='training process failed'):
        collect_process(output, worker, image, units=1, functions=['entry'])
    assert (output / 'workload/training.log').read_text() == 'training failed\n'
    assert not (output / 'collection.json').exists()
