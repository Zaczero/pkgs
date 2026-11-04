"""Source snapshot reuse, invalidation and retention."""

import gc
from concurrent.futures import ThreadPoolExecutor

from pyo3stubs.rust_scan import RustSource, iter_sources
from tests._support import make_config


def test_source_revision_replaces_cache_without_changing_retained_snapshot(tmp_path):
    cfg = make_config(tmp_path, src_text='pub struct Original;')
    (cfg.src_root / 'other.rs').write_text('pub struct Unchanged;')
    original = iter_sources(cfg, scope='all')
    assert iter_sources(cfg, scope='all') is original

    (cfg.src_root / 'lib.rs').write_text('pub struct Replacement;')
    current = iter_sources(cfg, scope='all')
    assert current is not original
    assert original[0].tree.root_node.text == b'pub struct Original;'
    assert current[0].tree.root_node.text == b'pub struct Replacement;'
    assert current[1] is original[1]
    assert iter_sources(cfg, scope='all') is current


def test_cache_releases_obsolete_source_revisions(tmp_path):
    cfg = make_config(tmp_path, src_text='pub struct Current;')
    filename = f'snapshot_{tmp_path.name}.rs'
    path = cfg.src_root / filename
    (cfg.src_root / 'lib.rs').rename(path)
    for revision in range(4):
        path.write_text('pub struct Current;' + '\n' * revision)
        iter_sources(cfg, scope='all')
        iter_sources(cfg)
    gc.collect()
    retained = [
        obj
        for obj in gc.get_objects()
        if isinstance(obj, RustSource) and obj.rel == filename
    ]
    assert len(retained) == 1


def test_concurrent_gates_share_each_source_view(tmp_path):
    cfg = make_config(
        tmp_path, src_text='pub struct Exported;\n#[cfg(test)] mod tests {}'
    )
    scopes = ['all', 'export'] * 8
    with ThreadPoolExecutor(max_workers=4) as pool:
        views = list(pool.map(lambda scope: iter_sources(cfg, scope=scope), scopes))
    assert all(view is views[0] for view in views[::2])
    assert all(view is views[1] for view in views[1::2])
    assert views[0][0].tree is views[1][0].tree
