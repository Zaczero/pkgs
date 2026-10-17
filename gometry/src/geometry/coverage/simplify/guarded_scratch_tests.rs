use super::{TopologyGuard, simplify_chain_guarded};
use crate::geometry::{Segment, XY};

fn dummy_guard() -> TopologyGuard<'static> {
    TopologyGuard::new(&[], Vec::new())
}

fn collinear_x(index: usize) -> XY {
    XY::new(index as f64, 0.0)
}

#[test]
fn vertex_index_is_lazy_and_adds_no_staging_allocation() {
    use crate::geometry::BulkRTree;
    use crate::test_support::{allocation_count, assert_no_allocations};

    for count in [1, 16, 33] {
        let cloud: Vec<_> = (0..count).map(|index| [f64::from(index), 1.0]).collect();
        let direct_cloud = cloud.clone();
        let guard = assert_no_allocations(|| TopologyGuard::new(&[], cloud));
        assert_no_allocations(|| {
            assert!(guard.removal_is_safe(collinear_x(0), collinear_x(1), collinear_x(2), &[]));
        });
        let (direct, direct_allocations) =
            allocation_count(|| BulkRTree::bulk_load_with_params(direct_cloud));
        let (indexed, lazy_allocations) = allocation_count(|| guard.vertex_tree());
        assert_eq!(indexed.size(), direct.size());
        assert_eq!(lazy_allocations, direct_allocations, "vertex count {count}");
        assert_no_allocations(|| {
            assert!(std::ptr::eq(indexed, guard.vertex_tree()));
        });
    }
}

#[test]
fn short_chain_keeps_both_endpoints() {
    let guard = dummy_guard();
    let kept = simplify_chain_guarded(2, collinear_x, false, &|_, _, _| Ok(None), &guard).unwrap();
    assert_eq!(kept, [0, 1]);
}

#[test]
fn declined_candidates_keep_every_vertex() {
    let guard = dummy_guard();
    let never = |_: usize, _: usize, _: usize| Ok(None);
    let kept = simplify_chain_guarded(6, collinear_x, false, &never, &guard).unwrap();
    assert_eq!(kept, vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn collinear_removal_keeps_only_endpoints() {
    let guard = dummy_guard();
    let drop_all = |_: usize, _: usize, _: usize| Ok(Some(0.0));
    let kept = simplify_chain_guarded(8, collinear_x, false, &drop_all, &guard).unwrap();
    assert_eq!(kept, vec![0, 7]);
}

#[test]
fn collinear_removal_keeps_endpoints_in_order() {
    let guard = dummy_guard();
    let drop_all = |_: usize, _: usize, _: usize| Ok(Some(0.0));
    let kept = simplify_chain_guarded(5, collinear_x, false, &drop_all, &guard).unwrap();
    assert_eq!(kept, vec![0, 4]);
    let cyclic = simplify_chain_guarded(6, collinear_x, true, &drop_all, &guard).unwrap();
    assert_eq!(cyclic.first().copied(), Some(0));
    assert_eq!(cyclic.last().copied(), Some(5));
    assert_eq!(cyclic.len(), 4);
}

#[test]
fn coverage_contact_guard_rejects_a_foreign_collinear_collapse() {
    let previous = XY::new(0.0, 0.0);
    let vertex = XY::new(1.0, 1.0);
    let next = XY::new(2.0, 0.0);
    let chain_segments = [
        Segment {
            start: previous,
            end: vertex,
        },
        Segment {
            start: vertex,
            end: next,
        },
    ];
    let segments = [chain_segments[0], chain_segments[1], Segment {
        start: previous,
        end: next,
    }];
    let guard = TopologyGuard::new(&segments, Vec::new());
    assert!(!guard.removal_is_safe(previous, vertex, next, &chain_segments));
}
