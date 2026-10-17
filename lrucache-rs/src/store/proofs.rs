use super::{INVALID, NonZeroUsize, Store};

#[kani::proof]
#[kani::unwind(5)]
fn recency_changes_preserve_slab_links_and_revision_semantics() {
    let mut store = Store::new(NonZeroUsize::new(3).unwrap());
    for hash in [17, 29, 41] {
        store.insert(hash);
    }
    let mut order = [2_u32, 1, 0];
    let revision: u64 = kani::any();
    store.revision = revision;
    let mut changes = 0_u128;
    for _ in 0..4 {
        let position = usize::from(kani::any::<u8>() % 3);
        let index = order[position];
        if kani::any() {
            store.changed_value(index);
            changes += 1;
        } else {
            store.touch(index);
        }
        let [first, second, third] = order;
        order = match position {
            0 => [first, second, third],
            1 => [second, first, third],
            _ => [third, first, second],
        };
        assert_eq!(store.head, order[0]);
        assert_eq!(store.lru(), Some(order[2]));
        assert_eq!(store.insertion_slot(), order[2]);
        assert_eq!(store.len(), 3);
        assert_eq!(
            u128::from(store.revision()),
            (u128::from(revision) + changes) % (1_u128 << 64)
        );
        for (position, &index) in order.iter().enumerate() {
            let node = store.node(index);
            let previous = if position == 0 {
                INVALID
            } else {
                order[position - 1]
            };
            let next = if position == 2 {
                INVALID
            } else {
                order[position + 1]
            };
            assert_eq!(node.prev, previous);
            assert_eq!(node.next, next);
        }
    }
    let indices = store.indices();
    assert_eq!(indices.len(), order.len());
    for (&actual, expected) in indices.iter().zip([order[2], order[1], order[0]]) {
        assert_eq!(actual, expected);
    }
}
