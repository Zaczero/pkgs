use std::cell::Cell;

use super::InlineFifo;

struct Owned<'a> {
    value: u8,
    bit: u8,
    dropped: &'a Cell<u8>,
}

impl Drop for Owned<'_> {
    fn drop(&mut self) {
        let previous = self.dropped.get();
        assert_eq!(
            previous & self.bit,
            0,
            "an item must be destroyed only once"
        );
        self.dropped.set(previous | self.bit);
    }
}

#[kani::proof]
#[kani::unwind(5)]
fn spilled_prefix_reuse_preserves_values_and_destroys_each_owner_once() {
    let dropped = Cell::new(0);
    let mut queue = InlineFifo::<Owned<'_>, 2>::new();
    let mut values: [u8; 4] = kani::any();
    for (index, &value) in values[..3].iter().enumerate() {
        queue.push_back(Owned {
            value,
            bit: 1 << index,
            dropped: &dropped,
        });
    }
    let consumed = usize::from(kani::any::<u8>() % 4);
    for &value in &values[..consumed] {
        assert_eq!(queue.pop_front().map(|item| item.value), Some(value));
    }
    assert_eq!(dropped.get(), (1 << consumed) - 1);
    queue.push_back(Owned {
        value: values[3],
        bit: 8,
        dropped: &dropped,
    });
    assert_eq!(queue.len(), 4 - consumed);
    let replacement: u8 = kani::any();
    queue.front_mut().unwrap().value = replacement;
    values[consumed] = replacement;
    assert_eq!(queue.front().map(|item| item.value), Some(replacement));
    for (index, item) in queue.iter().enumerate() {
        assert_eq!(item.value, values[consumed + index]);
    }
    if kani::any() {
        queue.clear();
        assert!(queue.is_empty());
        assert_eq!(dropped.get(), 15);
    }
    drop(queue);
    assert_eq!(dropped.get(), 15, "every inserted owner must be destroyed");
}
