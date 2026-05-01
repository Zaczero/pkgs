use std::fmt::{self, Debug, Formatter};

use smallvec::SmallVec;

pub struct SmallVecDeque<T, const N: usize> {
    items: SmallVec<[Option<T>; N]>,
    front: usize,
}

impl<T: Debug, const N: usize> Debug for SmallVecDeque<T, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SmallVecDeque")
            .field("items", &self.items)
            .field("front", &self.front)
            .finish()
    }
}

impl<T, const N: usize> Default for SmallVecDeque<T, N> {
    fn default() -> Self {
        Self {
            items: SmallVec::new(),
            front: 0,
        }
    }
}

impl<T, const N: usize> SmallVecDeque<T, N> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len().saturating_sub(self.front)
    }

    pub(crate) fn front(&self) -> Option<&T> {
        if self.is_empty() {
            return None;
        }

        Some(self.live_item(self.front))
    }

    pub(crate) fn front_mut(&mut self) -> Option<&mut T> {
        if self.is_empty() {
            return None;
        }

        Some(self.live_item_mut(self.front))
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        (self.front..self.items.len()).map(|index| self.live_item(index))
    }

    pub(crate) fn push_back(&mut self, item: T) {
        if self.front >= N && self.front * 2 >= self.items.len() {
            self.compact();
        } else if self.is_empty() {
            self.clear();
        }
        self.items.push(Some(item));
    }

    pub(crate) fn pop_front(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }

        let item = self.items[self.front]
            .take()
            .expect("smallvec deque front always points at a live item");
        self.advance_front();
        Some(item)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.front == self.items.len()
    }

    fn live_item(&self, index: usize) -> &T {
        self.items[index]
            .as_ref()
            .expect("smallvec deque live range always contains items")
    }

    fn live_item_mut(&mut self, index: usize) -> &mut T {
        self.items[index]
            .as_mut()
            .expect("smallvec deque live range always contains items")
    }

    pub(crate) fn clear(&mut self) {
        self.items.clear();
        self.front = 0;
    }

    fn advance_front(&mut self) {
        debug_assert!(!self.is_empty());
        self.front += 1;
        if self.is_empty() {
            self.clear();
        }
    }

    fn compact(&mut self) {
        if self.front == 0 {
            return;
        }
        self.items.drain(..self.front);
        self.front = 0;
    }
}
