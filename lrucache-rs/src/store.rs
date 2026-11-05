//! Python-free slab, hash index and doubly linked recency order.
//!
//! Reference positions use the same slab indices. Only membership and value
//! changes advance the revision; recency-only reads must not invalidate an
//! equality callback's enclosing lookup.

use core::num::niche_types::U32NotAllOnes;
use std::iter;
use std::num::NonZeroU32;

use hashbrown::HashTable;
use pyo3::ffi::Py_hash_t;

pub(crate) type Slot = U32NotAllOnes;

pub(crate) const MAX_CAPACITY: usize = (u32::MAX - 1) as usize;

#[cfg(kani)]
mod proofs;

#[derive(Clone, Copy)]
struct Node {
    hash: Py_hash_t,
    prev: Option<Slot>,
    // Occupied nodes link recency order; vacant nodes link the free list.
    next: Option<Slot>,
}

pub(crate) struct Store {
    table: HashTable<Slot>,
    nodes: Box<[Node]>,
    free: Option<Slot>,
    head: Option<Slot>,
    tail: Option<Slot>,
    len: u32,
    /// Cyclic mutation token for detecting changes across Python callbacks, not a total.
    revision: u64,
}

impl Store {
    pub(crate) fn new(maxsize: NonZeroU32) -> Self {
        let cap = maxsize.get();
        let nodes: Box<[_]> = (1..cap)
            .map(Slot::new)
            .chain(iter::once(None))
            .map(|next| Node {
                hash: 0,
                prev: None,
                next,
            })
            .collect();
        Self {
            table: HashTable::with_capacity(cap as usize),
            nodes,
            free: Slot::new(0),
            head: None,
            tail: None,
            len: 0,
            revision: 0,
        }
    }

    pub(crate) const fn len(&self) -> usize {
        self.len as usize
    }

    pub(crate) const fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn candidates(&self, hash: Py_hash_t) -> impl Iterator<Item = Slot> + '_ {
        self.table
            .iter_hash(hash as u64)
            .copied()
            .filter(move |&idx| self.node(idx).hash == hash)
    }

    pub(crate) fn indices(&self) -> impl ExactSizeIterator<Item = Slot> + '_ {
        let mut idx = self.tail;
        std::iter::repeat_with(move || {
            let current = idx.expect("one recency slot per live entry");
            idx = self.node(current).prev;
            current
        })
        .take(self.len())
    }

    pub(crate) fn touch(&mut self, idx: Slot) {
        if self.head == Some(idx) {
            return;
        }
        let Node { prev, next, .. } = *self.node(idx);
        self.unlink(prev, next);
        let head = self.head.expect("a non-head entry has a predecessor");
        self.node_mut(idx).prev = None;
        self.node_mut(idx).next = Some(head);
        self.node_mut(head).prev = Some(idx);
        self.head = Some(idx);
    }

    pub(crate) fn changed_value(&mut self, idx: Slot) {
        self.touch(idx);
        self.revision = self.revision.wrapping_add(1);
    }

    pub(crate) const fn lru(&self) -> Option<Slot> {
        self.tail
    }

    pub(crate) fn is_full(&self) -> bool {
        self.len() == self.nodes.len()
    }

    pub(crate) fn insertion_slot(&self) -> Slot {
        if self.is_full() {
            self.tail.expect("full store has a tail")
        } else {
            self.free.expect("free slot below capacity")
        }
    }

    pub(crate) fn insert(&mut self, hash: Py_hash_t) -> Slot {
        let idx = self.free.expect("free slot after admission or eviction");
        self.free = self.node(idx).next;
        let head = self.head;
        *self.node_mut(idx) = Node {
            hash,
            prev: None,
            next: head,
        };
        if let Some(head) = head {
            self.node_mut(head).prev = Some(idx);
        } else {
            self.tail = Some(idx);
        }
        self.head = Some(idx);
        self.len = self.len.checked_add(1).expect("live entries fit the slab");
        let nodes = &self.nodes;
        self.table.insert_unique(hash as u64, idx, |&stored| {
            nodes
                .get(stored.as_inner() as usize)
                .expect("occupied table index")
                .hash as u64
        });
        self.revision = self.revision.wrapping_add(1);
        idx
    }

    pub(crate) fn remove(&mut self, idx: Slot) {
        let Node { hash, prev, next } = *self.node(idx);
        self.table
            .find_entry(hash as u64, |&stored| stored == idx)
            .expect("occupied slot in hash index")
            .remove();
        self.unlink(prev, next);
        self.node_mut(idx).next = self.free;
        self.free = Some(idx);
        self.len = self.len.checked_sub(1).expect("removed slot was occupied");
        self.revision = self.revision.wrapping_add(1);
    }

    pub(crate) fn clear(&mut self) {
        if let Some(tail) = self.tail {
            // The live chain is already linked head-to-tail. Splice it onto
            // the vacant chain without visiting or rewriting every live node.
            self.node_mut(tail).next = self.free;
            self.free = self.head;
        }
        self.table.clear();
        self.head = None;
        self.tail = None;
        self.len = 0;
        self.revision = self.revision.wrapping_add(1);
    }

    fn node(&self, idx: Slot) -> &Node {
        self.nodes.get(idx.as_inner() as usize).expect("slab index")
    }

    fn node_mut(&mut self, idx: Slot) -> &mut Node {
        self.nodes
            .get_mut(idx.as_inner() as usize)
            .expect("slab index")
    }

    fn unlink(&mut self, prev: Option<Slot>, next: Option<Slot>) {
        if let Some(prev) = prev {
            self.node_mut(prev).next = next;
        } else {
            self.head = next;
        }
        if let Some(next) = next {
            self.node_mut(next).prev = prev;
        } else {
            self.tail = prev;
        }
    }
}
