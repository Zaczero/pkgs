//! Python-free slab, hash index and doubly linked recency order.
//!
//! Reference positions use the same slab indices. Only membership and value
//! changes advance the revision; recency-only reads must not invalidate an
//! equality callback's enclosing lookup.

use std::num::NonZeroUsize;

use hashbrown::HashTable;
use pyo3::ffi::Py_hash_t;

const INVALID: u32 = u32::MAX;
pub(crate) const MAX_CAPACITY: usize = (u32::MAX - 1) as usize;

#[cfg(kani)]
mod proofs;

#[derive(Clone, Copy)]
struct Node {
    hash: Py_hash_t,
    prev: u32,
    next: u32,
}

pub(crate) struct Store {
    table: HashTable<u32>,
    nodes: Box<[Node]>,
    free: Vec<u32>,
    head: u32,
    tail: u32,
    len: u32,
    revision: u64,
}

impl Store {
    pub(crate) fn new(maxsize: NonZeroUsize) -> Self {
        let cap = maxsize.get();
        Self {
            table: HashTable::with_capacity(cap),
            nodes: vec![
                Node {
                    hash: 0,
                    prev: INVALID,
                    next: INVALID,
                };
                cap
            ]
            .into_boxed_slice(),
            free: (0..cap as u32).rev().collect(),
            head: INVALID,
            tail: INVALID,
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

    pub(crate) fn candidates(&self, hash: Py_hash_t) -> impl Iterator<Item = u32> + '_ {
        self.table
            .iter_hash(hash as u64)
            .copied()
            .filter(move |&idx| self.node(idx).hash == hash)
    }

    pub(crate) fn indices(&self) -> Vec<u32> {
        let mut indices = Vec::with_capacity(self.len());
        let mut idx = self.tail;
        while idx != INVALID {
            indices.push(idx);
            idx = self.node(idx).prev;
        }
        indices
    }

    pub(crate) fn touch(&mut self, idx: u32) {
        if self.head == idx {
            return;
        }
        let Node { prev, next, .. } = *self.node(idx);
        self.unlink(prev, next);
        let head = self.head;
        self.node_mut(idx).prev = INVALID;
        self.node_mut(idx).next = head;
        self.node_mut(head).prev = idx;
        self.head = idx;
    }

    pub(crate) fn changed_value(&mut self, idx: u32) {
        self.touch(idx);
        self.revision = self.revision.wrapping_add(1);
    }

    pub(crate) const fn lru(&self) -> Option<u32> {
        if self.tail == INVALID {
            None
        } else {
            Some(self.tail)
        }
    }

    pub(crate) fn is_full(&self) -> bool {
        self.len() == self.nodes.len()
    }

    pub(crate) fn insertion_slot(&self) -> u32 {
        if self.is_full() {
            self.tail
        } else {
            *self.free.last().expect("free slot below capacity")
        }
    }

    pub(crate) fn insert(&mut self, hash: Py_hash_t) -> u32 {
        let idx = self
            .free
            .pop()
            .expect("free slot after admission or eviction");
        let head = self.head;
        *self.node_mut(idx) = Node {
            hash,
            prev: INVALID,
            next: head,
        };
        if head == INVALID {
            self.tail = idx;
        } else {
            self.node_mut(head).prev = idx;
        }
        self.head = idx;
        self.len = self.len.wrapping_add(1);
        let nodes = &self.nodes;
        self.table.insert_unique(hash as u64, idx, |&stored| {
            nodes
                .get(stored as usize)
                .expect("occupied table index")
                .hash as u64
        });
        self.revision = self.revision.wrapping_add(1);
        idx
    }

    pub(crate) fn remove(&mut self, idx: u32) {
        let Node { hash, prev, next } = *self.node(idx);
        self.table
            .find_entry(hash as u64, |&stored| stored == idx)
            .expect("occupied slot in hash index")
            .remove();
        self.unlink(prev, next);
        self.free.push(idx);
        self.len = self.len.wrapping_sub(1);
        self.revision = self.revision.wrapping_add(1);
    }

    pub(crate) fn clear(&mut self) {
        let mut idx = self.tail;
        while idx != INVALID {
            self.free.push(idx);
            idx = self.node(idx).prev;
        }
        self.table.clear();
        self.head = INVALID;
        self.tail = INVALID;
        self.len = 0;
        self.revision = self.revision.wrapping_add(1);
    }

    fn node(&self, idx: u32) -> &Node {
        self.nodes.get(idx as usize).expect("occupied slab index")
    }

    fn node_mut(&mut self, idx: u32) -> &mut Node {
        self.nodes
            .get_mut(idx as usize)
            .expect("occupied slab index")
    }

    fn unlink(&mut self, prev: u32, next: u32) {
        if prev == INVALID {
            self.head = next;
        } else {
            self.node_mut(prev).next = next;
        }
        if next == INVALID {
            self.tail = prev;
        } else {
            self.node_mut(next).prev = prev;
        }
    }
}
