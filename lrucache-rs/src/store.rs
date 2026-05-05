//! Slab-backed linked hash table tuned for an LRU cache of Python objects.
//!
//! Each operation accepts the pre-computed Python hash plus a fallible eq
//! closure, so unhashable keys and exceptions raised by `__eq__` propagate as
//! `PyResult` instead of panicking.
//!
//! The slab is a fixed-size `Box<[MaybeUninit<Node>]>` allocated up-front;
//! eviction recycles slots through a `Vec<u32>` LIFO. The
//! `hashbrown::HashTable` is sized once with `with_capacity(maxsize)`
//! so it never resizes, which would otherwise call back into Python via the
//! comparator. LRU order is a doubly-linked list embedded in the slab;
//! `INVALID` (`u32::MAX`) is the sentinel for both ends of the list and the
//! free chain.

use std::mem::MaybeUninit;
use std::num::NonZeroUsize;

use hashbrown::HashTable;
use pyo3::Py;
use pyo3::ffi::Py_hash_t;
use pyo3::prelude::*;
use pyo3::types::PyAny;

const INVALID: u32 = u32::MAX;

/// Maximum value of `maxsize` accepted by [`Store::new`]. The slab uses `u32`
/// indices so each node packs into 32 bytes; `INVALID` (`u32::MAX`) is reserved
/// as the link sentinel, leaving `u32::MAX - 1` as the largest representable
/// real index.
pub const MAX_CAPACITY: usize = (u32::MAX - 1) as usize;

struct Node {
    hash: Py_hash_t,
    key: Py<PyAny>,
    value: Py<PyAny>,
    prev: u32,
    next: u32,
}

pub struct Store {
    table: HashTable<u32>,
    nodes: Box<[MaybeUninit<Node>]>,
    free: Vec<u32>,
    head: u32,
    tail: u32,
    len: u32,
    maxsize: u32,
}

/// Python objects displaced by a successful [`Store::put`]. The caller must
/// drop these *outside* the store's lock: `Py_DECREF` can run arbitrary
/// finalizers, and a finalizer that re-enters the same cache would
/// self-deadlock on the non-reentrant mutex.
#[must_use = "displaced Python objects must be dropped outside the lock"]
#[expect(dead_code, reason = "fields are read implicitly via Drop")]
pub struct Displaced {
    replaced: Option<Py<PyAny>>,
    evicted: Option<(Py<PyAny>, Py<PyAny>)>,
}

impl Store {
    pub fn new(maxsize: NonZeroUsize) -> Self {
        let cap = maxsize.get().min(MAX_CAPACITY);
        let cap_u32 = cap as u32;

        // Reverse-ordered fill so the LIFO pops 0, 1, 2, ... on the initial insert
        // sequence, giving contiguous slab access on a fresh cache.
        let free: Vec<u32> = (0..cap_u32).rev().collect();

        Self {
            table: HashTable::with_capacity(cap),
            nodes: Box::new_uninit_slice(cap),
            free,
            head: INVALID,
            tail: INVALID,
            len: 0,
            maxsize: cap_u32,
        }
    }

    pub const fn len(&self) -> usize {
        self.len as usize
    }

    /// Look up `key` and bump it to most-recently-used on a hit.
    pub fn touch_get<F>(
        &mut self,
        py: Python<'_>,
        hash: Py_hash_t,
        eq: F,
    ) -> PyResult<Option<Py<PyAny>>>
    where
        F: FnMut(&Py<PyAny>) -> PyResult<bool>,
    {
        let Some(idx) = self.find(hash, eq)? else {
            return Ok(None);
        };
        // SAFETY: `idx` was just returned by `find`.
        let value = unsafe { self.node_ref(idx).value.clone_ref(py) };
        self.move_to_head(idx);
        Ok(Some(value))
    }

    /// Look up `key` without updating the LRU order.
    pub fn peek_get<F>(&self, py: Python<'_>, hash: Py_hash_t, eq: F) -> PyResult<Option<Py<PyAny>>>
    where
        F: FnMut(&Py<PyAny>) -> PyResult<bool>,
    {
        let Some(idx) = self.find(hash, eq)? else {
            return Ok(None);
        };
        // SAFETY: `idx` was just returned by `find`.
        let value = unsafe { self.node_ref(idx).value.clone_ref(py) };
        Ok(Some(value))
    }

    pub fn contains<F>(&self, hash: Py_hash_t, eq: F) -> PyResult<bool>
    where
        F: FnMut(&Py<PyAny>) -> PyResult<bool>,
    {
        Ok(self.find(hash, eq)?.is_some())
    }

    /// Insert or update. Evicts the LRU entry when a fresh insert would exceed
    /// `maxsize`.
    pub fn put<F>(
        &mut self,
        hash: Py_hash_t,
        key: Py<PyAny>,
        value: Py<PyAny>,
        eq: F,
    ) -> PyResult<Displaced>
    where
        F: FnMut(&Py<PyAny>) -> PyResult<bool>,
    {
        if let Some(idx) = self.find(hash, eq)? {
            // SAFETY: `idx` was returned from `find`.
            let replaced = std::mem::replace(unsafe { &mut self.node_mut(idx).value }, value);
            self.move_to_head(idx);
            return Ok(Displaced {
                replaced: Some(replaced),
                evicted: None,
            });
        }

        let evicted = (self.len == self.maxsize).then(|| {
            let tail = self.tail;
            // SAFETY: at-capacity implies `tail` indexes an occupied slot.
            let tail_hash = unsafe { self.node_ref(tail).hash };
            self.detach(tail_hash, tail)
        });

        let idx = self.free.pop().expect("free list invariant violated");
        let prev_head = self.head;
        self.nodes[idx as usize].write(Node {
            hash,
            key,
            value,
            prev: INVALID,
            next: prev_head,
        });
        if prev_head == INVALID {
            self.tail = idx;
        } else {
            // SAFETY: `prev_head` was the head; it is occupied while linked.
            unsafe { self.node_mut(prev_head).prev = idx };
        }
        self.head = idx;
        self.len += 1;

        let nodes = &self.nodes;
        self.table.insert_unique(hash as u64, idx, move |stored| {
            // SAFETY: every index in the table corresponds to an occupied slot.
            unsafe { nodes[*stored as usize].assume_init_ref() }.hash as u64
        });

        Ok(Displaced {
            replaced: None,
            evicted,
        })
    }

    pub fn remove<F>(&mut self, hash: Py_hash_t, eq: F) -> PyResult<Option<(Py<PyAny>, Py<PyAny>)>>
    where
        F: FnMut(&Py<PyAny>) -> PyResult<bool>,
    {
        Ok(self.find(hash, eq)?.map(|idx| self.detach(hash, idx)))
    }

    pub fn clear(&mut self, _py: Python<'_>) {
        if self.len == 0 {
            return;
        }
        self.table.clear();
        // Walk LRU -> MRU via `prev`; `next` points toward the LRU end and would
        // terminate immediately at the tail.
        let mut idx = self.tail;
        self.head = INVALID;
        self.tail = INVALID;
        self.len = 0;
        while idx != INVALID {
            // SAFETY: walking the LRU chain visits each occupied slot exactly once.
            // The `_py` argument keeps us attached, so the inlined `drop` decrements
            // refcounts inline.
            let node = unsafe { self.nodes[idx as usize].assume_init_read() };
            let prev = node.prev;
            drop(node.key);
            drop(node.value);
            self.free.push(idx);
            idx = prev;
        }
    }

    pub fn pop_lru(&mut self) -> Option<(Py<PyAny>, Py<PyAny>)> {
        let idx = self.tail;
        if idx == INVALID {
            return None;
        }
        // SAFETY: `tail` indexes an occupied slot when not `INVALID`.
        let hash = unsafe { self.node_ref(idx).hash };
        Some(self.detach(hash, idx))
    }

    pub const fn iter_keys(&self) -> StoreIter<'_> {
        StoreIter {
            store: self,
            idx: self.tail,
            remaining: self.len(),
        }
    }

    // --- Private helpers
    // -----------------------------------------------------------------------

    fn find<F>(&self, hash: Py_hash_t, mut eq: F) -> PyResult<Option<u32>>
    where
        F: FnMut(&Py<PyAny>) -> PyResult<bool>,
    {
        let mut err: Option<PyErr> = None;
        let nodes = &self.nodes;
        let found = self
            .table
            .find(hash as u64, |&idx| {
                if err.is_some() {
                    return false;
                }
                // SAFETY: every index in the table corresponds to an occupied slot.
                let node = unsafe { nodes[idx as usize].assume_init_ref() };
                match eq(&node.key) {
                    Ok(matched) => matched,
                    Err(e) => {
                        err = Some(e);
                        false
                    },
                }
            })
            .copied();
        err.map_or(Ok(found), Err)
    }

    /// SAFETY: caller must guarantee the slot at `idx` is occupied.
    unsafe fn node_ref(&self, idx: u32) -> &Node {
        // SAFETY: forwarded from the caller's guarantee.
        unsafe { self.nodes[idx as usize].assume_init_ref() }
    }

    /// SAFETY: caller must guarantee the slot at `idx` is occupied.
    unsafe fn node_mut(&mut self, idx: u32) -> &mut Node {
        // SAFETY: forwarded from the caller's guarantee.
        unsafe { self.nodes[idx as usize].assume_init_mut() }
    }

    /// Splice an occupied slot out of the LRU chain.
    fn unlink(&mut self, prev: u32, next: u32) {
        if prev == INVALID {
            self.head = next;
        } else {
            // SAFETY: `prev` is an occupied neighbour link.
            unsafe { self.node_mut(prev).next = next };
        }
        if next == INVALID {
            self.tail = prev;
        } else {
            // SAFETY: `next` is an occupied neighbour link.
            unsafe { self.node_mut(next).prev = prev };
        }
    }

    /// Remove an occupied slot from both the table and the LRU chain, returning
    /// its `(key, value)`.
    fn detach(&mut self, hash: Py_hash_t, idx: u32) -> (Py<PyAny>, Py<PyAny>) {
        match self.table.find_entry(hash as u64, |stored| *stored == idx) {
            Ok(entry) => drop(entry.remove()),
            Err(_) => unreachable!("table desync: occupied slot missing from hashtable"),
        }
        // SAFETY: `idx` was just removed from the table, so the slot was occupied.
        let Node {
            key,
            value,
            prev,
            next,
            ..
        } = unsafe { self.nodes[idx as usize].assume_init_read() };
        self.unlink(prev, next);
        self.free.push(idx);
        self.len -= 1;
        (key, value)
    }

    /// Move an occupied slot to the front of the LRU list.
    fn move_to_head(&mut self, idx: u32) {
        if self.head == idx {
            return;
        }
        // SAFETY: `idx` is occupied while linked.
        let (prev, next) = unsafe {
            let n = self.node_ref(idx);
            (n.prev, n.next)
        };
        self.unlink(prev, next);
        let prev_head = self.head;
        debug_assert_ne!(prev_head, INVALID, "non-empty cache implies a head");
        // SAFETY: `idx` is occupied; we are updating its prev/next to become the new
        // head.
        unsafe {
            let n = self.node_mut(idx);
            n.prev = INVALID;
            n.next = prev_head;
        }
        // SAFETY: `prev_head` was occupied as the previous head.
        unsafe { self.node_mut(prev_head).prev = idx };
        self.head = idx;
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        if self.len == 0 {
            return;
        }
        Python::attach(|py| self.clear(py));
    }
}

pub struct StoreIter<'a> {
    store: &'a Store,
    idx: u32,
    remaining: usize,
}

impl<'a> Iterator for StoreIter<'a> {
    type Item = &'a Py<PyAny>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == INVALID {
            return None;
        }
        // SAFETY: `idx` walks the LRU chain through occupied slots only.
        let node = unsafe { self.store.node_ref(self.idx) };
        self.idx = node.prev;
        self.remaining -= 1;
        Some(&node.key)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for StoreIter<'_> {}
