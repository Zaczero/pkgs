#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::hash::BuildHasher;
use std::mem::{size_of, size_of_val};
use std::sync::{Arc, Mutex, OnceLock};

use smol_str::SmolStr;

pub(crate) trait HeapSize {
    fn heap_bytes(&self) -> usize;

    fn total_size(&self) -> usize {
        size_of_val(self) + self.heap_bytes()
    }
}

#[macro_export]
macro_rules! heapless {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl $crate::heap_size::HeapSize for $ty {
                fn heap_bytes(&self) -> usize {
                    0
                }
            }
        )+
    };
}

heapless!(
    (),
    bool,
    u8,
    u16,
    u32,
    u64,
    usize,
    i8,
    i16,
    i32,
    i64,
    isize,
    f32,
    f64,
    char,
    &'static str,
);

impl<T: HeapSize, const N: usize> HeapSize for [T; N] {
    fn heap_bytes(&self) -> usize {
        self.iter().map(HeapSize::heap_bytes).sum()
    }
}

impl<A: HeapSize, B: HeapSize> HeapSize for (A, B) {
    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes() + self.1.heap_bytes()
    }
}

impl<T: HeapSize> HeapSize for Option<T> {
    fn heap_bytes(&self) -> usize {
        self.as_ref().map_or(0, HeapSize::heap_bytes)
    }
}

impl<T: HeapSize + ?Sized> HeapSize for Box<T> {
    fn heap_bytes(&self) -> usize {
        (**self).heap_bytes()
    }
}

impl<T: HeapSize> HeapSize for Vec<T> {
    fn heap_bytes(&self) -> usize {
        self.capacity() * size_of::<T>() + self.iter().map(HeapSize::heap_bytes).sum::<usize>()
    }
}

impl<T: HeapSize, S: BuildHasher> HeapSize for std::collections::HashSet<T, S> {
    fn heap_bytes(&self) -> usize {
        self.capacity() * size_of::<T>() + self.iter().map(HeapSize::heap_bytes).sum::<usize>()
    }
}

impl<T: HeapSize> HeapSize for Box<[T]> {
    fn heap_bytes(&self) -> usize {
        self.len() * size_of::<T>() + self.iter().map(HeapSize::heap_bytes).sum::<usize>()
    }
}

impl<T: HeapSize> HeapSize for Arc<T> {
    fn heap_bytes(&self) -> usize {
        (**self).heap_bytes()
    }
}

impl<T: HeapSize> HeapSize for Arc<[T]> {
    fn heap_bytes(&self) -> usize {
        self.len() * size_of::<T>() + self.iter().map(HeapSize::heap_bytes).sum::<usize>()
    }
}

impl HeapSize for String {
    fn heap_bytes(&self) -> usize {
        self.capacity()
    }
}

impl HeapSize for SmolStr {
    fn heap_bytes(&self) -> usize {
        if self.is_heap_allocated() {
            self.len()
        } else {
            0
        }
    }
}

impl HeapSize for str {
    fn heap_bytes(&self) -> usize {
        self.len()
    }
}

impl<T: HeapSize> HeapSize for OnceLock<T> {
    fn heap_bytes(&self) -> usize {
        self.get().map_or(0, HeapSize::heap_bytes)
    }
}

impl<T: HeapSize> HeapSize for Mutex<T> {
    fn heap_bytes(&self) -> usize {
        self.lock().ok().map_or(0, |value| value.heap_bytes())
    }
}
