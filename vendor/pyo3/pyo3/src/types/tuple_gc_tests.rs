use crate as pyo3;
use alloc::vec::Vec;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

#[test]
#[cfg(feature = "nightly")]
fn trusted_tuple_iterators_preserve_ranges_and_random_access() {
    fn check<I>(mut iter: I)
    where
        I: Iterator<Item = i32>
            + DoubleEndedIterator
            + ExactSizeIterator
            + core::iter::TrustedLen
            + core::iter::TrustedFused
            + core::iter::TrustedRandomAccess,
    {
        assert_eq!(iter.size_hint(), (8, Some(8)));
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.next_back(), Some(7));
        assert_eq!(iter.nth(1), Some(2));
        assert_eq!(iter.size_hint(), (4, Some(4)));
        // SAFETY: index 1 is below the remaining length 4. After this access
        // only two disjoint backward reads, length queries and drop are used.
        assert_eq!(unsafe { iter.__iterator_get_unchecked(1) }, 4);
        assert_eq!(iter.len(), 4);
        assert_eq!(iter.next_back(), Some(6));
        assert_eq!(iter.next_back(), Some(5));
        assert_eq!(iter.size_hint(), (2, Some(2)));
    }

    Python::initialize();
    Python::attach(|py| {
        let tuple = PyTuple::new(py, 0..8).unwrap();
        check(tuple.iter().map(|item| item.extract::<i32>().unwrap()));
        check(
            tuple
                .iter_borrowed()
                .map(|item| item.extract::<i32>().unwrap()),
        );
        let zipped = tuple
            .iter_borrowed()
            .zip(tuple.iter())
            .rev()
            .map(|(a, b)| (a.extract::<i32>().unwrap(), b.extract::<i32>().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(zipped, (0..8).rev().map(|i| (i, i)).collect::<Vec<_>>());
        let mut exhausted = tuple.iter();
        assert!(exhausted.advance_by(usize::MAX).is_err());
        assert_eq!(exhausted.size_hint(), (0, Some(0)));
        assert!(exhausted.next().is_none());
        assert!(exhausted.next_back().is_none());
        let mut borrowed = tuple.iter_borrowed();
        assert!(borrowed.nth(usize::MAX).is_none());
        assert_eq!(borrowed.size_hint(), (0, Some(0)));
        assert!(borrowed.next_back().is_none());
    });
}

struct CustomValue<'py>(Bound<'py, PyAny>);

impl<'py> IntoPyObject<'py> for CustomValue<'py> {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, _py: Python<'py>) -> PyResult<Self::Output> {
        Ok(self.0)
    }
}

#[test]
fn custom_conversions_cannot_claim_atomic_results() {
    Python::initialize();
    Python::attach(|py| {
        let list = pyo3::types::PyList::empty(py);
        let tuple = (CustomValue(list.clone().into_any()), 42_u8)
            .into_pyobject(py)
            .unwrap();
        list.append(&tuple).unwrap();
        assert!(
            py.import("gc")
                .unwrap()
                .call_method1("is_tracked", (&tuple,))
                .unwrap()
                .is_truthy()
                .unwrap()
        );
    });
}

#[test]
fn unknown_length_iterator_is_consumed_once() {
    Python::initialize();
    Python::attach(|py| {
        let mut calls = 0;
        let stream = std::iter::from_fn(|| {
            calls += 1;
            (calls <= 4).then_some(calls)
        });
        let tuple = PyTuple::new(py, stream).unwrap();
        assert_eq!(tuple.extract::<Vec<i32>>().unwrap(), vec![1, 2, 3, 4]);
        assert_eq!(calls, 5);
    });
}

#[test]
fn ordinary_tuple_construction_handles_filtered_iterators_and_gc() {
    Python::initialize();
    Python::attach(|py| {
        let tuple = PyTuple::new(py, (0_u32..20).filter(|value| value % 2 == 0)).unwrap();
        assert_eq!(
            tuple.extract::<Vec<u32>>().unwrap(),
            (0..20).step_by(2).collect::<Vec<_>>()
        );
        let gc = py.import("gc").unwrap();
        assert!(
            !gc.call_method1("is_tracked", (&tuple,))
                .unwrap()
                .is_truthy()
                .unwrap()
        );
        let scalar = (1.25_f64, 2.5_f64).into_pyobject(py).unwrap();
        assert!(
            !gc.call_method1("is_tracked", (&scalar,))
                .unwrap()
                .is_truthy()
                .unwrap()
        );
        let list = pyo3::types::PyList::empty(py);
        let tracked = PyTuple::new(py, [&list]).unwrap();
        list.append(&tracked).unwrap();
        assert!(
            gc.call_method1("is_tracked", (&tracked,))
                .unwrap()
                .is_truthy()
                .unwrap()
        );
    });
}

#[test]
fn atomic_scalars_are_untracked_and_preserve_values() {
    Python::initialize();
    Python::attach(|py| {
        let gc = py.import("gc").unwrap();
        let point = (1.25_f64, -2.5_f64).into_pyobject(py).unwrap();
        let triple = (f64::INFINITY, -0.0_f64, 255_u8).into_pyobject(py).unwrap();
        assert_eq!(point.extract::<(f64, f64)>().unwrap(), (1.25, -2.5));
        assert!(
            !gc.call_method1("is_tracked", (&point,))
                .unwrap()
                .is_truthy()
                .unwrap()
        );
        assert!(
            !gc.call_method1("is_tracked", (&triple,))
                .unwrap()
                .is_truthy()
                .unwrap()
        );
        let (infinity, zero, integer) = triple.extract::<(f64, f64, u8)>().unwrap();
        assert!(infinity.is_infinite() && infinity.is_sign_positive());
        assert!(zero == 0.0 && zero.is_sign_negative());
        assert_eq!(integer, 255);
    });
}

#[test]
fn borrowed_pairs_retain_identity_and_collect_cycles() {
    Python::initialize();
    Python::attach(|py| {
        let namespace = PyDict::new(py);
        py.run(c"class Node: pass\nnode = Node()", Some(&namespace), None)
            .unwrap();
        let node = namespace.get_item("node").unwrap().unwrap();
        let weak = py
            .import("weakref")
            .unwrap()
            .getattr("ref")
            .unwrap()
            .call1((&node,))
            .unwrap();
        let tuple = PyTuple::new(py, [&node, &node]).unwrap();
        assert!(tuple.get_item(0).unwrap().is(&node));
        assert!(tuple.get_item(1).unwrap().is(&node));
        node.setattr("cycle", &tuple).unwrap();
        namespace.del_item("node").unwrap();
        drop(node);
        assert!(!weak.call0().unwrap().is_none());
        drop(tuple);
        py.import("gc").unwrap().call_method0("collect").unwrap();
        assert!(weak.call0().unwrap().is_none());
    });
}

#[test]
fn borrowed_arrays_support_empty_and_long_tuples() {
    Python::initialize();
    Python::attach(|py| {
        let empty = PyTuple::new(py, std::iter::empty::<u8>()).unwrap();
        assert!(empty.is_empty());
        let value = 123_u32.into_pyobject(py).unwrap().into_any();
        let long = PyTuple::new(py, [&value; 17]).unwrap();
        assert_eq!(long.len(), 17);
        assert!(long.iter().all(|item| item.is(&value)));
    });
}

#[test]
fn gc_capable_bytes_subclasses_stay_tracked() {
    Python::initialize();
    Python::attach(|py| {
        let namespace = PyDict::new(py);
        py.run(
            c"class Bytes(bytes): pass\nvalue = Bytes(b'header')",
            Some(&namespace),
            None,
        )
        .unwrap();
        let value = namespace.get_item("value").unwrap().unwrap();
        let tuple = PyTuple::new(py, [&value, &value]).unwrap();
        value.setattr("cycle", &tuple).unwrap();
        assert!(
            py.import("gc")
                .unwrap()
                .call_method1("is_tracked", (&tuple,))
                .unwrap()
                .is_truthy()
                .unwrap()
        );
    });
}
