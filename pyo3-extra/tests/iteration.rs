use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_extra::iter as py_iter;

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn protocol_classification_uses_type_slots_and_calls_descriptors_once() {
    Python::initialize();
    Python::attach(|py| -> PyResult<()> {
        let locals = pyo3::types::PyDict::new(py);
        py.run(
            c"
instance_lookups = []
class InstanceOnly:
    def __init__(self):
        object.__setattr__(self, '__iter__', lambda: iter((1,)))
        object.__setattr__(self, '__getitem__', lambda index: 1)
        object.__setattr__(self, '__len__', lambda: 1)
    def __len__(self):
        return 7
    def __getattribute__(self, name):
        if name in ('__iter__', '__getitem__', '__len__'):
            instance_lookups.append(name)
            raise RuntimeError('instance magic attribute was inspected')
        return object.__getattribute__(self, name)
instance_only = InstanceOnly()

descriptor_calls = []
class IterDescriptor:
    def __get__(self, instance, owner):
        if instance is None:
            return self
        descriptor_calls.append('iter')
        return lambda: iter(range(7))
class LenDescriptor:
    def __get__(self, instance, owner):
        if instance is None:
            return self
        descriptor_calls.append('len')
        return lambda: 7
class DescribedProtocols:
    __iter__ = IterDescriptor()
    __len__ = LenDescriptor()
described = DescribedProtocols()
",
            Some(&locals),
            Some(&locals),
        )?;

        let instance_only = locals
            .get_item("instance_only")?
            .expect("instance-only fixture");
        assert!(!py_iter::has_iter(&instance_only));
        assert!(py_iter::try_iter(&instance_only)?.is_none());
        assert_eq!(py_iter::optional_len(&instance_only)?, Some(7));
        let lookups: Vec<String> = locals
            .get_item("instance_lookups")?
            .expect("lookup log")
            .extract()?;
        assert!(lookups.is_empty());

        let described = locals.get_item("described")?.expect("descriptor fixture");
        let iter = py_iter::try_iter(&described)?.expect("type supplies __iter__");
        let items = py_iter::collect_from(&described, iter, |item| item.extract::<i64>())?;
        assert_eq!(items, [0, 1, 2, 3, 4, 5, 6]);
        let calls: Vec<String> = locals
            .get_item("descriptor_calls")?
            .expect("descriptor call log")
            .extract()?;
        assert_eq!(calls, ["iter", "len"]);
        Ok(())
    })
    .expect("type-slot protocol classification");
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn protocol_admission_preserves_legacy_sequences_and_provider_errors() {
    Python::initialize();
    Python::attach(|py| -> PyResult<()> {
        let locals = pyo3::types::PyDict::new(py);
        py.run(
            c"
class DisabledIterator:
    __iter__ = None
    def __getitem__(self, index):
        return index
disabled = DisabledIterator()

class LegacySequence:
    def __getitem__(self, index):
        if index == 3:
            raise IndexError
        return index + 10
legacy = LegacySequence()

iter_error = TypeError('iterator provider sentinel')
class BrokenIterator:
    def __iter__(self):
        raise iter_error
broken_iter = BrokenIterator()

len_error = RuntimeError('length provider sentinel')
class BrokenLength:
    def __len__(self):
        raise len_error
broken_len = BrokenLength()
",
            Some(&locals),
            Some(&locals),
        )?;

        let disabled = locals
            .get_item("disabled")?
            .expect("disabled iterator fixture");
        let Err(error) = py_iter::try_iter(&disabled) else {
            panic!("__iter__ = None must keep Python's TypeError");
        };
        assert!(error.is_instance_of::<PyTypeError>(py));

        let legacy = locals.get_item("legacy")?.expect("legacy sequence fixture");
        let iter = py_iter::try_iter(&legacy)?.expect("legacy sequence protocol");
        let items = py_iter::collect_from(&legacy, iter, |item| item.extract::<i64>())?;
        assert_eq!(items, [10, 11, 12]);

        let broken_iter = locals
            .get_item("broken_iter")?
            .expect("broken iterator fixture");
        let Err(error) = py_iter::try_iter(&broken_iter) else {
            panic!("iterator provider error must propagate");
        };
        let sentinel = locals.get_item("iter_error")?.expect("iterator exception");
        assert_eq!(error.value(py).as_ptr(), sentinel.as_ptr());

        let broken_len = locals
            .get_item("broken_len")?
            .expect("broken length fixture");
        let error =
            py_iter::optional_len(&broken_len).expect_err("length provider error must propagate");
        let sentinel = locals.get_item("len_error")?.expect("length exception");
        assert_eq!(error.value(py).as_ptr(), sentinel.as_ptr());
        Ok(())
    })
    .expect("protocol admission preserves providers");
}
