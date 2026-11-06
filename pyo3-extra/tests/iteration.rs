use pyo3::prelude::*;
use pyo3_extra::iter as py_iter;

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn length_admission_uses_type_slots_and_calls_descriptors_once() {
    Python::initialize();
    Python::attach(|py| -> PyResult<()> {
        let locals = pyo3::types::PyDict::new(py);
        py.run(
            c"
instance_lookups = []
class InstanceOnly:
    def __init__(self):
        object.__setattr__(self, '__len__', lambda: 1)
    def __len__(self):
        return 7
    def __getattribute__(self, name):
        if name == '__len__':
            instance_lookups.append(name)
            raise RuntimeError('instance magic attribute was inspected')
        return object.__getattribute__(self, name)
instance_only = InstanceOnly()

descriptor_calls = []
class LenDescriptor:
    def __get__(self, instance, owner):
        if instance is None:
            return self
        descriptor_calls.append('len')
        return lambda: 7
class DescribedProtocols:
    __len__ = LenDescriptor()
described = DescribedProtocols()
",
            Some(&locals),
            Some(&locals),
        )?;

        let instance_only = locals
            .get_item("instance_only")?
            .expect("instance-only fixture");
        assert_eq!(py_iter::optional_len(&instance_only)?, Some(7));
        let lookups: Vec<String> = locals
            .get_item("instance_lookups")?
            .expect("lookup log")
            .extract()?;
        assert!(lookups.is_empty());

        let described = locals.get_item("described")?.expect("descriptor fixture");
        assert_eq!(py_iter::optional_len(&described)?, Some(7));
        let calls: Vec<String> = locals
            .get_item("descriptor_calls")?
            .expect("descriptor call log")
            .extract()?;
        assert_eq!(calls, ["len"]);
        Ok(())
    })
    .expect("type-slot length admission");
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn optional_length_distinguishes_absence_from_provider_errors() {
    Python::initialize();
    Python::attach(|py| -> PyResult<()> {
        let locals = pyo3::types::PyDict::new(py);
        py.run(
            c"
class Lengthless:
    pass
lengthless = Lengthless()
lengthless.__len__ = lambda: 7
generator = (value for value in range(3))

len_error = RuntimeError('length provider sentinel')
class BrokenLength:
    def __len__(self):
        raise len_error
broken_len = BrokenLength()
",
            Some(&locals),
            Some(&locals),
        )?;

        let lengthless = locals.get_item("lengthless")?.expect("lengthless fixture");
        assert_eq!(py_iter::optional_len(&lengthless)?, None);
        let generator = locals.get_item("generator")?.expect("generator fixture");
        assert_eq!(py_iter::optional_len(&generator)?, None);

        let broken_len = locals
            .get_item("broken_len")?
            .expect("broken length fixture");
        let error =
            py_iter::optional_len(&broken_len).expect_err("length provider error must propagate");
        let sentinel = locals.get_item("len_error")?.expect("length exception");
        assert_eq!(error.value(py).as_ptr(), sentinel.as_ptr());
        Ok(())
    })
    .expect("optional length preserves providers");
}
