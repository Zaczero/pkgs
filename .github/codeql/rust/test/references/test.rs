//! Reference conversions use the real PyO3 and CPython API declarations.

use pyo3::types::{PyBytesMethods as _, PyTuple};
use pyo3::{Bound, PyAny, PyResult, Python, ffi};

pub fn bad_index<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    unsafe { Bound::from_borrowed_ptr_or_err(value.py(), ffi::PyNumber_Index(value.as_ptr())) }
}

pub fn bad_alias<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    use pyo3::Bound as Handle;
    use pyo3::ffi::PyNumber_Index as index;
    unsafe { Handle::from_borrowed_ptr_or_err(value.py(), index(value.as_ptr())) }
}

pub fn bad_new_ref<'py>(value: &Bound<'py, PyAny>) -> Bound<'py, PyAny> {
    unsafe { Bound::from_borrowed_ptr(value.py(), ffi::Py_NewRef(value.as_ptr())) }
}

pub fn bad_list(py: Python<'_>) -> Option<Bound<'_, PyAny>> {
    unsafe { Bound::from_borrowed_ptr_or_opt(py, ffi::PyList_New(0)) }
}

pub fn bad_tuple(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    unsafe { Bound::from_borrowed_ptr_or_err(py, ffi::PyTuple_New(0)) }
}

pub fn bad_bytes(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    unsafe {
        Bound::from_borrowed_ptr_or_err(py, ffi::PyBytes_FromStringAndSize(b"x".as_ptr().cast(), 1))
    }
}

pub fn bad_capsule(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    static TOKEN: u8 = 0;
    unsafe {
        Bound::from_borrowed_ptr_or_err(
            py,
            ffi::PyCapsule_New((&raw const TOKEN).cast_mut().cast(), c"test".as_ptr(), None),
        )
    }
}

pub fn good_index<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    unsafe { Bound::from_owned_ptr_or_err(value.py(), ffi::PyNumber_Index(value.as_ptr())) }
}

pub fn good_new_ref<'py>(value: &Bound<'py, PyAny>) -> Bound<'py, PyAny> {
    unsafe { Bound::from_owned_ptr(value.py(), ffi::Py_NewRef(value.as_ptr())) }
}

pub fn good_borrowed_item<'py>(value: &Bound<'py, PyTuple>) -> PyResult<Bound<'py, PyAny>> {
    unsafe { Bound::from_borrowed_ptr_or_err(value.py(), ffi::PyTuple_GetItem(value.as_ptr(), 0)) }
}

pub fn good_unrelated<'py>(value: &Bound<'py, PyAny>) -> Bound<'py, PyAny> {
    #[allow(non_snake_case)]
    fn PyNumber_Index(value: &Bound<'_, PyAny>) -> *mut ffi::PyObject {
        value.as_ptr()
    }
    unsafe { Bound::from_borrowed_ptr(value.py(), PyNumber_Index(value)) }
}

pub fn bad_local_new_reference<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    let pointer = unsafe { ffi::PyNumber_Index(value.as_ptr()) };
    let alias = pointer;
    unsafe { Bound::from_borrowed_ptr_or_err(value.py(), alias) }
}

pub fn bad_dict_factory(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let pointer = unsafe { ffi::PyDict_New() };
    unsafe { Bound::from_borrowed_ptr_or_err(py, pointer) }
}

pub fn bad_new_reference_view<'a, 'py>(
    py: Python<'py>,
) -> PyResult<pyo3::Borrowed<'a, 'py, PyAny>> {
    unsafe { pyo3::Borrowed::from_ptr_or_err(py, ffi::PyList_New(0)) }
}

pub fn good_retained_reference_view<'a, 'py>(
    value: &'a Bound<'py, PyAny>,
) -> pyo3::Borrowed<'a, 'py, PyAny> {
    unsafe { pyo3::Borrowed::from_ptr(value.py(), value.as_ptr()) }
}

pub fn good_local_new_reference<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    let pointer = unsafe { ffi::PyNumber_Index(value.as_ptr()) };
    unsafe { Bound::from_owned_ptr_or_err(value.py(), pointer) }
}

unsafe fn borrowed_conversion(
    py: Python<'_>,
    pointer: *mut ffi::PyObject,
) -> PyResult<Bound<'_, PyAny>> {
    unsafe { Bound::from_borrowed_ptr_or_err(py, pointer) }
}

pub fn bad_helper_new_reference<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    let pointer = unsafe { ffi::PyNumber_Index(value.as_ptr()) };
    unsafe { borrowed_conversion(value.py(), pointer) }
}

unsafe fn new_list_pointer() -> *mut ffi::PyObject {
    unsafe { ffi::PyList_New(0) }
}

pub fn bad_factory_helper(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    unsafe { Bound::from_borrowed_ptr_or_err(py, new_list_pointer()) }
}

pub fn good_separately_released<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    let pointer = unsafe { ffi::PyNumber_Index(value.as_ptr()) };
    let result = unsafe { Bound::from_borrowed_ptr_or_err(value.py(), pointer) };
    unsafe { ffi::Py_XDECREF(pointer) };
    result
}

pub fn bad_borrowed_as_owned<'py>(value: &Bound<'py, PyTuple>) -> PyResult<Bound<'py, PyAny>> {
    let pointer = unsafe { ffi::PyTuple_GetItem(value.as_ptr(), 0) };
    unsafe { Bound::from_owned_ptr_or_err(value.py(), pointer) }
}

pub fn bad_borrowed_alias<'py>(value: &Bound<'py, PyTuple>) -> Option<Bound<'py, PyAny>> {
    use pyo3::Bound as Owner;
    use pyo3::ffi::PyTuple_GetItem as item;
    unsafe { Owner::from_owned_ptr_or_opt(value.py(), item(value.as_ptr(), 0)) }
}

pub fn good_explicit_retain<'py>(value: &Bound<'py, PyTuple>) -> PyResult<Bound<'py, PyAny>> {
    let pointer = unsafe { ffi::PyTuple_GetItem(value.as_ptr(), 0) };
    unsafe { ffi::Py_XINCREF(pointer) };
    unsafe { Bound::from_owned_ptr_or_err(value.py(), pointer) }
}

pub fn good_new_ref_from_borrowed<'py>(value: &Bound<'py, PyTuple>) -> PyResult<Bound<'py, PyAny>> {
    let pointer = unsafe { ffi::PyTuple_GetItem(value.as_ptr(), 0) };
    let owned = unsafe { ffi::Py_XNewRef(pointer) };
    unsafe { Bound::from_owned_ptr_or_err(value.py(), owned) }
}

pub fn good_unrelated_borrowed_name<'py>(value: &Bound<'py, PyAny>) -> Bound<'py, PyAny> {
    #[allow(non_snake_case)]
    fn PyTuple_GetItem(value: &Bound<'_, PyAny>) -> *mut ffi::PyObject {
        value.clone().into_ptr()
    }
    unsafe { Bound::from_owned_ptr(value.py(), PyTuple_GetItem(value)) }
}

pub fn bad_owner_pointer_adopted<'py>(value: &Bound<'py, PyAny>) -> Bound<'py, PyAny> {
    unsafe { Bound::from_owned_ptr(value.py(), value.as_ptr()) }
}

pub fn bad_stolen_owner_pointer(list: &Bound<'_, pyo3::types::PyList>, value: &Bound<'_, PyAny>) {
    unsafe { ffi::PyList_SetItem(list.as_ptr(), 0, value.as_ptr()) };
}

pub fn bad_stolen_borrowed_item(list: &Bound<'_, pyo3::types::PyList>, tuple: &Bound<'_, PyTuple>) {
    let pointer = unsafe { ffi::PyTuple_GetItem(tuple.as_ptr(), 0) };
    unsafe { ffi::PyList_SetItem(list.as_ptr(), 0, pointer) };
}

pub fn good_stolen_transferred_owner(
    list: &Bound<'_, pyo3::types::PyList>,
    value: Bound<'_, PyAny>,
) {
    unsafe { ffi::PyList_SetItem(list.as_ptr(), 0, value.into_ptr()) };
}

pub fn good_nonstealing_dict(dict: &Bound<'_, pyo3::types::PyDict>, value: &Bound<'_, PyAny>) {
    unsafe { ffi::PyDict_SetItem(dict.as_ptr(), value.as_ptr(), value.as_ptr()) };
}

pub fn bad_success_only_steal(module: &Bound<'_, pyo3::types::PyModule>, value: &Bound<'_, PyAny>) {
    unsafe { ffi::PyModule_AddObject(module.as_ptr(), c"value".as_ptr(), value.as_ptr()) };
}

pub fn good_success_only_steal(
    module: &Bound<'_, pyo3::types::PyModule>,
    value: Bound<'_, PyAny>,
) -> PyResult<()> {
    let pointer = value.into_ptr();
    if unsafe { ffi::PyModule_AddObject(module.as_ptr(), c"value".as_ptr(), pointer) } < 0 {
        unsafe { ffi::Py_DECREF(pointer) };
        return Err(pyo3::PyErr::fetch(module.py()));
    }
    Ok(())
}

pub fn bad_truth_error_as_true(value: &Bound<'_, PyAny>) -> bool {
    unsafe { ffi::PyObject_IsTrue(value.as_ptr()) != 0 }
}

pub fn bad_local_truth_error_as_true(value: &Bound<'_, PyAny>) -> bool {
    let status = unsafe { ffi::PyObject_IsTrue(value.as_ptr()) };
    status != 0
}

pub fn good_checked_truth(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    let status = unsafe { ffi::PyObject_IsTrue(value.as_ptr()) };
    if status < 0 {
        return Err(pyo3::PyErr::fetch(value.py()));
    }
    Ok(status != 0)
}

pub fn good_nonnegative_truth_branch(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    let status = unsafe { ffi::PyObject_IsTrue(value.as_ptr()) };
    if status >= 0 {
        Ok(status != 0)
    } else {
        Err(pyo3::PyErr::fetch(value.py()))
    }
}

pub fn good_nonfailing_type_check(value: &Bound<'_, PyAny>) -> bool {
    unsafe { ffi::PyIndex_Check(value.as_ptr()) != 0 }
}

pub fn bad_unrelated_status_check(value: &Bound<'_, PyAny>, other: i32) -> PyResult<bool> {
    let status = unsafe { ffi::PyObject_IsTrue(value.as_ptr()) };
    if other < 0 {
        return Err(pyo3::PyErr::fetch(value.py()));
    }
    Ok(status != 0)
}

pub unsafe fn bad_writer_finish_then_discard(writer: *mut ffi::compat::PyBytesWriter) {
    unsafe {
        ffi::compat::PyBytesWriter_Finish(writer);
        ffi::compat::PyBytesWriter_Discard(writer);
    }
}

pub unsafe fn bad_writer_failure_cleanup(writer: *mut ffi::compat::PyBytesWriter) {
    unsafe {
        let result = ffi::compat::PyBytesWriter_Finish(writer);
        if result.is_null() {
            ffi::compat::PyBytesWriter_Discard(writer);
        }
    }
}

pub unsafe fn bad_writer_finish_then_read(
    writer: *mut ffi::compat::PyBytesWriter,
) -> *mut std::ffi::c_void {
    unsafe {
        ffi::compat::PyBytesWriter_Finish(writer);
        ffi::compat::PyBytesWriter_GetData(writer)
    }
}

pub unsafe fn good_writer_alternative_paths(
    writer: *mut ffi::compat::PyBytesWriter,
    publish: bool,
) {
    unsafe {
        if publish {
            ffi::compat::PyBytesWriter_Finish(writer);
        } else {
            ffi::compat::PyBytesWriter_Discard(writer);
        }
    }
}

pub unsafe fn good_writer_distinct_handles(
    first: *mut ffi::compat::PyBytesWriter,
    second: *mut ffi::compat::PyBytesWriter,
) {
    unsafe {
        ffi::compat::PyBytesWriter_Finish(first);
        ffi::compat::PyBytesWriter_Discard(second);
    }
}

pub fn bad_allocation_as_absence(py: Python<'_>) -> Option<Bound<'_, PyAny>> {
    unsafe { Bound::from_owned_ptr_or_opt(py, ffi::PyList_New(0)) }
}

pub unsafe fn bad_writer_nonnull_handle(writer: std::ptr::NonNull<ffi::compat::PyBytesWriter>) {
    unsafe {
        ffi::compat::PyBytesWriter_Finish(writer.as_ptr());
        ffi::compat::PyBytesWriter_Discard(writer.as_ptr());
    }
}

pub fn bad_protocol_as_absence<'py>(value: &Bound<'py, PyAny>) -> Option<Bound<'py, PyAny>> {
    let pointer = unsafe { ffi::PyNumber_Index(value.as_ptr()) };
    unsafe { Bound::from_owned_ptr_or_opt(value.py(), pointer) }
}

pub fn good_checked_allocation(py: Python<'_>) -> PyResult<Option<Bound<'_, PyAny>>> {
    let pointer = unsafe { ffi::PyList_New(0) };
    if pointer.is_null() {
        return Err(pyo3::PyErr::fetch(py));
    }
    Ok(unsafe { Bound::from_owned_ptr_or_opt(py, pointer) })
}

pub fn good_optional_error_indicator(py: Python<'_>) -> Option<Bound<'_, PyAny>> {
    unsafe { Bound::from_borrowed_ptr_or_opt(py, ffi::PyErr_Occurred()) }
}

unsafe fn raw_python_work() {
    unsafe { ffi::PyList_New(0) };
}

pub fn bad_detached_object_allocation(py: Python<'_>) {
    py.detach(|| {
        unsafe { ffi::PyList_New(0) };
    });
}

pub fn bad_detached_helper(py: Python<'_>) {
    py.detach(|| unsafe { raw_python_work() });
}

pub fn good_detached_reattachment(py: Python<'_>) {
    py.detach(|| {
        Python::attach(|_| unsafe { raw_python_work() });
    });
}

pub fn good_detached_native_work(py: Python<'_>, values: &[u64]) -> u64 {
    py.detach(|| values.iter().sum())
}

pub struct TraversalFixture;

impl TraversalFixture {
    pub fn __traverse__(
        &self,
        _visit: pyo3::pyclass::PyVisit<'_>,
    ) -> Result<(), pyo3::pyclass::PyTraverseError> {
        unsafe { raw_python_work() };
        Ok(())
    }
}

pub struct GoodTraversalFixture {
    child: pyo3::Py<PyAny>,
}

pub fn bad_python_storage_as_vec(bytes: &Bound<'_, pyo3::types::PyBytes>) -> Vec<u8> {
    let pointer = unsafe { ffi::PyBytes_AsString(bytes.as_ptr()) }.cast::<u8>();
    unsafe { Vec::from_raw_parts(pointer, bytes.as_bytes().len(), bytes.as_bytes().len()) }
}

pub fn bad_python_storage_as_string(bytes: &Bound<'_, pyo3::types::PyBytes>) -> String {
    let pointer = unsafe { ffi::PyBytes_AsString(bytes.as_ptr()) }.cast::<u8>();
    unsafe { String::from_raw_parts(pointer, bytes.as_bytes().len(), bytes.as_bytes().len()) }
}

pub fn good_python_storage_copy(bytes: &Bound<'_, pyo3::types::PyBytes>) -> Vec<u8> {
    let pointer = unsafe { ffi::PyBytes_AsString(bytes.as_ptr()) }.cast::<u8>();
    unsafe { std::slice::from_raw_parts(pointer, bytes.as_bytes().len()) }.to_vec()
}

impl GoodTraversalFixture {
    pub fn __traverse__(
        &self,
        visit: pyo3::pyclass::PyVisit<'_>,
    ) -> Result<(), pyo3::pyclass::PyTraverseError> {
        visit.call(&self.child)
    }
}

pub fn good_manual_owner_transfer(value: Bound<'_, PyAny>) -> Bound<'_, PyAny> {
    let py = value.py();
    let retained = std::mem::ManuallyDrop::new(value);
    unsafe { Bound::from_owned_ptr(py, retained.as_ptr()) }
}

pub fn bad_manual_borrowed_wrapper<'py>(value: &Bound<'py, PyAny>) -> Bound<'py, PyAny> {
    let retained = std::mem::ManuallyDrop::new(value);
    unsafe { Bound::from_owned_ptr(value.py(), retained.as_ptr()) }
}

pub struct WriterState {
    writer: PyResult<std::ptr::NonNull<ffi::PyBytesWriter>>,
}

pub fn bad_consumed_writer_from_state(state: WriterState) {
    let state = std::mem::ManuallyDrop::new(state);
    match unsafe { std::ptr::read(&state.writer) } {
        Ok(writer) => unsafe {
            ffi::PyBytesWriter_Finish(writer.as_ptr());
            ffi::PyBytesWriter_Discard(writer.as_ptr());
        },
        Err(_) => {},
    }
}

pub fn good_alternative_writer_from_state(state: WriterState, finish: bool) {
    let state = std::mem::ManuallyDrop::new(state);
    match unsafe { std::ptr::read(&state.writer) } {
        Ok(writer) => unsafe {
            if finish {
                ffi::PyBytesWriter_Finish(writer.as_ptr());
            } else {
                ffi::PyBytesWriter_Discard(writer.as_ptr());
            }
        },
        Err(_) => {},
    }
}

pub struct WriterPair {
    first: std::cell::Cell<*mut ffi::PyBytesWriter>,
    second: *mut ffi::PyBytesWriter,
}

impl WriterPair {
    fn as_ptr(&self) -> *mut ffi::PyBytesWriter {
        self.first.replace(self.second)
    }
}

pub fn good_distinct_writer_getter(pair: WriterPair) {
    unsafe {
        ffi::PyBytesWriter_Finish(pair.as_ptr());
        ffi::PyBytesWriter_Discard(pair.as_ptr());
    }
}

pub fn bad_binary_number_factory<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    unsafe {
        Bound::from_borrowed_ptr_or_err(
            value.py(),
            ffi::PyNumber_Add(value.as_ptr(), value.as_ptr()),
        )
    }
}

pub fn good_binary_number_factory<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    unsafe {
        Bound::from_owned_ptr_or_err(
            value.py(),
            ffi::PyNumber_Add(value.as_ptr(), value.as_ptr()),
        )
    }
}
