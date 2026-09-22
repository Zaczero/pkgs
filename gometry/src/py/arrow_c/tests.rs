use std::ptr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use libc::{EINVAL, ENOMEM};
use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::types::PyList;

use crate::geometry::{LineSeq, Point, Polygon, Ring, Shape};
use crate::py::arrow::ArrowEncoding;
use crate::py::arrow_c::capsule_lifecycle::stream_from_export;
use crate::py::arrow_c::*;

static NATIVE_SIGNAL_STREAM_TEST_LOCK: Mutex<()> = Mutex::new(());
static NATIVE_SCHEMA_TEST_LOCK: Mutex<()> = Mutex::new(());
static NATIVE_SIGNAL_ARRAY_RELEASES: AtomicUsize = AtomicUsize::new(0);

fn release_twice(export: ExportedArray) {
    let mut schema = export.schema;
    let mut array = export.array.into_box();
    // SAFETY: this test owns both boxed C structs and is explicitly
    // exercising idempotent release callbacks before the boxes drop.
    unsafe {
        release_schema(schema.as_mut());
        release_schema(schema.as_mut());
        release_array(array.as_mut());
        release_array(array.as_mut());
    }
}

fn release_schema_twice(mut schema: Box<ArrowSchema>) {
    // SAFETY: this test owns the boxed C schema and is explicitly
    // exercising idempotent release callbacks before the shell drops.
    unsafe {
        release_schema(schema.as_mut());
        release_schema(schema.as_mut());
    }
}

fn exported(geometry: &PyGeometry) -> ExportedArray {
    let shapes = vec![geometry.shape.shape().clone()];
    let plan = plan_shapes(
        &shapes,
        geometry.crs_str().map(str::to_owned),
        geometry.epoch(),
    )
    .expect("test export plan");
    materialize_export_plan(prepare_export_plan(plan).expect("test export schema"))
        .expect("test export")
}

#[expect(
    clippy::unnecessary_box_returns,
    reason = "C Data Interface consumers hold the schema's address, which must stay stable"
)]
fn exported_schema(geometry: &PyGeometry) -> Box<ArrowSchema> {
    let shapes = vec![geometry.shape.shape().clone()];
    let plan = plan_shapes(
        &shapes,
        geometry.crs_str().map(str::to_owned),
        geometry.epoch(),
    )
    .expect("test schema plan");
    materialize_schema_plan(prepare_export_plan(plan).expect("test schema"))
        .expect("test schema")
        .into_box()
}

fn exported_array(array: &PyGeometryArray) -> ExportedArray {
    let plan = plan_geometry_array(array, ArrowEncoding::Auto).expect("test array export plan");
    materialize_export_plan(prepare_export_plan(plan).expect("test array export schema"))
        .expect("test array export")
}

#[derive(Debug, Eq, PartialEq)]
struct SchemaSnapshot {
    format: Vec<u8>,
    name: Vec<u8>,
    metadata: Option<Vec<u8>>,
    flags: i64,
    children: Vec<Self>,
}

unsafe fn metadata_snapshot(metadata: *const c_char) -> Option<Vec<u8>> {
    if metadata.is_null() {
        return None;
    }
    let bytes = metadata.cast::<u8>();
    let read_i32 = |offset: usize| {
        // SAFETY: production metadata is a native-endian i32 envelope and the
        // caller only uses offsets admitted by the preceding length fields.
        unsafe { ptr::read_unaligned(bytes.add(offset).cast::<i32>()) }
    };
    let count = usize::try_from(read_i32(0)).expect("metadata count is non-negative");
    let mut end = std::mem::size_of::<i32>();
    for _ in 0..count {
        let key_len = usize::try_from(read_i32(end)).expect("metadata key length");
        end = end
            .checked_add(std::mem::size_of::<i32>() + key_len)
            .expect("metadata key envelope length");
        let value_len = usize::try_from(read_i32(end)).expect("metadata value length");
        end = end
            .checked_add(std::mem::size_of::<i32>() + value_len)
            .expect("metadata value envelope length");
    }
    // SAFETY: the length was decoded from the complete production envelope.
    Some(unsafe { std::slice::from_raw_parts(bytes, end) }.to_vec())
}

unsafe fn schema_snapshot(schema: *const ArrowSchema) -> SchemaSnapshot {
    assert!(!schema.is_null());
    // SAFETY: callers provide a live production schema tree and retain it for
    // the duration of this recursive copy.
    let schema = unsafe { &*schema };
    let format = if schema.format.is_null() {
        Vec::new()
    } else {
        // SAFETY: schema text is NUL-terminated by the schema owner.
        unsafe { CStr::from_ptr(schema.format) }.to_bytes().to_vec()
    };
    let name = if schema.name.is_null() {
        Vec::new()
    } else {
        // SAFETY: schema text is NUL-terminated by the schema owner.
        unsafe { CStr::from_ptr(schema.name) }.to_bytes().to_vec()
    };
    let child_count = usize::try_from(schema.n_children).expect("schema child count");
    let children = if child_count == 0 {
        Vec::new()
    } else {
        assert!(!schema.children.is_null());
        // SAFETY: the owner retains a pointer table with n_children entries.
        unsafe { std::slice::from_raw_parts(schema.children, child_count) }
            .iter()
            .map(|child| {
                // SAFETY: every child pointer in a production schema tree is live.
                unsafe { schema_snapshot(*child) }
            })
            .collect()
    };
    SchemaSnapshot {
        format,
        name,
        // SAFETY: the metadata pointer is owned by this live schema.
        metadata: unsafe { metadata_snapshot(schema.metadata) },
        flags: schema.flags,
        children,
    }
}

fn schema_only_snapshot(py: Python<'_>, array: &PyGeometryArray) -> PyResult<SchemaSnapshot> {
    let capsule = array_to_schema_capsule(py, array)?;
    // SAFETY: the capsule was just created by the production schema exporter
    // with the exact name used here and remains alive until after the copy.
    let pointer =
        unsafe { ffi::PyCapsule_GetPointer(capsule.bind(py).as_ptr(), schema_capsule_name()) };
    if pointer.is_null() {
        return Err(PyErr::fetch(py));
    }
    // SAFETY: the capsule owns a live ArrowSchema until it is dropped below.
    let snapshot = unsafe { schema_snapshot(pointer.cast::<ArrowSchema>().cast_const()) };
    drop(capsule);
    Ok(snapshot)
}

fn install_embedded_gometry_module(py: Python<'_>) -> PyResult<()> {
    // Match the native-module setup used by the embedded Python tests in
    // lib.rs, then let the real source package load _arrow.py from this crate.
    let native = PyModule::new(py, "gometry._lib")?;
    crate::lib(&native)?;

    let package = PyModule::new(py, "gometry")?;
    let package_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("python/gometry");
    let package_path = package_path.to_str().expect("gometry source path is UTF-8");
    package.setattr("__path__", PyList::new(py, [package_path])?)?;
    package.setattr("__package__", "gometry")?;

    let modules = PyModule::import(py, "sys")?.getattr("modules")?;
    modules.set_item("gometry", &package)?;
    modules.set_item("gometry._lib", &native)?;
    Ok(())
}

unsafe extern "C" fn counted_array_release(array: *mut ArrowArray) {
    NATIVE_SIGNAL_ARRAY_RELEASES.fetch_add(1, Ordering::Relaxed);
    // SAFETY: the callback receives a live production-owned array shell from
    // this test stream and forwards its release to the production callback.
    unsafe { release_array(array) }
}

struct NativeSignalStreamState {
    schema: SchemaNode,
    batches: Vec<Option<Box<ArrowArray>>>,
    next: usize,
    interrupt_at: usize,
    remaining_at_release: Arc<AtomicUsize>,
    released: Arc<AtomicBool>,
}

impl Drop for NativeSignalStreamState {
    fn drop(&mut self) {
        let remaining = self.batches.iter().filter(|batch| batch.is_some()).count();
        self.remaining_at_release
            .store(remaining, Ordering::Release);
        for batch in &mut self.batches {
            if let Some(mut batch) = batch.take()
                && let Some(release) = batch.release
            {
                // SAFETY: `batch` is a uniquely owned production Arrow shell
                // taken out of this stream state; `release` is that shell's
                // callback and consumes the C struct through `as_mut()`.
                unsafe { release(batch.as_mut()) }
            }
        }
        self.released.store(true, Ordering::Release);
    }
}

unsafe extern "C" fn native_signal_stream_get_schema(
    stream: *mut ArrowArrayStream,
    out: *mut ArrowSchema,
) -> c_int {
    if stream.is_null() || out.is_null() {
        return EINVAL;
    }
    // SAFETY: the importer calls this with the live test stream and a writable
    // output shell. private_data is the NativeSignalStreamState box installed
    // by native_signal_stream and is not released until native_signal_stream_release.
    let state = unsafe { (*stream).private_data.cast::<NativeSignalStreamState>() };
    if state.is_null() {
        return EINVAL;
    }
    // SAFETY: private_data is non-null and uniquely owned by this stream for
    // the callback; the schema node is borrowed only until materialize_schema returns.
    let state = unsafe { &*state };
    let Ok(schema) = state.schema.materialize_schema() else {
        return ENOMEM;
    };
    // SAFETY: `schema` is an independent production-built Arrow schema shell
    // whose ownership transfers to the importer through `out`.
    unsafe {
        ptr::write(out, *schema.into_box());
    }
    0
}

unsafe extern "C" fn native_signal_stream_get_next(
    stream: *mut ArrowArrayStream,
    out: *mut ArrowArray,
) -> c_int {
    if stream.is_null() || out.is_null() {
        return EINVAL;
    }
    // SAFETY: the importer calls this serially while the stream release slot is
    // live; private_data is the NativeSignalStreamState box installed by
    // native_signal_stream and is not released until native_signal_stream_release.
    let state = unsafe { (*stream).private_data.cast::<NativeSignalStreamState>() };
    if state.is_null() {
        return EINVAL;
    }
    // SAFETY: private_data is non-null and uniquely owned by this serial
    // callback sequence until release; the mutable borrow does not outlive
    // this get_next invocation.
    let state = unsafe { &mut *state };
    let index = state.next;
    state.next = state.next.wrapping_add(1);
    if index == state.interrupt_at {
        // SAFETY: this is CPython's public pending-interrupt API, called while
        // the importer owns an attached Python context.
        unsafe {
            pyo3::ffi::PyErr_SetInterrupt();
        }
    }
    if let Some(batch) = state.batches.get_mut(index).and_then(Option::take) {
        // SAFETY: the batch shell is live and its ownership transfers to the
        // caller's writable ArrowArray output.
        unsafe {
            ptr::write(out, *batch);
        }
    } else {
        // Arrow C Stream EOF is a successful callback with release == NULL.
        // SAFETY: `out` is the writable shell supplied by the importer.
        unsafe {
            ptr::write(out, empty_array());
        }
    }
    0
}

unsafe extern "C" fn native_signal_stream_last_error(
    _stream: *mut ArrowArrayStream,
) -> *const c_char {
    c"".as_ptr()
}

unsafe extern "C" fn native_signal_stream_release(stream: *mut ArrowArrayStream) {
    if stream.is_null() {
        return;
    }
    // SAFETY: release is called once by the moved stream shell for this test-owned
    // stream, and the private box is the allocation installed below.
    unsafe {
        let state = (*stream).private_data.cast::<NativeSignalStreamState>();
        (*stream).release = None;
        (*stream).private_data = ptr::null_mut();
        if !state.is_null() {
            drop(Box::from_raw(state));
        }
    }
}

fn native_signal_stream(
    geometry: &PyGeometry,
    batch_count: usize,
    interrupt_at: usize,
) -> (Box<ArrowArrayStream>, Arc<AtomicUsize>, Arc<AtomicBool>) {
    let exports = std::iter::repeat_with(|| exported(geometry))
        .take(batch_count)
        .collect::<Vec<_>>();
    let schema = exports
        .first()
        .expect("signal stream needs one schema")
        .schema_node
        .clone();
    let batches = exports
        .into_iter()
        .map(|export| {
            let ExportedArray {
                mut schema, array, ..
            } = export;
            // SAFETY: this standalone schema is no longer exposed; the stream
            // owns the cloned SchemaNode used by get_schema instead.
            unsafe {
                release_schema(schema.as_mut());
            }
            let mut array = array.into_box();
            array.release = Some(counted_array_release);
            Some(array)
        })
        .collect();
    let remaining_at_release = Arc::new(AtomicUsize::new(0));
    let released = Arc::new(AtomicBool::new(false));
    let state = Box::new(NativeSignalStreamState {
        schema,
        batches,
        next: 0,
        interrupt_at,
        remaining_at_release: Arc::clone(&remaining_at_release),
        released: Arc::clone(&released),
    });
    (
        Box::new(ArrowArrayStream {
            get_schema: Some(native_signal_stream_get_schema),
            get_next: Some(native_signal_stream_get_next),
            get_last_error: Some(native_signal_stream_last_error),
            release: Some(native_signal_stream_release),
            private_data: Box::into_raw(state).cast(),
        }),
        remaining_at_release,
        released,
    )
}

#[test]
fn release_callbacks_are_idempotent_for_native_exports() {
    let point = PyGeometry::with_frame(
        Shape::Point(Point::new_raw_xy(1.0, 2.0)),
        crate::boundary::Frame::None,
    );
    release_twice(exported(&point));

    let line = PyGeometry::with_frame(
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(1.0, 1.0),
                ])
                .unwrap(),
            )
            .expect("test line is valid"),
        ),
        crate::boundary::Frame::None,
    );
    release_twice(exported(&line));

    let ring = Ring::closed(vec![
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(1.0, 0.0),
        Point::new_raw_xy(1.0, 1.0),
        Point::new_raw_xy(0.0, 0.0),
    ])
    .unwrap();
    let polygon = PyGeometry::with_frame(
        Shape::Polygon(Polygon::new(ring, Vec::new())),
        crate::boundary::Frame::None,
    );
    release_twice(exported(&polygon));
}

#[test]
fn schema_only_export_releases_without_array_export() {
    let point = PyGeometry::with_frame(
        Shape::Point(Point::new_raw_xy(1.0, 2.0)),
        crate::boundary::Frame::None,
    );
    reset_schema_release_callbacks();
    release_schema_twice(exported_schema(&point));
    assert_eq!(schema_release_callbacks(), 3);

    let collection = PyGeometry::with_frame(
        Shape::typed_empty(
            crate::geometry::GeometryKind::GeometryCollection,
            crate::geometry::CoordinateAxes::XY,
        ),
        crate::boundary::Frame::None,
    );
    reset_schema_release_callbacks();
    release_schema_twice(exported_schema(&collection));
    assert_eq!(schema_release_callbacks(), 1);
}

#[test]
fn schema_only_uses_borrowed_description_without_payload_work() {
    let _test_lock = NATIVE_SCHEMA_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    crate::test_support::initialize_python();
    Python::attach(|py| -> PyResult<()> {
        install_embedded_gometry_module(py)?;
        let present = PyGeometryArray::from_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(1.0, 2.0)),
                Shape::Point(Point::new_raw_xy(3.0, 4.0)),
            ],
            crate::boundary::Frame::None,
        )?;
        let mask = crate::array::ValidityMask::from_vec(3, vec![false, true, false])?
            .expect("one missing row");
        let nullable = PyGeometryArray::scatter_present_rows(&present, mask)?;
        let gathered = nullable.gather_logical_rows(&[2, 1, 0])?;
        let line = PyGeometryArray::from_shapes(
            vec![crate::io::parse_wkt("LINESTRING ZM (0 0 1 2, 1 1 3 4)")?],
            crate::boundary::Frame::None,
        )?;
        let polygon = PyGeometryArray::from_shapes(
            vec![crate::io::parse_wkt("POLYGON ((0 0, 1 0, 0 1, 0 0))")?],
            crate::boundary::Frame::None,
        )?;
        let mixed_wkb = PyGeometryArray::from_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(5.0, 6.0)),
                crate::io::parse_wkt("POINT Z (7 8 9)")?,
            ],
            crate::boundary::Frame::None,
        )?;
        let empty_wkb = PyGeometryArray::from_shapes(
            vec![crate::io::parse_wkt("LINESTRING ZM EMPTY")?],
            crate::boundary::Frame::None,
        )?;
        let framed = PyGeometryArray::from_shapes(
            vec![Shape::Point(Point::new_raw_xy(9.0, 10.0))],
            crate::boundary::Frame::new(Some(crate::boundary::crs_arc_static("EPSG:4326")), None)?,
        )?;

        for array in [&gathered, &line, &polygon, &mixed_wkb, &empty_wkb, &framed] {
            reset_payload_events();
            reset_schema_release_callbacks();
            let schema_only = schema_only_snapshot(py, array)?;
            assert_eq!(payload_events(), (0, 0));
            assert_eq!(
                schema_release_callbacks(),
                schema_snapshot_node_count(&schema_only),
                "schema-only capsule releases its schema tree once"
            );

            reset_payload_events();
            reset_schema_release_callbacks();
            let full = exported_array(array);
            // SAFETY: the export owns this schema until `full` is dropped.
            let full_schema = unsafe { schema_snapshot(full.schema.as_ref()) };
            let events = payload_events();
            assert!(events.0 > 0, "full export must plan payloads");
            assert!(events.1 > 0, "full export must materialize payloads");
            assert_eq!(schema_only, full_schema);
            drop(full);
            assert_eq!(
                schema_release_callbacks(),
                schema_snapshot_node_count(&full_schema),
                "full export releases its schema tree once"
            );
        }
        Ok(())
    })
    .expect("schema-only payload negative control");
}

fn schema_snapshot_node_count(snapshot: &SchemaSnapshot) -> usize {
    1 + snapshot
        .children
        .iter()
        .map(schema_snapshot_node_count)
        .sum::<usize>()
}

#[test]
fn stream_export_release_is_idempotent_after_batch_move() {
    let point = PyGeometry::with_frame(
        Shape::Point(Point::new_raw_xy(1.0, 2.0)),
        crate::boundary::Frame::None,
    );
    let mut stream = stream_from_export(exported(&point));
    let mut array = empty_array();

    // SAFETY: this test owns the stream and moves its one batch into a
    // stack ArrowArray, then releases both sides through their callbacks.
    unsafe {
        assert_eq!(stream_get_next(&raw mut stream, &raw mut array), 0);
        assert!(array.release.is_some());
        release_array(&raw mut array);
        release_stream(&raw mut stream);
        release_stream(&raw mut stream);
    }
}

#[test]
fn stream_import_checks_native_pending_interrupt_before_exhaustion() {
    let _test_lock = NATIVE_SIGNAL_STREAM_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    NATIVE_SIGNAL_ARRAY_RELEASES.store(0, Ordering::Relaxed);
    crate::test_support::initialize_python();
    Python::attach(|py| -> PyResult<()> {
        let _signal_guard = crate::test_support::PythonSignalTestGuard::new(py);
        let point = PyGeometry::with_frame(
            Shape::Point(Point::new_raw_xy(1.0, 2.0)),
            crate::boundary::Frame::None,
        );
        let batch_count = 65;
        let (mut stream, remaining_at_release, released) =
            native_signal_stream(&point, batch_count, 63);
        let stream_ptr = std::ptr::from_mut::<ArrowArrayStream>(stream.as_mut());
        // SAFETY: `stream` is a live test stream owned by this frame.
        let result = unsafe { super::import::import_stream(py, stream_ptr, None, None) };
        let late_signal = py.check_signals().err();
        let Err(error) = result else {
            panic!(
                "stream import completed; {} batches remain at release; late signal={}",
                remaining_at_release.load(Ordering::Acquire),
                late_signal.is_some()
            );
        };

        assert!(error.is_instance_of::<PyKeyboardInterrupt>(py));
        assert!(
            late_signal.is_none(),
            "signal was observed after stream exhaustion"
        );
        assert!(
            remaining_at_release.load(Ordering::Acquire) > 0,
            "finite stream was exhausted before interruption"
        );
        assert!(released.load(Ordering::Acquire));
        assert_eq!(
            NATIVE_SIGNAL_ARRAY_RELEASES.load(Ordering::Relaxed),
            batch_count,
            "every producer batch shell must be released"
        );
        Ok(())
    })
    .expect("native Arrow stream signal test");
}

// Compile-fail fixtures for arrow_c / frozen-i64 are driven by the crate-root
// `compile_fail_gate` (stable error codes + production dep-info).
