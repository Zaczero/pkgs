use std::ffi::{c_char, c_int};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pyo3::exceptions::PyKeyboardInterrupt;

use crate::PyGeometry;
use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Ring, Shape};
use crate::py::arrow_c::capsule_lifecycle::stream_from_export;
use crate::py::arrow_c::*;

static NATIVE_SIGNAL_STREAM_TEST_LOCK: Mutex<()> = Mutex::new(());
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

fn exported_schema(geometry: &PyGeometry) -> Box<ArrowSchema> {
    let shapes = vec![geometry.shape.shape().clone()];
    let plan = plan_shapes(
        &shapes,
        geometry.crs_str().map(str::to_owned),
        geometry.epoch(),
    )
    .expect("test schema plan");
    materialize_schema_plan(prepare_export_plan(plan).expect("test schema")).expect("test schema")
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
            if let Some(mut batch) = batch.take() {
                // SAFETY: each retained shell was produced by the production
                // Arrow builder and remains uniquely owned by this state.
                if let Some(release) = batch.release {
                    unsafe { release(batch.as_mut()) }
                }
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
        return -1;
    }
    // SAFETY: the importer calls this with the live test stream and a writable
    // output shell. The state owns the schema node for the whole stream.
    let state = unsafe { (*stream).private_data.cast::<NativeSignalStreamState>() };
    if state.is_null() {
        return -1;
    }
    let Ok(schema) = (unsafe { &*state }).schema.clone().into_schema() else {
        return -1;
    };
    // SAFETY: `schema` is an independent production-built Arrow schema shell
    // whose ownership transfers to the importer through `out`.
    unsafe { ptr::write(out, *schema) };
    0
}

unsafe extern "C" fn native_signal_stream_get_next(
    stream: *mut ArrowArrayStream,
    out: *mut ArrowArray,
) -> c_int {
    if stream.is_null() || out.is_null() {
        return -1;
    }
    // SAFETY: the importer calls this serially while the stream release slot is
    // live; the state is uniquely owned by the stream callback sequence.
    let state = unsafe { (*stream).private_data.cast::<NativeSignalStreamState>() };
    if state.is_null() {
        return -1;
    }
    let state = unsafe { &mut *state };
    let index = state.next;
    state.next = state.next.wrapping_add(1);
    if index == state.interrupt_at {
        // SAFETY: this is CPython's public pending-interrupt API, called while
        // the importer owns an attached Python context.
        unsafe { pyo3::ffi::PyErr_SetInterrupt() };
    }
    if let Some(batch) = state.batches.get_mut(index).and_then(Option::take) {
        // SAFETY: the batch shell is live and its ownership transfers to the
        // caller's writable ArrowArray output.
        unsafe { ptr::write(out, *batch) };
    } else {
        // Arrow C Stream EOF is a successful callback with release == NULL.
        // SAFETY: `out` is the writable shell supplied by the importer.
        unsafe { ptr::write(out, empty_array()) };
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
    // SAFETY: release is called once by ImportedStreamGuard for this test-owned
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
    let mut exports = (0..batch_count)
        .map(|_| exported(geometry))
        .collect::<Vec<_>>();
    let schema = exports
        .first()
        .expect("signal stream needs one schema")
        .schema_node
        .clone();
    let batches = exports
        .drain(..)
        .map(|export| {
            let ExportedArray {
                mut schema, array, ..
            } = export;
            // SAFETY: this standalone schema is no longer exposed; the stream
            // owns the cloned SchemaNode used by get_schema instead.
            unsafe { release_schema(schema.as_mut()) };
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
        remaining_at_release: remaining_at_release.clone(),
        released: released.clone(),
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
    let point = PyGeometry::from_shape_crs(Shape::Point(Point::new_raw_xy(1.0, 2.0)), None);
    release_twice(exported(&point));

    let line = PyGeometry::from_shape_crs(
        Shape::LineString(
            LineSeq::try_new(CoordSeq::from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(1.0, 1.0),
            ]))
            .expect("test line is valid"),
        ),
        None,
    );
    release_twice(exported(&line));

    let ring = Ring::closed(vec![
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(1.0, 0.0),
        Point::new_raw_xy(1.0, 1.0),
        Point::new_raw_xy(0.0, 0.0),
    ])
    .unwrap();
    let polygon = PyGeometry::from_shape_crs(Shape::Polygon(Polygon::new(ring, Vec::new())), None);
    release_twice(exported(&polygon));
}

#[test]
fn schema_only_export_releases_without_array_export() {
    let point = PyGeometry::from_shape_crs(Shape::Point(Point::new_raw_xy(1.0, 2.0)), None);
    release_schema_twice(exported_schema(&point));

    let collection = PyGeometry::from_shape_crs(
        Shape::typed_empty(
            crate::geometry::GeometryKind::GeometryCollection,
            crate::geometry::CoordinateAxes::XY,
        ),
        None,
    );
    release_schema_twice(exported_schema(&collection));
}

#[test]
fn stream_export_release_is_idempotent_after_batch_move() {
    let point = PyGeometry::from_shape_crs(Shape::Point(Point::new_raw_xy(1.0, 2.0)), None);
    let mut stream = stream_from_export(exported(&point));
    let mut array = empty_array();

    // SAFETY: this test owns the stream and moves its one batch into a
    // stack ArrowArray, then releases both sides through their callbacks.
    unsafe {
        assert_eq!(stream_get_next(stream.as_mut(), &raw mut array), 0);
        assert!(array.release.is_some());
        release_array(&raw mut array);
        release_stream(stream.as_mut());
        release_stream(stream.as_mut());
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
        let point = PyGeometry::from_shape_crs(Shape::Point(Point::new_raw_xy(1.0, 2.0)), None);
        let batch_count = 65;
        let (mut stream, remaining_at_release, released) =
            native_signal_stream(&point, batch_count, 63);
        let stream_ptr = stream.as_mut() as *mut ArrowArrayStream;
        let result = super::import::import_stream(py, stream_ptr, None, None);
        let late_signal = py.check_signals().err();
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!(
                "stream import completed; {} batches remain at release; late signal={}",
                remaining_at_release.load(Ordering::Acquire),
                late_signal.is_some()
            ),
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
