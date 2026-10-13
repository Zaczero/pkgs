use pyo3::prelude::*;
use pyo3::types::PyList;

use super::{
    CLEARANCE_BVH_FACETS, CLEARANCE_BVH_NODES, CLEARANCE_BVH_SEGMENTS,
    CLEARANCE_STRIP_BUILD_VERTICES, CLEARANCE_STRIP_TESTS, NEAREST_WORK,
};
use crate::geometry::CLEARANCE_STRIP_SUPPORT_FALLBACKS;

#[test]
fn public_clearance_bounds_radial_candidate_work() {
    crate::test_support::initialize_python();
    Python::attach(|py| {
        let module = PyModule::new(py, "gometry._lib").unwrap();
        crate::lib(&module).unwrap();
        let mut previous = None;
        let mut previous_count = None;
        for count in [128_usize, 512, 1024, 4096] {
            let coords = PyList::empty(py);
            for index in 0..count {
                let radius = 0.5 + (index.wrapping_mul(17) % 31) as f64 / 31.0;
                let angle = std::f64::consts::TAU * index as f64 / count as f64;
                coords
                    .append((radius * angle.cos(), radius * angle.sin()))
                    .unwrap();
            }
            let polygon = module
                .getattr("Polygon")
                .unwrap()
                .call1((&coords,))
                .unwrap();
            for operation in ["minimum_clearance", "minimum_clearance_line"] {
                NEAREST_WORK.with(|work| work.set(0));
                CLEARANCE_BVH_NODES.with(|work| work.set(0));
                CLEARANCE_BVH_SEGMENTS.with(|work| work.set(0));
                CLEARANCE_BVH_FACETS.with(|work| work.set(0));
                CLEARANCE_STRIP_TESTS.with(|work| work.set(0));
                CLEARANCE_STRIP_BUILD_VERTICES.with(|work| work.set(0));
                CLEARANCE_STRIP_SUPPORT_FALLBACKS.with(|work| work.set(0));
                let result = polygon.call_method0(operation).unwrap();
                if operation == "minimum_clearance" {
                    assert!(result.extract::<f64>().unwrap() > 0.0);
                } else {
                    assert!(
                        !result
                            .getattr("is_empty")
                            .unwrap()
                            .extract::<bool>()
                            .unwrap()
                    );
                }
                let work = NEAREST_WORK.with(std::cell::Cell::get);
                let nodes = CLEARANCE_BVH_NODES.with(std::cell::Cell::get);
                let segments = CLEARANCE_BVH_SEGMENTS.with(std::cell::Cell::get);
                let facets = CLEARANCE_BVH_FACETS.with(std::cell::Cell::get);
                let strips = CLEARANCE_STRIP_TESTS.with(std::cell::Cell::get);
                let build_vertices = CLEARANCE_STRIP_BUILD_VERTICES.with(std::cell::Cell::get);
                let support_fallbacks =
                    CLEARANCE_STRIP_SUPPORT_FALLBACKS.with(std::cell::Cell::get);
                assert_eq!(work, nodes.saturating_add(segments));
                assert!(facets > 0, "clearance BVH did not reach a facet");
                assert!(strips > 0, "clearance BVH did not test a source strip");
                assert!(build_vertices > 0);
                eprintln!(
                    "{operation}: {count} vertices, {nodes} BVH nodes, {facets} facets, {segments} segments, {strips} strip tests, {build_vertices} support-vertex visits, {support_fallbacks} support exact fallbacks",
                );
                assert!(
                    work > count,
                    "the public call did not reach nearest queries"
                );
                if let Some(previous) = previous
                    && ((previous_count == Some(128) && count == 512)
                        || (previous_count == Some(1024) && count == 4096))
                {
                    assert!(
                        work < previous * 6,
                        "{count} radial vertices took {work} visits versus {previous} at the previous fourfold checkpoint",
                    );
                }
                if operation == "minimum_clearance_line" {
                    previous = Some(work);
                    previous_count = Some(count);
                }
            }
        }
    });
}

#[test]
fn public_clearance_bit_reversal_work_scales_at_extreme_magnitudes() {
    crate::test_support::initialize_python();
    Python::attach(|py| {
        let module = PyModule::new(py, "gometry._lib").unwrap();
        crate::lib(&module).unwrap();
        for scale in [1.0, 2.0_f64.powi(-700), 2.0_f64.powi(600)] {
            let mut previous = None;
            for count in [64_usize, 256] {
                let lines = PyList::empty(py);
                let bits = count.trailing_zeros();
                for index in 0..count {
                    let source_index = index.reverse_bits() >> (usize::BITS - bits);
                    let start = (10 * source_index) as f64 * scale;
                    let end = (10 * source_index + 1) as f64 * scale;
                    lines
                        .append(
                            PyList::new(py, [(start, 0.0), (end, 0.0)])
                                .expect("two-point line coordinates are accepted"),
                        )
                        .unwrap();
                }
                let shape = module
                    .getattr("MultiLineString")
                    .unwrap()
                    .call1((&lines,))
                    .unwrap();

                CLEARANCE_BVH_NODES.with(|work| work.set(0));
                CLEARANCE_BVH_SEGMENTS.with(|work| work.set(0));
                let answer = shape
                    .call_method0("minimum_clearance")
                    .unwrap()
                    .extract::<f64>()
                    .unwrap();
                assert_eq!(answer, scale);
                let nodes = CLEARANCE_BVH_NODES.with(std::cell::Cell::get);
                let segments = CLEARANCE_BVH_SEGMENTS.with(std::cell::Cell::get);
                let work = nodes.saturating_add(segments);
                eprintln!(
                    "bit-reversal clearance: scale={scale:e}, {count} segments, answer={answer:e}, {nodes} BVH nodes, {segments} segment visits, {work} total",
                );

                if let Some(previous) = previous {
                    assert!(
                        work < previous * 7,
                        "fourfold bit-reversal input at scale {scale:e} took {work} events versus {previous}",
                    );
                }
                previous = Some(work);
            }
        }
    });
}

#[test]
fn public_split_bounds_point_projection_work() {
    crate::test_support::initialize_python();
    Python::attach(|py| {
        let module = PyModule::new(py, "gometry._lib").unwrap();
        crate::lib(&module).unwrap();
        let mut previous = None;
        for count in [128_usize, 512] {
            let coords = PyList::new(
                py,
                (0..=count).map(|index| (index as f64, (index % 2) as f64)),
            )
            .unwrap();
            let points =
                PyList::new(py, (0..count).map(|index| (index as f64 + 0.5, 0.5))).unwrap();
            let line = module
                .getattr("LineString")
                .unwrap()
                .call1((&coords,))
                .unwrap();
            let points = module
                .getattr("MultiPoint")
                .unwrap()
                .call1((&points,))
                .unwrap();
            NEAREST_WORK.with(|work| work.set(0));
            let pieces = module
                .getattr("split")
                .unwrap()
                .call1((&line, &points))
                .unwrap();
            assert_eq!(pieces.len().unwrap(), count + 1);
            let work = NEAREST_WORK.with(std::cell::Cell::get);
            eprintln!("split: {count} segments/points, {work} candidate visits");
            assert!(
                work > count,
                "the public call did not reach nearest queries"
            );
            if let Some(previous) = previous {
                assert!(
                    work < previous * 6,
                    "{count} split segments/points took {work} visits versus {previous} at one quarter the size",
                );
            }
            previous = Some(work);
        }
    });
}
