use std::sync::Arc;

use super::admission::{
    AdmittedGraph, AdmittedLayout, admit_candidate, admit_known_graph, admit_logical_layout,
};
use super::binding::bind_literals;
use super::contacts::{edges_have_only_vertex_contacts, point_edge_contacts_preserved};
use super::layout::LogicalArealWalk;
use super::literals::{
    LiteralAddress, LiteralPoint, LiteralPolygon, LiteralRowLayout, LiteralSequence,
    ShapeLiteralLayout,
};
use super::points::ExactPoints;
use super::storage::{CheckedImages, FrozenLiteralBuffers, LiteralChart, LiteralCheck};
use crate::error::{ErrorKind, Result};
use crate::geometry::embedding_search::account::{EvaluationBudget, EvaluationOnly};
use crate::geometry::segment_index::sweep::{
    CrossPairProbeProgress, cross_pair_probe_progress, reset_cross_pair_probe_progress,
};
use crate::geometry::storage::{
    GeometryArrayStorage, PackedStorage, RowSelection, StorageLayout, ValidityMask,
};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::topology::ExactChartPoint;
use crate::geometry::{
    CoordSeq, CoordinateAxes, GeometryErrorKind, LineSeq, MOrdinate, Point, Polygon, Ring, Shape,
    Unmetered, Work, XY, ZOrdinate,
};

#[derive(Clone, Copy)]
struct SweepCharge {
    spent_before: u128,
    ticks: u128,
    progress: CrossPairProbeProgress,
    contact_progress: super::contacts::ContactCheckProgress,
}

struct SweepRecordingWork<'a> {
    budget: &'a mut EvaluationBudget,
    spent: u128,
    charges: Vec<SweepCharge>,
    refused: Option<SweepCharge>,
}

impl<'a> SweepRecordingWork<'a> {
    fn new(budget: &'a mut EvaluationBudget) -> Self {
        Self {
            budget,
            spent: 0,
            charges: Vec::new(),
            refused: None,
        }
    }

    fn record(&mut self, ticks: u128, result: &Result<()>) {
        let event = SweepCharge {
            spent_before: self.spent,
            ticks,
            progress: cross_pair_probe_progress(),
            contact_progress: super::contacts::contact_check_progress(),
        };
        if result.is_ok() {
            self.charges.push(event);
            self.spent = self.spent.wrapping_add(ticks);
        } else {
            self.refused = Some(event);
        }
    }
}

impl Work for SweepRecordingWork<'_> {
    type Error = crate::error::Error;

    fn charge(&mut self, ticks: u64) -> Result<()> {
        let result = <EvaluationBudget as Work>::charge(self.budget, ticks);
        self.record(u128::from(ticks), &result);
        result
    }

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
        let ticks = (count as u128).wrapping_mul(ticks_per_item as u128);
        let result = <EvaluationBudget as Work>::charge_product(self.budget, count, ticks_per_item);
        self.record(ticks, &result);
        result
    }
}

fn budget_refusing_before(operation: &'static str, event: &SweepCharge) -> EvaluationBudget {
    let remaining = event
        .spent_before
        .checked_add(event.ticks)
        .and_then(|ticks| ticks.checked_sub(1))
        .expect("the target sweep charge is positive and fits the budget");
    EvaluationBudget::with_remaining_ticks_for_test(operation, 1, remaining)
        .expect("calibrated sweep budget is representable")
}

fn assert_sweep_limit(error: &crate::error::Error, operation: &'static str) {
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: actual,
            ..
        }) if *actual == operation
    ));
}

fn isolated_contact_result<W: Work>(work: &mut W) -> Result<bool> {
    let points = points(&[
        (0.0, 0.0),
        (10.0, 10.0),
        (100.0, 100.0),
        (101.0, 100.0),
        (200.0, 200.0),
        (201.0, 200.0),
    ]);
    point_edge_contacts_preserved(&points, &points, &[0, 1], &[[2, 3], [4, 5]], work)
}

fn edge_contact_result<W: Work>(
    coordinates: &[(f64, f64)],
    edges: &[[u32; 2]],
    work: &mut W,
) -> Result<bool> {
    edges_have_only_vertex_contacts(&points(coordinates), edges, work)
}

fn points(values: &[(f64, f64)]) -> ExactPoints {
    let mut work = Unmetered;
    ExactPoints::from_exact(
        values
            .iter()
            .map(|&(x, y)| {
                ExactChartPoint::from_rationals(
                    ExactRational::from_f64(x),
                    ExactRational::from_f64(y),
                )
            })
            .collect(),
        &mut work,
    )
    .unwrap()
}

fn exact_points(values: &[(ExactRational, ExactRational)]) -> ExactPoints {
    let mut work = Unmetered;
    ExactPoints::from_exact(
        values
            .iter()
            .map(|(x, y)| ExactChartPoint::from_rationals(x.clone(), y.clone()))
            .collect(),
        &mut work,
    )
    .unwrap()
}

fn near_one_exact_chart_point() -> ExactChartPoint {
    let x = ExactDyadic::from_f64(1.0)
        .try_add_with_work(ExactDyadic::from_f64(2.0_f64.powi(-256)), &mut Unmetered)
        .expect("the test rational is representable");
    ExactChartPoint::from_rationals(ExactRational::from_dyadic(x), ExactRational::from_f64(0.0))
}

struct ImageRows(Vec<ExactChartPoint>);

impl CheckedImages<u32> for ImageRows {
    fn image(
        &self,
        _address: super::literals::LiteralAddress,
        image: u32,
    ) -> Result<&ExactChartPoint> {
        self.0.get(image as usize).ok_or_else(|| {
            crate::geometry::GeometryErrorKind::Invalid(
                "test literal image is outside its receipt".into(),
            )
            .into()
        })
    }
}

struct IdentityLiteralChart;

impl LiteralChart<u32> for IdentityLiteralChart {
    fn read_point<W: crate::geometry::Work>(
        &mut self,
        _address: super::literals::LiteralAddress,
        _image: u32,
        record: Point,
        _expected_axes: CoordinateAxes,
        _stored_axes: CoordinateAxes,
        _work: &mut W,
    ) -> Result<Option<ExactChartPoint>> {
        Ok(Some(ExactChartPoint::from_rationals(
            ExactRational::from_f64(record.x),
            ExactRational::from_f64(record.y),
        )))
    }
}

struct TestLiteralReceiver {
    images: ImageRows,
    chart: IdentityLiteralChart,
}

impl CheckedImages<u32> for TestLiteralReceiver {
    fn image(
        &self,
        address: super::literals::LiteralAddress,
        image: u32,
    ) -> Result<&ExactChartPoint> {
        self.images.image(address, image)
    }
}

struct EvaluationRecorder {
    budget: EvaluationBudget,
    successful_charges: usize,
    refusals: usize,
    charges_after_refusal: usize,
    refused: bool,
}

impl EvaluationRecorder {
    fn record(&mut self, result: Result<()>) -> Result<()> {
        match result {
            Ok(()) => {
                self.successful_charges = self
                    .successful_charges
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                Ok(())
            },
            Err(error) => {
                self.refusals = self
                    .refusals
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                self.refused = true;
                Err(error)
            },
        }
    }
}

impl Work for EvaluationRecorder {
    type Error = crate::error::Error;

    fn charge(&mut self, ticks: u64) -> Result<()> {
        if self.refused {
            self.charges_after_refusal = self
                .charges_after_refusal
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        let result = self.budget.charge(ticks);
        self.record(result)
    }

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
        if self.refused {
            self.charges_after_refusal = self
                .charges_after_refusal
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        let result = self.budget.charge_product(count, ticks_per_item);
        self.record(result)
    }
}

impl LiteralChart<u32> for TestLiteralReceiver {
    fn read_point<W: crate::geometry::Work>(
        &mut self,
        address: super::literals::LiteralAddress,
        image: u32,
        record: Point,
        expected_axes: CoordinateAxes,
        stored_axes: CoordinateAxes,
        work: &mut W,
    ) -> Result<Option<ExactChartPoint>> {
        self.chart
            .read_point(address, image, record, expected_axes, stored_axes, work)
    }
}

struct FullRecordReceiver {
    images: ImageRows,
    expected: Vec<(LiteralAddress, Point)>,
}

impl CheckedImages<u32> for FullRecordReceiver {
    fn image(
        &self,
        address: super::literals::LiteralAddress,
        image: u32,
    ) -> Result<&ExactChartPoint> {
        self.images.image(address, image)
    }
}

impl LiteralChart<u32> for FullRecordReceiver {
    fn read_point<W: crate::geometry::Work>(
        &mut self,
        address: LiteralAddress,
        image: u32,
        record: Point,
        expected_axes: CoordinateAxes,
        stored_axes: CoordinateAxes,
        _work: &mut W,
    ) -> Result<Option<ExactChartPoint>> {
        let Some((expected_address, expected_record)) = self.expected.get(image as usize) else {
            return Ok(None);
        };
        if address != *expected_address
            || record != *expected_record
            || expected_axes != stored_axes
        {
            return Ok(None);
        }
        Ok(Some(ExactChartPoint::from_rationals(
            ExactRational::from_f64(record.x),
            ExactRational::from_f64(record.y),
        )))
    }
}

fn xyzm_point(x: f64, y: f64, z: f64, m: f64) -> Point {
    Point::new_raw_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m)))
}

fn point_literal_row(
    image: u32,
    operation: u32,
    logical_row: usize,
    group_row: Option<usize>,
    value_row: Option<usize>,
) -> LiteralRowLayout<u32> {
    LiteralRowLayout {
        operation,
        logical_row,
        group_row,
        value_row,
        layout: ShapeLiteralLayout {
            points: vec![LiteralPoint {
                output: 0,
                axes: CoordinateAxes::XYZM,
                image,
            }],
            lines: Vec::new(),
            polygons: Vec::new(),
        },
    }
}

fn point_rows(points: &[Point]) -> Arc<GeometryArrayStorage> {
    Arc::new(GeometryArrayStorage::Packed(PackedStorage::Points {
        coords: Arc::new(CoordSeq::try_from_points(points).expect("pack XYZM points")),
        row_map: RowSelection::Identity,
    }))
}

fn line_rows(points: &[Point]) -> Arc<GeometryArrayStorage> {
    Arc::new(GeometryArrayStorage::Packed(PackedStorage::Lines {
        coords: Arc::new(CoordSeq::try_from_points(points).expect("pack XYZM line")),
        offsets: crate::geometry::CsrOffsetColumn::try_new(vec![0, points.len()], points.len())
            .expect("valid line offsets"),
        row_map: RowSelection::Identity,
    }))
}

fn polygon_rows(points: &[Point]) -> Arc<GeometryArrayStorage> {
    Arc::new(GeometryArrayStorage::Packed(PackedStorage::Polygons {
        coords: Arc::new(CoordSeq::try_from_points(points).expect("pack XYZM ring")),
        ring_offsets: crate::geometry::CsrOffsetColumn::<crate::geometry::RingLevel>::try_new(
            vec![0, points.len()],
            points.len(),
        )
        .expect("valid ring offsets"),
        polygon_offsets:
            crate::geometry::CsrOffsetColumn::<crate::geometry::PolygonLevel>::try_new(
                vec![0, 1],
                1,
            )
            .expect("valid polygon offsets"),
        row_map: RowSelection::Identity,
    }))
}

fn polygon_layout(graph: &AdmittedGraph, vertices: &[u32]) -> AdmittedLayout {
    admit_logical_layout(
        graph,
        vec![LogicalArealWalk {
            output: 0,
            walk: 0,
            vertices: vertices.to_vec(),
        }],
        None,
        &mut Unmetered,
    )
    .unwrap()
}
