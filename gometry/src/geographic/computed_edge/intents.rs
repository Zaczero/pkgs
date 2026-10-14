use super::publish::map_geographic_error;
use super::{
    ChartStep, Coordinates, EdgeDirection, GeographicFrame, GeometryErrorKind, MOrdinate, Point,
    Result, RetainedChartDisplacement, RetainedComputedSample, RetainedLongitudeClass, ZOrdinate,
    interpolate_subdivision_ordinate, retained_computed_samples_connection,
    retained_edge_from_chart_witness, retained_edge_from_lifted_binary64_pair,
    retained_exact_interval_class, retained_exact_publisher_sample,
    retained_exact_split_piece_count, retained_native_interval_class,
    retained_native_publisher_sample, retained_native_split_piece_count,
};
use crate::geographic::PeriodicAxis;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ComputedLongitudeRelation<'a> {
    Native(RetainedChartDisplacement),
    ExactLifted {
        endpoints: [&'a crate::geometry::ExactRational; 2],
    },
}

impl ComputedLongitudeRelation<'_> {
    pub(crate) fn from_chart_connection(
        frame: GeographicFrame,
        endpoints: [f64; 2],
    ) -> Result<Self> {
        let connection = frame
            .longitude()
            .chart_connection(endpoints[0], endpoints[1])
            .map_err(map_geographic_error)?;
        Ok(Self::Native(connection.retained()))
    }

    pub(crate) fn from_chart_witness(
        frame: GeographicFrame,
        endpoints: [f64; 2],
        witness: ChartStep,
    ) -> Result<Self> {
        Ok(Self::Native(retained_edge_from_chart_witness(
            frame.longitude(),
            endpoints,
            witness,
        )?))
    }

    pub(super) fn class(
        self,
        axis: PeriodicAxis,
        from: f64,
        to: f64,
    ) -> Result<RetainedLongitudeClass> {
        match self {
            Self::Native(edge) => retained_native_interval_class(edge, axis, from, to),
            Self::ExactLifted { endpoints } => {
                retained_exact_interval_class(endpoints, axis, from, to)
            },
        }
    }

    pub(super) fn sample(
        self,
        axis: PeriodicAxis,
        parameter: f64,
    ) -> Result<RetainedComputedSample> {
        match self {
            Self::Native(edge) => retained_native_publisher_sample(axis, edge, parameter),
            Self::ExactLifted { endpoints } => {
                retained_exact_publisher_sample(axis, endpoints, parameter)
            },
        }
    }

    pub(super) fn split_piece_count(self, axis: PeriodicAxis) -> Result<usize> {
        match self {
            Self::Native(edge) => retained_native_split_piece_count(edge, axis),
            Self::ExactLifted { endpoints } => retained_exact_split_piece_count(endpoints, axis),
        }
    }

    #[expect(
        clippy::float_cmp,
        reason = "from and to use the exact binary64 endpoint parameters supplied by the publisher"
    )]
    pub(super) fn reads_as(
        self,
        frame: GeographicFrame,
        start: f64,
        end: f64,
        from: f64,
        to: f64,
    ) -> Result<bool> {
        let actual = frame
            .longitude()
            .chart_connection(start, end)
            .map_err(map_geographic_error)?
            .retained();
        if from == 0.0
            && to == 1.0
            && let Self::Native(expected) = self
        {
            return Ok(expected == actual);
        }
        let start_sample = self.sample(frame.longitude(), from)?;
        let end_sample = self.sample(frame.longitude(), to)?;
        let intended =
            retained_computed_samples_connection(frame.longitude(), [&start_sample, &end_sample])?;
        Ok(intended == actual)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CertifiedMinorEdge([u64; 2]);

impl CertifiedMinorEdge {
    fn try_new(frame: GeographicFrame, start: Point, end: Point) -> Result<Self> {
        if !certified_minor_relation(frame, start, end)? {
            return Err(GeometryErrorKind::Invalid(
                "certified-minor witness requires a strictly minor longitude edge".into(),
            )
            .into());
        }
        Ok(Self([start.x.to_bits(), end.x.to_bits()]))
    }

    pub(super) fn matches(self, start: Point, end: Point) -> bool {
        self.0 == [start.x.to_bits(), end.x.to_bits()]
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ComputedEdgeIntent<'a> {
    /// A source-provenance edge whose written coordinates must be replayed.
    Preserved { zero_longitude: bool },
    /// An exact producer relation retained through canonical publication.
    Retained(ComputedLongitudeRelation<'a>),
    /// A local projected segment certified to sweep less than half a period.
    CertifiedMinor(CertifiedMinorEdge),
}

pub(super) fn certified_minor_relation(
    frame: GeographicFrame,
    start: Point,
    end: Point,
) -> Result<bool> {
    Ok(matches!(
        frame
            .longitude()
            .chart_step(start.x, end.x)
            .map_err(map_geographic_error)?,
        ChartStep::Shortest(EdgeDirection::Minor(delta))
            if delta.abs() < frame.longitude().half_period()
    ))
}

impl<'a> ComputedEdgeIntent<'a> {
    pub(crate) const fn preserved(zero_longitude: bool) -> Self {
        Self::Preserved { zero_longitude }
    }

    pub(crate) const fn retained_native(edge: RetainedChartDisplacement) -> Self {
        Self::Retained(ComputedLongitudeRelation::Native(edge))
    }

    pub(crate) const fn exact_lifted(endpoints: [&'a crate::geometry::ExactRational; 2]) -> Self {
        Self::Retained(ComputedLongitudeRelation::ExactLifted { endpoints })
    }

    pub(crate) fn retained_binary64_pair(axis: PeriodicAxis, endpoints: [f64; 2]) -> Result<Self> {
        Ok(Self::retained_native(
            retained_edge_from_lifted_binary64_pair(axis, endpoints)?,
        ))
    }

    pub(crate) fn retained_chart_connection(
        frame: GeographicFrame,
        endpoints: [f64; 2],
    ) -> Result<Self> {
        Ok(Self::Retained(
            ComputedLongitudeRelation::from_chart_connection(frame, endpoints)?,
        ))
    }

    pub(crate) fn retained_chart_witness(
        frame: GeographicFrame,
        endpoints: [f64; 2],
        witness: ChartStep,
    ) -> Result<Self> {
        Ok(Self::Retained(
            ComputedLongitudeRelation::from_chart_witness(frame, endpoints, witness)?,
        ))
    }

    pub(crate) fn certified_minor(
        frame: GeographicFrame,
        start: Point,
        end: Point,
    ) -> Result<Self> {
        Ok(Self::CertifiedMinor(CertifiedMinorEdge::try_new(
            frame, start, end,
        )?))
    }

    pub(super) fn certifies_zero_longitude_change(self, axis: PeriodicAxis) -> Result<bool> {
        match self {
            Self::Preserved { zero_longitude } => Ok(zero_longitude),
            Self::Retained(relation) => {
                Ok(relation.class(axis, 0.0, 1.0)? == RetainedLongitudeClass::Zero)
            },
            // A minor certificate supplies no exact displacement. Mixed-source
            // chains retain explicit or stored displacement for their zero classes.
            Self::CertifiedMinor(_) => Ok(false),
        }
    }

    pub(super) const fn preserves_source_vertices(self) -> bool {
        matches!(self, Self::Preserved { .. })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ComputedVertexIntent {
    Canonical,
    Preserved,
    /// This vertex's XY and ordinates are sampled from an adjacent retained
    /// relation, rather than read from the layout Point.
    Sampled,
}

pub(crate) struct ComputedSampleOrdinates {
    pub(crate) y: f64,
    pub(crate) z: ZOrdinate,
    pub(crate) m: MOrdinate,
}

pub(super) fn collect_edge_intents<'intent, P: Coordinates + ?Sized>(
    points: &P,
    intent: impl Fn(usize, Point, Point) -> Result<ComputedEdgeIntent<'intent>>,
) -> Result<Vec<ComputedEdgeIntent<'intent>>> {
    let edge_count = points.coord_count().saturating_sub(1);
    let mut intents = Vec::with_capacity(edge_count);
    for index in 0..edge_count {
        intents.push(intent(
            index,
            points.nth_coord(index),
            points.nth_coord(index.wrapping_add(1)),
        )?);
    }
    Ok(intents)
}

pub(super) fn sample_xy_ordinates(
    _edge_index: usize,
    endpoints: &[Point; 2],
    _relation: &ComputedLongitudeRelation<'_>,
    parameter: f64,
    _retained: &RetainedComputedSample,
) -> Result<ComputedSampleOrdinates> {
    let [start, end] = *endpoints;
    if start.z().is_some() || start.m().is_some() || end.z().is_some() || end.m().is_some() {
        return Err(GeometryErrorKind::ordinate_dropped(
            "geographic computed publication",
        ));
    }
    Ok(ComputedSampleOrdinates {
        y: interpolate_subdivision_ordinate(start.y, end.y, parameter),
        z: ZOrdinate(None),
        m: MOrdinate(None),
    })
}
