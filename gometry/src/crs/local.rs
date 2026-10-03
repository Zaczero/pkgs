//! Local conformal projection planning.
//!
//! The planner is deliberately extent-aware: every candidate is checked with
//! PROJ factors over the receiver's vertices and its spherical bounding frame.
//! A projection is returned only when its worst linear scale error is at most
//! [`LOCAL_SCALE_ERROR_LIMIT`].

use std::borrow::Cow;
use std::num::NonZeroUsize;

use super::catalog::catalog_with_state;
use crate::crs::in_core::{
    DatumKey, ParamUnit, Pole, datum_key, parse_param_value, polar_certificate_terms,
    tmerc_certificate_terms,
};
use crate::crs::{
    AreaOfInterest, ChartEdge, CrsCatalogOptions, CrsCoordinateOperationInfo, CrsError, CrsInfo,
    CrsObjectKind, MethodInfo, OperationInfo, OperationParameterInfo, RuntimeState, Shape,
    TransformOptions, factors_at, info_with_state, is_wgs84_lonlat, normalize,
    operation_info_with_state, parse_epsg, preserve_resource_error, to_projjson_with_options_at,
    transform_at, utm_crs,
};
use crate::error::{Error, Result};
use crate::geographic::{ChartStep, EdgeDirection, GeographicFrame, Latitude};
use crate::geometry::{
    CoordSeq, Point, ShapeCountQuery, admit_shape_vector_capacity, line_segments,
};

/// Maximum admitted meridional or parallel scale error (0.1%).
pub(crate) const LOCAL_SCALE_ERROR_LIMIT: f64 = 0.001;
const LOCAL_INPUT_REFINEMENT_FRACTION: f64 = 1.0e-4;
const LOCAL_CRS_DISPLAY_NAME: &str = "Local conformal CRS";

pub(crate) fn local_projection_refinement_budget(
    frame_extent_m: f64,
    operation_distance_m: Option<f64>,
) -> f64 {
    let extent = operation_distance_m.map_or(frame_extent_m, |distance| {
        frame_extent_m.min(distance.abs())
    });
    extent * LOCAL_INPUT_REFINEMENT_FRACTION
}

#[derive(Debug)]
pub(crate) struct LocalCrsDefinition {
    pub(crate) definition: String,
    pub(crate) display_name: Option<&'static str>,
}

impl LocalCrsDefinition {
    const fn authority(definition: String) -> Self {
        Self {
            definition,
            display_name: None,
        }
    }

    const fn custom(definition: String) -> Self {
        Self {
            definition,
            display_name: Some(LOCAL_CRS_DISPLAY_NAME),
        }
    }

    pub(crate) fn annotate_error(&self, mut error: Error) -> Error {
        error.annotate_crs_display(&self.definition, self.display_name);
        error
    }
}

/// Estimate one conformal metric CRS for all coordinates in `shape`.
///
/// `source_crs` is mandatory by design: interpreting unframed coordinates as
/// longitude/latitude would turn a convenience method into a relabeling trap.
pub(crate) fn estimate_local_crs(shape: &Shape, source_crs: &str) -> Result<String> {
    let state = crate::crs::runtime_state_snapshot()?;
    Ok(estimate_local_crs_at(&[shape], source_crs, &state)?.definition)
}

/// One conformal metric CRS for all coordinates of `shapes` together.
pub(crate) fn estimate_local_crs_at(
    shapes: &[&Shape],
    source_crs: &str,
    state: &RuntimeState,
) -> Result<LocalCrsDefinition> {
    let extent =
        LocalExtent::from_shapes(shapes, source_crs, state)?.ok_or(CrsError::EmptyGeometry)?;

    let pole = if extent.latitude >= 0.0 {
        Pole::North
    } else {
        Pole::South
    };
    if extent.latitude > 84.0 || extent.latitude < -80.0 {
        if has_wgs84_datum_fast_path(source_crs) {
            let ups = match pole {
                Pole::North => "EPSG:5041",
                Pole::South => "EPSG:5042",
            };
            if candidate_fits(ups, &extent.samples, state)? {
                return validated_local_crs(
                    LocalCrsDefinition::authority(ups.to_owned()),
                    source_crs,
                    state,
                );
            }
        }
        let local = best_custom_candidate(source_crs, &extent, state)?;
        return validated_local_crs(local, source_crs, state);
    }

    let utm = best_utm_from_anchor(
        LocalProjectionAnchor {
            longitude: extent.longitude,
            latitude: extent.latitude,
        },
        source_crs,
        state,
    )?;
    if let Some(utm) = utm
        && candidate_fits(&utm, &extent.samples, state)?
    {
        return validated_local_crs(LocalCrsDefinition::authority(utm), source_crs, state);
    }

    let local =
        best_custom_candidate(source_crs, &extent, state).map_err(best_custom_candidate_error)?;
    validated_local_crs(local, source_crs, state)
}

fn best_custom_candidate_error(error: crate::error::Error) -> crate::error::Error {
    preserve_resource_error(error, |_| {
        CrsError::invalid(format!(
            "geometry cannot be represented by one local conformal CRS within the {:.1}% linear scale-error limit; split the geometry or choose a projected CRS explicitly",
            LOCAL_SCALE_ERROR_LIMIT * 100.0
        ))
    })
}

pub(crate) fn local_crs_fits_at(
    shape: &Shape,
    source_crs: &str,
    candidate: &str,
    state: &RuntimeState,
) -> Result<bool> {
    let Some(extent) = LocalExtent::from_shapes(&[shape], source_crs, state)? else {
        return Ok(true);
    };
    candidate_fits(candidate, &extent.samples, state)
}

/// Check the resolved operation that a local-frame certificate will cover.
///
/// Projection curvature does not bound a datum transformation. Local frames
/// must retain the source ellipsoid and prime meridian, and the selected
/// operation may contain only its projection and coordinate-convention steps.
pub(crate) fn ensure_datum_preserving_local_operation(
    source_crs: &str,
    local_crs: &str,
    state: &RuntimeState,
) -> Result<()> {
    let source_crs_info = info_with_state(source_crs, state)?;
    let source = geodetic_info(source_crs, state)?;
    let local = info_with_state(local_crs, state)?;
    let fail = || {
        CrsError::invalid(format!(
            "local projection from {source_crs} to {local_crs} changes the source datum or contains an unowned coordinate operation"
        ))
    };

    if !same_ellipsoid(&source, &local)
        || !same_prime_meridian(&source, &local)
        || !same_datum(&source, &local)
    {
        return Err(fail());
    }

    // Projected inputs already carry a projection in their source operation.
    // The local constructive path does not use these frames; their base datum
    // and the destination ellipsoid are still checked above.
    if !source_crs_info.is_geographic() {
        return Ok(());
    }

    let Some(expected_method) = local
        .coordinate_operation
        .as_ref()
        .and_then(|operation| operation.method.as_ref())
        .and_then(|method| method.code.as_deref())
        .filter(|code| matches!(*code, "9807" | "9810"))
    else {
        return Err(fail());
    };
    let operation =
        operation_info_with_state(source_crs, local_crs, &TransformOptions::default(), state)?;
    if !operation_is_datum_preserving(&operation, expected_method) {
        return Err(fail());
    }
    Ok(())
}

fn validated_local_crs(
    local: LocalCrsDefinition,
    source_crs: &str,
    state: &RuntimeState,
) -> Result<LocalCrsDefinition> {
    ensure_datum_preserving_local_operation(source_crs, &local.definition, state)?;
    Ok(local)
}

fn geodetic_info(crs: &str, state: &RuntimeState) -> Result<std::sync::Arc<CrsInfo>> {
    let info = info_with_state(crs, state)?;
    if info.kind.starts_with("geographic") {
        return Ok(info);
    }
    let Some(geodetic_crs) = info.geodetic_crs.as_ref() else {
        return Err(CrsError::invalid(format!(
            "{crs} does not expose a geodetic CRS for a local projection"
        )));
    };
    info_with_state(
        geodetic_crs
            .definition
            .as_deref()
            .unwrap_or(&geodetic_crs.crs),
        state,
    )
}

const fn same_ellipsoid(left: &CrsInfo, right: &CrsInfo) -> bool {
    let (Some(left), Some(right)) = (&left.ellipsoid, &right.ellipsoid) else {
        return false;
    };
    left.semi_major_metre.to_bits() == right.semi_major_metre.to_bits()
        && left.semi_minor_metre.to_bits() == right.semi_minor_metre.to_bits()
}

fn same_prime_meridian(left: &CrsInfo, right: &CrsInfo) -> bool {
    let longitude_radians = |info: &CrsInfo| {
        info.prime_meridian.as_ref().map_or(0.0, |meridian| {
            meridian.longitude * meridian.unit_conversion_factor
        })
    };
    longitude_radians(left).to_bits() == longitude_radians(right).to_bits()
}

fn same_datum(left: &CrsInfo, right: &CrsInfo) -> bool {
    match (datum_key(left), datum_key(right)) {
        (Some(left), Some(right)) => left == right,
        _ => left
            .datum
            .as_ref()
            .zip(right.datum.as_ref())
            .is_some_and(|(left, right)| left.kind == right.kind && left.name == right.name),
    }
}

fn operation_is_datum_preserving(operation: &OperationInfo, expected_method: &str) -> bool {
    if operation.has_ballpark_transformation
        || operation.requires_coordinate_epoch
        || !operation.grids.is_empty()
        || !operation_definition_is_projection_only(
            operation.definition.as_deref().unwrap_or_default(),
        )
    {
        return false;
    }

    let mut projection_steps = 0_usize;
    if let Some(method) = operation.method.as_ref()
        && !allow_local_operation_method(
            method,
            &operation.parameters,
            expected_method,
            &mut projection_steps,
        )
    {
        return false;
    }
    operation.steps.iter().all(|step| {
        coordinate_operation_is_datum_preserving(step, expected_method, &mut projection_steps)
    }) && projection_steps == 1
}

fn coordinate_operation_is_datum_preserving(
    operation: &CrsCoordinateOperationInfo,
    expected_method: &str,
    projection_steps: &mut usize,
) -> bool {
    if operation.has_ballpark_transformation
        || operation.requires_coordinate_epoch
        || !operation.grids.is_empty()
    {
        return false;
    }
    if let Some(method) = operation.method.as_ref()
        && !allow_local_operation_method(
            method,
            &operation.parameters,
            expected_method,
            projection_steps,
        )
    {
        return false;
    }
    operation.steps.iter().all(|step| {
        coordinate_operation_is_datum_preserving(step, expected_method, projection_steps)
    })
}

fn allow_local_operation_method(
    method: &MethodInfo,
    parameters: &[OperationParameterInfo],
    expected_method: &str,
    projection_steps: &mut usize,
) -> bool {
    if method.authority.as_deref() == Some("EPSG")
        && method.code.as_deref() == Some(expected_method)
    {
        *projection_steps = projection_steps.wrapping_add(1);
        return true;
    }
    matches!(method.code.as_deref(), Some("9601" | "9843" | "9844"))
        || method.name.as_deref().is_some_and(|name| {
            name.starts_with("Axis Order Reversal")
                || name.eq_ignore_ascii_case("Unit conversion")
                || name.eq_ignore_ascii_case("Longitude rotation")
        })
        || (method.authority.as_deref() == Some("EPSG")
            && method.code.as_deref() == Some("9660")
            && null_geographic_3d_offsets(parameters))
}

fn null_geographic_3d_offsets(parameters: &[OperationParameterInfo]) -> bool {
    !parameters.is_empty() && parameters.iter().all(|parameter| parameter.value == 0.0)
}

fn operation_definition_is_projection_only(definition: &str) -> bool {
    let mut projection_count = 0_usize;
    for token in definition.split_ascii_whitespace() {
        if let Some(projection) = token.strip_prefix("proj=") {
            match projection {
                "pipeline" | "unitconvert" | "axisswap" | "longlat" | "noop" => {},
                "tmerc" | "utm" | "stere" => {
                    projection_count = projection_count.wrapping_add(1);
                },
                _ => return false,
            }
        } else if token.starts_with("nadgrids=")
            || token.starts_with("geoidgrids=")
            || token.starts_with("towgs84=")
        {
            return false;
        }
    }
    projection_count == 1
}

// These simple decimal upper bounds follow by squaring against the exact
// constants: 1.09² > 32/27 and 0.385² > 4/27.
const GAUSSIAN_TANGENTIAL_CURVATURE: f64 = 1.09;
const GAUSSIAN_LATITUDE_ACCELERATION: f64 = 0.385;
const FAST_TMER_BETA: f64 = 0.125;

#[derive(Clone, Copy, Debug)]
enum ProjectionCertificate {
    Tmerc(TmercCertificate),
    Ups(UpsCertificate),
}

#[derive(Clone, Copy, Debug)]
struct EllipsoidRadii {
    a: f64,
    e2_lower: f64,
    e2_upper: f64,
    one_es_lower: f64,
    one_es_upper: f64,
    n_max: f64,
    m_max: f64,
    min_radius: f64,
}

#[derive(Clone, Copy, Debug)]
struct TmercCertificate {
    radius_lower: f64,
    radius_upper: f64,
    p_min: f64,
    p_max: f64,
    p2_max: f64,
    gtu: [f64; 6],
    c1_fast: f64,
    c2_fast: f64,
}

#[derive(Clone, Copy, Debug)]
struct UpsCertificate {
    radius_lower: f64,
    radius_upper: f64,
    pole: Pole,
    p_min: f64,
    p_max: f64,
    p2_max: f64,
}

#[derive(Clone, Copy)]
struct CertificateEdge {
    longitude_delta_native: f64,
    longitude_from_central_meridian: f64,
    longitude_delta: f64,
    latitude0: f64,
    latitude1: f64,
    is_selected_pole_parallel: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct LocalProjectionCertificate {
    frame: GeographicFrame,
    central_meridian: f64,
    radii: EllipsoidRadii,
    projection: ProjectionCertificate,
}

impl LocalProjectionCertificate {
    /// `local` comes from `estimate_local_crs_at`, whose return path validates
    /// the resolved datum-preserving operation before a certificate can exist.
    pub(crate) fn from_selected_frame(
        local: &LocalCrsDefinition,
        geodesic: &geographiclib_rs::Geodesic,
        frame: GeographicFrame,
        state: &RuntimeState,
    ) -> Result<Self> {
        let info = info_with_state(&local.definition, state)?;
        if !is_east_north_metre_axes(&info) && !is_projected_metre_cartesian_axes(&info) {
            return Err(CrsError::invalid(
                "local projection certificate requires metre Cartesian axes",
            ));
        }
        let operation = info
            .coordinate_operation
            .as_ref()
            .ok_or_else(|| CrsError::invalid("local projection has no coordinate operation"))?;
        let method_info = operation
            .method
            .as_ref()
            .ok_or_else(|| CrsError::invalid("local projection has no EPSG method code"))?;
        if method_info.authority.as_deref() != Some("EPSG") {
            return Err(CrsError::invalid(
                "local projection certificate requires an EPSG projection method",
            ));
        }
        let method = method_info
            .code
            .as_deref()
            .ok_or_else(|| CrsError::invalid("local projection has no EPSG method code"))?;
        let latitude_of_origin = local_projection_parameter(operation, "8801", ParamUnit::Degrees)
            .ok_or_else(|| CrsError::invalid("local projection has no latitude of origin"))?;
        let longitude_of_origin = local_projection_parameter(operation, "8802", ParamUnit::Degrees)
            .ok_or_else(|| CrsError::invalid("local projection has no longitude of origin"))?;
        let k0 = local_projection_parameter(operation, "8805", ParamUnit::Unity)
            .ok_or_else(|| CrsError::invalid("local projection has no central scale factor"))?;
        let central_meridian = frame
            .point_from_geodesic(Point::new_raw_xy(longitude_of_origin, 0.0))
            .map_err(|error| {
                CrsError::invalid(format!(
                    "local projection origin is outside the source chart: {error}"
                ))
            })?
            .x;
        let radii = certificate_math::ellipsoid_radii(geodesic.a, geodesic.f)?;
        let projection = match method {
            "9807" => {
                let terms = tmerc_certificate_terms(geodesic.a, geodesic.f, k0);
                let (
                    latitude_derivative_minimum,
                    latitude_derivative_maximum,
                    latitude_second_derivative_maximum,
                ) = certificate_math::tmerc_latitude_derivative_bounds(&terms.cbg);
                if !(latitude_derivative_minimum.is_finite()
                    && latitude_derivative_minimum > 0.0
                    && latitude_derivative_maximum.is_finite()
                    && latitude_second_derivative_maximum.is_finite())
                {
                    return Err(CrsError::invalid(
                        "local transverse Mercator latitude map cannot be certified",
                    ));
                }
                let (c1_fast, c2_fast) = certificate_math::correction_derivative_bounds(
                    &terms.gtu,
                    FAST_TMER_BETA,
                    FAST_TMER_BETA,
                );
                ProjectionCertificate::Tmerc(TmercCertificate {
                    radius_lower: certificate_math::multiply_down(geodesic.a, terms.qn),
                    radius_upper: certificate_math::multiply_up(geodesic.a, terms.qn),
                    p_min: latitude_derivative_minimum,
                    p_max: latitude_derivative_maximum,
                    p2_max: latitude_second_derivative_maximum,
                    gtu: terms.gtu,
                    c1_fast,
                    c2_fast,
                })
            },
            "9810" => {
                let terms = polar_certificate_terms(geodesic.a, geodesic.f, latitude_of_origin, k0)
                    .ok_or_else(|| {
                        CrsError::invalid("local UPS parameters do not match polar stereographic")
                    })?;
                let (
                    latitude_derivative_minimum,
                    latitude_derivative_maximum,
                    latitude_second_derivative_maximum,
                ) = certificate_math::ups_latitude_derivative_bounds(terms.e);
                if !(latitude_derivative_minimum.is_finite()
                    && latitude_derivative_minimum > 0.0
                    && latitude_derivative_maximum.is_finite()
                    && latitude_second_derivative_maximum.is_finite())
                {
                    return Err(CrsError::invalid(
                        "local polar stereographic latitude map cannot be certified",
                    ));
                }
                ProjectionCertificate::Ups(UpsCertificate {
                    radius_lower: certificate_math::multiply_down(terms.a, terms.akm1) / 2.0,
                    radius_upper: certificate_math::multiply_up(terms.a, terms.akm1) / 2.0,
                    pole: terms.pole,
                    p_min: latitude_derivative_minimum,
                    p_max: latitude_derivative_maximum,
                    p2_max: latitude_second_derivative_maximum,
                })
            },
            _ => {
                return Err(CrsError::invalid(
                    "local projection certificate supports only Transverse Mercator and UPS",
                ));
            },
        };
        Ok(Self {
            frame,
            central_meridian,
            radii,
            projection,
        })
    }

    fn chart_edge(&self, start: Point, end: Point) -> Result<CertificateEdge> {
        let axis = self.frame.longitude();
        let longitude_delta_native = match axis
            .chart_step(start.x, end.x)
            .expect("screen coordinates were admitted before local projection")
        {
            ChartStep::Shortest(EdgeDirection::Minor(delta)) => delta,
            step @ ChartStep::Circle(_) => step.delta(axis),
            ChartStep::Shortest(EdgeDirection::HalfTurn(direction)) => {
                // Preserve ChartEdge's public-facing exact-half-turn error.
                ChartEdge::new(start, end, self.frame)?;
                unreachable!("ChartEdge rejects an exact half-turn: {direction:?}")
            },
        };
        let latitude0_native = Latitude::from_admitted(self.frame, start.y).get();
        let latitude1_native = Latitude::from_admitted(self.frame, end.y).get();
        let latitude_unit = self.frame.latitude_unit().radians_per_unit();
        let longitude_from_central_meridian = axis
            .shortest_alias_step(self.central_meridian, start.x)
            .expect("screen coordinates were admitted before local projection")
            .delta(axis)
            * axis.unit().radians_per_unit();
        let selected_pole = match self.projection {
            ProjectionCertificate::Tmerc(_) => None,
            ProjectionCertificate::Ups(certificate) => Some(match certificate.pole {
                Pole::North => self.frame.pole(),
                Pole::South => -self.frame.pole(),
            }),
        };
        Ok(CertificateEdge {
            longitude_delta_native,
            longitude_from_central_meridian,
            longitude_delta: longitude_delta_native * axis.unit().radians_per_unit(),
            latitude0: latitude0_native * latitude_unit,
            latitude1: latitude1_native * latitude_unit,
            is_selected_pole_parallel: selected_pole.is_some_and(|pole| {
                start.y.to_bits() == pole.to_bits() && end.y.to_bits() == pole.to_bits()
            }),
        })
    }

    fn is_straight_chart_edge(&self, edge: CertificateEdge) -> bool {
        match self.projection {
            ProjectionCertificate::Tmerc(_) => {
                (edge.longitude_delta_native == 0.0 && edge.longitude_from_central_meridian == 0.0)
                    || (edge.latitude0 == 0.0 && edge.latitude1 == 0.0)
            },
            ProjectionCertificate::Ups(_) => {
                edge.longitude_delta_native == 0.0 || edge.is_selected_pole_parallel
            },
        }
    }

    fn cheap_bound(&self, edge: CertificateEdge) -> f64 {
        match self.projection {
            ProjectionCertificate::Tmerc(certificate) => self.cheap_tmerc_bound(
                &certificate,
                edge.longitude_from_central_meridian,
                edge.longitude_delta,
                edge.latitude0,
                edge.latitude1,
            ),
            ProjectionCertificate::Ups(certificate) => self.cheap_ups_bound(
                certificate,
                edge.longitude_delta,
                edge.latitude0,
                edge.latitude1,
            ),
        }
    }

    /// Arithmetic-only screen for one source-chart edge.
    ///
    /// It performs no projection, geodesic integration, or allocation. The
    /// caller can scan a geometry and return its original source sequence
    /// immediately when every edge is safe.
    pub(crate) fn edge_needs_refinement(
        &self,
        start: Point,
        end: Point,
        budget: f64,
    ) -> Result<bool> {
        let edge = self.chart_edge(start, end)?;
        if self.is_straight_chart_edge(edge) {
            return Ok(false);
        }
        Ok(self
            .cheap_bound(edge)
            .partial_cmp(&budget)
            .is_none_or(std::cmp::Ordering::is_gt))
    }

    /// Return whether any stored chart edge needs refinement, without
    /// allocating a plan or a replacement coordinate sequence.
    pub(crate) fn has_edges_to_refine(&self, points: &CoordSeq, budget: f64) -> Result<bool> {
        for segment in line_segments(points) {
            if self.edge_needs_refinement(
                Point::new_raw_xy(segment.start.x, segment.start.y),
                Point::new_raw_xy(segment.end.x, segment.end.y),
                budget,
            )? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Return the certified piece count for one stored chart edge.
    pub(crate) fn piece_count(
        &self,
        start: Point,
        end: Point,
        budget: f64,
    ) -> Result<NonZeroUsize> {
        let edge = self.chart_edge(start, end)?;
        if self.is_straight_chart_edge(edge) || self.cheap_bound(edge) <= budget {
            return Ok(NonZeroUsize::MIN);
        }

        let precise_bound = match self.projection {
            ProjectionCertificate::Tmerc(certificate) => self.precise_tmerc_bound(
                &certificate,
                edge.longitude_from_central_meridian,
                edge.longitude_delta,
                edge.latitude0,
                edge.latitude1,
            ),
            ProjectionCertificate::Ups(certificate) => self.precise_ups_bound(
                certificate,
                edge.longitude_delta,
                edge.latitude0,
                edge.latitude1,
            ),
        };
        Ok(certificate_math::piece_count_from_bound(
            precise_bound,
            budget,
        ))
    }

    fn cheap_tmerc_bound(
        &self,
        certificate: &TmercCertificate,
        longitude_from_central_meridian: f64,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        certificate_math::cheap_tmerc_bound(
            &self.radii,
            certificate,
            longitude_from_central_meridian,
            longitude_delta,
            latitude0,
            latitude1,
        )
    }

    fn cheap_ups_bound(
        &self,
        certificate: UpsCertificate,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        certificate_math::cheap_ups_bound(
            &self.radii,
            certificate,
            longitude_delta,
            latitude0,
            latitude1,
        )
    }

    fn precise_tmerc_bound(
        &self,
        certificate: &TmercCertificate,
        longitude_from_central_meridian: f64,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        certificate_math::precise_tmerc_bound(
            &self.radii,
            certificate,
            longitude_from_central_meridian,
            longitude_delta,
            latitude0,
            latitude1,
        )
    }

    fn precise_ups_bound(
        &self,
        certificate: UpsCertificate,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        certificate_math::precise_ups_bound(
            &self.radii,
            certificate,
            longitude_delta,
            latitude0,
            latitude1,
        )
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    clippy::float_cmp,
    reason = "these operations compute the outward-rounded analytic local-projection certificate"
)]
mod certificate_math {
    use super::{
        CrsError, EllipsoidRadii, FAST_TMER_BETA, GAUSSIAN_LATITUDE_ACCELERATION,
        GAUSSIAN_TANGENTIAL_CURVATURE, NonZeroUsize, Pole, Result, TmercCertificate,
        UpsCertificate,
    };

    #[derive(Clone, Copy)]
    struct IntervalRadii {
        near_abs_lower: f64,
        n_max: f64,
        m_max: f64,
        min_radius: f64,
    }

    fn round_up(value: f64) -> f64 {
        if value == 0.0 || !value.is_finite() {
            value
        } else {
            value.next_up()
        }
    }

    fn round_down(value: f64) -> f64 {
        if value == 0.0 || !value.is_finite() {
            value
        } else {
            value.next_down()
        }
    }

    fn add_up(left: f64, right: f64) -> f64 {
        round_up(left + right)
    }

    fn add_down(left: f64, right: f64) -> f64 {
        round_down(left + right)
    }

    fn subtract_up(left: f64, right: f64) -> f64 {
        round_up(left - right)
    }

    fn subtract_down(left: f64, right: f64) -> f64 {
        round_down(left - right)
    }

    pub(super) fn multiply_up(left: f64, right: f64) -> f64 {
        round_up(left * right)
    }

    pub(super) fn multiply_down(left: f64, right: f64) -> f64 {
        round_down(left * right)
    }

    fn divide_up(numerator: f64, denominator: f64) -> f64 {
        round_up(numerator / denominator)
    }

    fn divide_down(numerator: f64, denominator: f64) -> f64 {
        round_down(numerator / denominator)
    }

    fn multiply_interval(
        left_lower: f64,
        left_upper: f64,
        right_lower: f64,
        right_upper: f64,
    ) -> (f64, f64) {
        let lower = multiply_down(left_lower, right_lower)
            .min(multiply_down(left_lower, right_upper))
            .min(multiply_down(left_upper, right_lower))
            .min(multiply_down(left_upper, right_upper));
        let upper = multiply_up(left_lower, right_lower)
            .max(multiply_up(left_lower, right_upper))
            .max(multiply_up(left_upper, right_lower))
            .max(multiply_up(left_upper, right_upper));
        (lower, upper)
    }

    pub(super) fn ellipsoid_radii(a: f64, f: f64) -> Result<EllipsoidRadii> {
        if !(a.is_finite() && a > 0.0 && f.is_finite() && f < 1.0) {
            return Err(CrsError::invalid(
                "local projection certificate requires a supported ellipsoid",
            ));
        }
        let flattening_complement_lower = subtract_down(2.0, f);
        let flattening_complement_upper = subtract_up(2.0, f);
        let (e2_lower, e2_upper) = if f >= 0.0 {
            (
                multiply_down(f, flattening_complement_lower),
                multiply_up(f, flattening_complement_upper),
            )
        } else {
            (
                multiply_down(f, flattening_complement_upper),
                multiply_up(f, flattening_complement_lower),
            )
        };
        let one_es_lower = subtract_down(1.0, e2_upper);
        let one_es_upper = subtract_up(1.0, e2_lower);
        if one_es_lower <= 0.0 {
            return Err(CrsError::invalid(
                "local projection certificate requires positive ellipsoid curvature radii",
            ));
        }

        let (e2_product_lower, e2_product_upper) = multiply_interval(e2_lower, e2_upper, 0.0, 1.0);
        let d_min = subtract_down(1.0, e2_product_upper);
        let d_max = subtract_up(1.0, e2_product_lower);
        if d_min <= 0.0 {
            return Err(CrsError::invalid(
                "local projection certificate requires positive ellipsoid curvature radii",
            ));
        }
        // For D > 0, sqrt(D) >= min(D, 1) and D^(3/2) >= min(D², 1).
        let n_denominator_lower = d_min.min(1.0);
        let m_denominator_lower = multiply_down(d_min, d_min).min(1.0);
        let n_max = divide_up(a, n_denominator_lower);
        let m_max = divide_up(multiply_up(a, one_es_upper), m_denominator_lower);
        let n_denominator_upper = d_max.max(1.0);
        let m_denominator_upper = multiply_up(d_max, d_max).max(1.0);
        let n_min = divide_down(a, n_denominator_upper);
        let m_min = divide_down(multiply_down(a, one_es_lower), m_denominator_upper);
        Ok(EllipsoidRadii {
            a,
            e2_lower,
            e2_upper,
            one_es_lower,
            one_es_upper,
            n_max,
            m_max,
            min_radius: n_min.min(m_min),
        })
    }

    pub(super) fn tmerc_latitude_derivative_bounds(coefficients: &[f64; 6]) -> (f64, f64, f64) {
        let mut first_derivative = 0.0;
        let mut second_derivative = 0.0;
        for (index, coefficient) in coefficients.iter().enumerate() {
            let harmonic = 2.0 * f64::from(u32::try_from(index + 1).expect("six TM coefficients"));
            let magnitude = coefficient.abs();
            first_derivative = add_up(first_derivative, multiply_up(harmonic, magnitude));
            second_derivative = add_up(
                second_derivative,
                multiply_up(multiply_up(harmonic, harmonic), magnitude),
            );
        }
        (
            subtract_down(1.0, first_derivative),
            add_up(1.0, first_derivative),
            second_derivative,
        )
    }

    pub(super) fn ups_latitude_derivative_bounds(e: f64) -> (f64, f64, f64) {
        let e_upper = round_up(e);
        let eccentricity_squared_upper = multiply_up(e_upper, e_upper);
        let one_es_lower = subtract_down(1.0, eccentricity_squared_upper);
        let h = divide_up(eccentricity_squared_upper, one_es_lower);
        let one_minus_h = subtract_down(1.0, h);
        let one_minus_two_h = subtract_down(1.0, multiply_up(2.0, h));
        if one_es_lower <= 0.0 || one_minus_h <= 0.0 || one_minus_two_h <= 0.0 {
            return (0.0, f64::INFINITY, f64::INFINITY);
        }
        // atanh(e) <= e/(1-e²), and exp(x) <= 1/(1-x) for 0 <= x < 1.
        let exp_h_upper = divide_up(1.0, one_minus_h);
        let exp_two_h_upper = divide_up(1.0, one_minus_two_h);
        let first = multiply_up(h, exp_two_h_upper);
        let second = divide_up(multiply_up(3.0, eccentricity_squared_upper), one_es_lower);
        let p_max = exp_h_upper;
        let derivative_minimum = divide_down(one_es_lower, exp_h_upper);
        let second_derivative_maximum = multiply_up(p_max, add_up(first, second));
        (derivative_minimum, p_max, second_derivative_maximum)
    }

    pub(super) fn correction_derivative_bounds(
        coefficients: &[f64; 6],
        beta_lower: f64,
        beta_upper: f64,
    ) -> (f64, f64) {
        if !(0.0..1.0).contains(&beta_upper) {
            return (f64::INFINITY, f64::INFINITY);
        }
        let beta_lower = beta_lower.clamp(0.0, beta_upper);
        let beta_squared_lower = multiply_down(beta_lower, beta_lower).max(0.0);
        let beta_squared_upper = multiply_up(beta_upper, beta_upper);
        let cosh_numerator_lower = add_down(1.0, beta_squared_lower);
        let cosh_numerator_upper = add_up(1.0, beta_squared_upper);
        let cosh_denominator_lower = subtract_down(1.0, beta_squared_upper);
        let cosh_denominator_upper = subtract_up(1.0, beta_squared_lower);
        if cosh_denominator_lower <= 0.0 {
            return (f64::INFINITY, f64::INFINITY);
        }
        let cosh_two_eta_lower = divide_down(cosh_numerator_lower, cosh_denominator_upper).max(1.0);
        let cosh_two_eta_upper = divide_up(cosh_numerator_upper, cosh_denominator_lower);
        let mut previous_lower = 1.0;
        let mut previous_upper = 1.0;
        let mut current_lower = cosh_two_eta_lower;
        let mut current_upper = cosh_two_eta_upper;
        let mut c1_upper = 0.0;
        let mut c2_upper = 0.0;

        for (index, coefficient) in coefficients.iter().enumerate() {
            let harmonic = 2.0 * f64::from(u32::try_from(index + 1).expect("six TM coefficients"));
            let magnitude = coefficient.abs();
            let weighted = multiply_up(multiply_up(harmonic, magnitude), current_upper);
            c1_upper = add_up(c1_upper, weighted);
            c2_upper = add_up(c2_upper, multiply_up(harmonic, weighted));

            if index + 1 < coefficients.len() {
                let next_upper = subtract_up(
                    multiply_up(multiply_up(2.0, cosh_two_eta_upper), current_upper),
                    previous_lower,
                );
                let next_lower = subtract_down(
                    multiply_down(multiply_down(2.0, cosh_two_eta_lower), current_lower),
                    previous_upper,
                )
                .max(1.0);
                previous_lower = current_lower;
                previous_upper = current_upper;
                current_lower = next_lower;
                current_upper = next_upper;
            }
        }
        (c1_upper, c2_upper)
    }

    fn latitude_absolute_range(latitude0: f64, latitude1: f64) -> (f64, f64) {
        let lower = round_down(latitude0.min(latitude1));
        let upper = round_up(latitude0.max(latitude1));
        let nearest = if lower <= 0.0 && upper >= 0.0 {
            0.0
        } else {
            round_down(if lower > 0.0 { lower } else { upper.abs() }).max(0.0)
        };
        let farthest =
            round_up(lower.abs().max(upper.abs())).min(round_up(std::f64::consts::FRAC_PI_2));
        (nearest, farthest)
    }

    fn ellipsoid_interval_radii(
        radii: &EllipsoidRadii,
        latitude0: f64,
        latitude1: f64,
    ) -> IntervalRadii {
        let (near_abs_lower, far_abs_upper) = latitude_absolute_range(latitude0, latitude1);
        let sin_near_lower = sine_lower(near_abs_lower);
        let sin_far_upper = sine_upper(far_abs_upper);
        let squared_sine_near_lower = multiply_down(sin_near_lower, sin_near_lower).max(0.0);
        let squared_sine_far_upper = multiply_up(sin_far_upper, sin_far_upper).min(1.0);
        let (e2_product_lower, e2_product_upper) = multiply_interval(
            radii.e2_lower,
            radii.e2_upper,
            squared_sine_near_lower,
            squared_sine_far_upper,
        );
        let d_min = subtract_down(1.0, e2_product_upper);
        let d_max = subtract_up(1.0, e2_product_lower);
        if d_min <= 0.0 || d_max <= 0.0 {
            return IntervalRadii {
                near_abs_lower,
                n_max: f64::INFINITY,
                m_max: f64::INFINITY,
                min_radius: 0.0,
            };
        }
        let n_denominator_lower = d_min.min(1.0);
        let m_denominator_lower = multiply_down(d_min, d_min).min(1.0);
        let n_max = divide_up(radii.a, n_denominator_lower);
        let m_max = divide_up(
            multiply_up(radii.a, radii.one_es_upper),
            m_denominator_lower,
        );
        let n_denominator_upper = d_max.max(1.0);
        let m_denominator_upper = multiply_up(d_max, d_max).max(1.0);
        let n_min = divide_down(radii.a, n_denominator_upper);
        let m_min = divide_down(
            multiply_down(radii.a, radii.one_es_lower),
            m_denominator_upper,
        );
        IntervalRadii {
            near_abs_lower,
            n_max,
            m_max,
            min_radius: n_min.min(m_min),
        }
    }

    fn absolute_difference_upper(left: f64, right: f64) -> f64 {
        if left == right {
            return 0.0;
        }
        let left_lower = round_down(left);
        let left_upper = round_up(left);
        let right_lower = round_down(right);
        let right_upper = round_up(right);
        round_up(
            (left_upper - right_lower)
                .abs()
                .max((right_upper - left_lower).abs()),
        )
    }

    fn cosine_upper(angle_lower: f64) -> f64 {
        // On [0, π/2], the alternating Taylor polynomial through x⁴/4!
        // is an upper bound for cos(x).
        let square_lower = multiply_down(angle_lower, angle_lower).max(0.0);
        let square_upper = multiply_up(angle_lower, angle_lower).max(0.0);
        let fourth_upper = multiply_up(square_upper, square_upper);
        add_up(
            subtract_up(1.0, divide_down(square_lower, 2.0)),
            divide_up(fourth_upper, 24.0),
        )
        .min(1.0)
    }

    fn sine_lower(angle_lower: f64) -> f64 {
        // sin(x) >= x − x³/3! on [0, π/2], by the alternating series.
        let square_upper = multiply_up(angle_lower, angle_lower);
        subtract_down(
            angle_lower,
            divide_up(multiply_up(angle_lower, square_upper), 6.0),
        )
        .max(0.0)
    }

    fn sine_upper(angle_upper: f64) -> f64 {
        if angle_upper >= round_down(std::f64::consts::FRAC_PI_2) {
            return 1.0;
        }
        let square_lower = multiply_down(angle_upper, angle_upper).max(0.0);
        let square_upper = multiply_up(angle_upper, angle_upper).max(0.0);
        let cube_lower = multiply_down(square_lower, angle_upper);
        let fourth_upper = multiply_up(square_upper, square_upper);
        let fifth_upper = multiply_up(fourth_upper, angle_upper);
        add_up(
            subtract_up(angle_upper, divide_down(cube_lower, 6.0)),
            divide_up(fifth_upper, 120.0),
        )
        .min(1.0)
    }

    fn tangent_upper(angle_upper: f64) -> f64 {
        let pole_distance_lower =
            subtract_down(round_down(std::f64::consts::FRAC_PI_2), angle_upper);
        if pole_distance_lower <= 0.0 {
            return f64::INFINITY;
        }
        let reciprocal_colatitude = divide_up(1.0, pole_distance_lower);
        if angle_upper >= round_down(std::f64::consts::FRAC_PI_2) {
            return reciprocal_colatitude;
        }

        let square_upper = multiply_up(angle_upper, angle_upper);
        let square_lower = multiply_down(angle_upper, angle_upper).max(0.0);
        let fourth_lower = multiply_down(square_lower, square_lower);
        let fourth_upper = multiply_up(square_upper, square_upper);
        let sixth_upper = multiply_up(fourth_upper, square_upper);
        let cosine_lower = add_down(
            subtract_down(1.0, divide_up(square_upper, 2.0)),
            divide_down(fourth_lower, 24.0),
        );
        let cosine_lower = subtract_down(cosine_lower, divide_up(sixth_upper, 720.0));
        if cosine_lower <= 0.0 {
            return reciprocal_colatitude;
        }
        divide_up(sine_upper(angle_upper), cosine_lower).min(reciprocal_colatitude)
    }

    fn source_length_upper_squared(
        radii: &EllipsoidRadii,
        latitude0: f64,
        latitude1: f64,
        longitude_delta: f64,
        coarse: bool,
    ) -> f64 {
        let interval = ellipsoid_interval_radii(radii, latitude0, latitude1);
        let cosine = if coarse {
            round_up(round_up(std::f64::consts::FRAC_PI_2) - interval.near_abs_lower).min(1.0)
        } else {
            cosine_upper(interval.near_abs_lower)
        };
        let n_max = if coarse { radii.n_max } else { interval.n_max };
        let m_max = if coarse { radii.m_max } else { interval.m_max };
        let east = multiply_up(multiply_up(n_max, cosine), round_up(longitude_delta.abs()));
        let north = multiply_up(m_max, absolute_difference_upper(latitude0, latitude1));
        add_up(multiply_up(east, east), multiply_up(north, north))
    }

    fn gaussian_latitude_endpoint(phi: f64, p_min: f64, p_max: f64) -> (f64, f64) {
        let absolute_lower = round_down(phi.abs()).max(0.0);
        let absolute_upper = round_up(phi.abs()).min(round_up(std::f64::consts::FRAC_PI_2));
        let pole_lower = round_down(std::f64::consts::FRAC_PI_2);
        let pole_upper = round_up(std::f64::consts::FRAC_PI_2);
        let delta_lower = subtract_down(pole_lower, absolute_upper).max(0.0);
        let delta_upper = subtract_up(pole_upper, absolute_lower).max(0.0);
        let lower = multiply_down(p_min, absolute_lower)
            .max(subtract_down(pole_lower, multiply_up(p_max, delta_upper)))
            .max(0.0);
        let upper = multiply_up(p_max, absolute_upper)
            .min(subtract_up(pole_upper, multiply_down(p_min, delta_lower)))
            .min(pole_upper);
        if phi < 0.0 {
            (round_down(-upper), round_up(-lower))
        } else {
            (lower, upper)
        }
    }

    fn gaussian_latitude_interval(
        latitude0: f64,
        latitude1: f64,
        p_min: f64,
        p_max: f64,
    ) -> (f64, f64) {
        let (lower0, upper0) = gaussian_latitude_endpoint(latitude0, p_min, p_max);
        let (lower1, upper1) = gaussian_latitude_endpoint(latitude1, p_min, p_max);
        (lower0.min(lower1), upper0.max(upper1))
    }

    fn absolute_interval_extrema(lower: f64, upper: f64) -> (f64, f64) {
        let near = if lower <= 0.0 && upper >= 0.0 {
            0.0
        } else {
            round_down(lower.abs().min(upper.abs())).max(0.0)
        };
        let far = round_up(lower.abs().max(upper.abs())).min(round_up(std::f64::consts::FRAC_PI_2));
        (near, far)
    }

    fn lambda_absolute_upper(longitude0: f64, longitude_delta: f64) -> f64 {
        let lambda0_lower = round_down(longitude0);
        let lambda0_upper = round_up(longitude0);
        let delta_lower = round_down(longitude_delta);
        let delta_upper = round_up(longitude_delta);
        let lambda1_lower = add_down(lambda0_lower, delta_lower);
        let lambda1_upper = add_up(lambda0_upper, delta_upper);
        round_up(
            lambda0_lower
                .abs()
                .max(lambda0_upper.abs())
                .max(lambda1_lower.abs())
                .max(lambda1_upper.abs()),
        )
    }

    fn maximum_absolute_sine(longitude0: f64, longitude_delta: f64) -> f64 {
        // |sin(x)| <= min(1, |x|), so the interval needs no trigonometric call.
        lambda_absolute_upper(longitude0, longitude_delta).min(1.0)
    }

    pub(super) fn piece_count_from_bound(bound: f64, budget: f64) -> NonZeroUsize {
        if bound <= budget {
            return NonZeroUsize::MIN;
        }
        if budget <= 0.0 {
            return NonZeroUsize::new(crate::geometry::GENERATED_ITEM_LIMIT + 2)
                .expect("the generated-item limit is positive");
        }
        let ratio = divide_up(bound, budget);
        let maximum_pieces = crate::geometry::GENERATED_ITEM_LIMIT + 1;
        if !ratio.is_finite() || ratio > (maximum_pieces * maximum_pieces) as f64 {
            return NonZeroUsize::new(maximum_pieces + 1)
                .expect("the generated-item limit is positive");
        }
        let mut lower = 1_usize;
        let mut upper = maximum_pieces;
        while lower < upper {
            let middle = lower.wrapping_add(upper.wrapping_sub(lower) / 2);
            let square = (middle * middle) as f64;
            if square >= ratio {
                upper = middle;
            } else {
                lower = middle.wrapping_add(1);
            }
        }
        let pieces = lower;
        NonZeroUsize::new(pieces).expect("the analytic piece count is positive")
    }

    pub(super) fn cheap_tmerc_bound(
        radii: &EllipsoidRadii,
        certificate: &TmercCertificate,
        longitude0: f64,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        let (near_abs, far_abs) = latitude_absolute_range(latitude0, latitude1);
        let pi_half_upper = round_up(std::f64::consts::FRAC_PI_2);
        let delta_near_upper = subtract_up(pi_half_upper, near_abs).max(0.0);
        let delta_far_lower =
            subtract_down(round_down(std::f64::consts::FRAC_PI_2), far_abs).max(0.0);
        if delta_far_lower == 0.0 {
            return f64::INFINITY;
        }
        let cos_chi_upper = multiply_up(certificate.p_max, delta_near_upper).min(1.0);
        let sin_lambda_upper = lambda_absolute_upper(longitude0, longitude_delta).min(1.0);
        let beta = multiply_up(cos_chi_upper, sin_lambda_upper);
        let beta2 = multiply_up(beta, beta);
        let spherical_denominator = subtract_down(1.0, beta2);
        if spherical_denominator <= 0.0 {
            return f64::INFINITY;
        }
        let (c1, c2) = if beta <= FAST_TMER_BETA {
            (certificate.c1_fast, certificate.c2_fast)
        } else {
            correction_derivative_bounds(&certificate.gtu, 0.0, beta)
        };
        let minimum_scale = subtract_down(1.0, c1);
        if minimum_scale <= 0.0 {
            return f64::INFINITY;
        }
        let latitude_curvature = add_up(
            divide_up(
                GAUSSIAN_TANGENTIAL_CURVATURE,
                multiply_down(certificate.p_min, delta_far_lower),
            ),
            divide_up(
                multiply_up(GAUSSIAN_LATITUDE_ACCELERATION, certificate.p2_max),
                multiply_down(certificate.p_min, certificate.p_min),
            ),
        );
        let logarithmic_gradient = divide_up(beta, spherical_denominator);
        let correction_gradient = divide_up(c2, minimum_scale);
        let curvature_numerator = add_up(
            add_up(latitude_curvature, logarithmic_gradient),
            correction_gradient,
        );
        let curvature = divide_up(
            curvature_numerator,
            multiply_down(certificate.radius_lower, minimum_scale),
        );
        let image_scale = divide_up(
            multiply_up(
                multiply_up(certificate.radius_upper, add_up(1.0, c1)),
                certificate.p_max,
            ),
            multiply_down(radii.min_radius, spherical_denominator),
        );
        let coefficient = divide_up(
            multiply_up(curvature, multiply_up(image_scale, image_scale)),
            8.0,
        );
        let cosine_latitude_upper = round_up(delta_near_upper).min(1.0);
        let east_weight = multiply_up(radii.n_max, cosine_latitude_upper);
        let east = multiply_up(east_weight, round_up(longitude_delta.abs()));
        let north = multiply_up(radii.m_max, absolute_difference_upper(latitude0, latitude1));
        add_up(
            multiply_up(coefficient, multiply_up(east, east)),
            multiply_up(coefficient, multiply_up(north, north)),
        )
    }

    pub(super) fn precise_tmerc_bound(
        radii: &EllipsoidRadii,
        certificate: &TmercCertificate,
        longitude0: f64,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        let Some((curvature, image_scale)) = tmerc_curvature_and_scale(
            radii,
            certificate,
            longitude0,
            longitude_delta,
            latitude0,
            latitude1,
        ) else {
            return f64::INFINITY;
        };
        let length2 =
            source_length_upper_squared(radii, latitude0, latitude1, longitude_delta, false);
        divide_up(
            multiply_up(
                multiply_up(curvature, multiply_up(image_scale, image_scale)),
                length2,
            ),
            8.0,
        )
    }

    fn tmerc_curvature_and_scale(
        radii: &EllipsoidRadii,
        certificate: &TmercCertificate,
        longitude0: f64,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> Option<(f64, f64)> {
        let (chi_lower, chi_upper) =
            gaussian_latitude_interval(latitude0, latitude1, certificate.p_min, certificate.p_max);
        let (chi_abs_lower, chi_abs_upper) = absolute_interval_extrema(chi_lower, chi_upper);
        let cosine_chi_upper = cosine_upper(chi_abs_lower);
        let longitude_sine_upper = maximum_absolute_sine(longitude0, longitude_delta);
        let beta = multiply_up(cosine_chi_upper, longitude_sine_upper);
        let beta2 = multiply_up(beta, beta);
        let spherical_denominator = subtract_down(1.0, beta2);
        if beta >= 1.0 || spherical_denominator <= 0.0 {
            return None;
        }
        let (c1, c2) = correction_derivative_bounds(&certificate.gtu, 0.0, beta);
        let minimum_scale = subtract_down(1.0, c1);
        if minimum_scale <= 0.0 {
            return None;
        }
        let curvature_sphere = longitude_curvature(
            certificate.p_min,
            certificate.p2_max,
            longitude_delta,
            latitude0,
            latitude1,
            chi_abs_upper,
        );
        let logarithmic_gradient = divide_up(beta, spherical_denominator);
        let correction_gradient = divide_up(c2, minimum_scale);
        let curvature = divide_up(
            add_up(
                add_up(curvature_sphere, logarithmic_gradient),
                correction_gradient,
            ),
            multiply_down(certificate.radius_lower, minimum_scale),
        );
        let image_scale = divide_up(
            multiply_up(
                multiply_up(certificate.radius_upper, add_up(1.0, c1)),
                certificate.p_max,
            ),
            multiply_down(radii.min_radius, spherical_denominator),
        );
        Some((curvature, image_scale))
    }

    pub(super) fn cheap_ups_bound(
        radii: &EllipsoidRadii,
        certificate: UpsCertificate,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        let (latitude0, latitude1) = toward_pole(certificate.pole, latitude0, latitude1);
        let (chi_lower, _chi_upper) =
            gaussian_latitude_interval(latitude0, latitude1, certificate.p_min, certificate.p_max);
        let (scale_upper, gradient_upper) = if chi_lower >= 0.0 {
            let delta_north_upper = subtract_up(round_up(std::f64::consts::FRAC_PI_2), chi_lower);
            let half_delta = divide_up(delta_north_upper, 2.0);
            let cosine_lower =
                subtract_down(1.0, divide_up(multiply_up(half_delta, half_delta), 2.0));
            if cosine_lower <= 0.0 {
                return f64::INFINITY;
            }
            (
                divide_up(1.0, multiply_down(cosine_lower, cosine_lower)),
                divide_up(half_delta, cosine_lower),
            )
        } else {
            let antipode_colatitude_lower =
                add_down(round_down(std::f64::consts::FRAC_PI_2), chi_lower);
            if antipode_colatitude_lower <= 0.0 {
                return f64::INFINITY;
            }
            (
                divide_up(
                    multiply_up(std::f64::consts::PI, std::f64::consts::PI),
                    multiply_down(antipode_colatitude_lower, antipode_colatitude_lower),
                ),
                divide_up(2.0, antipode_colatitude_lower),
            )
        };
        let (near_abs, far_abs) = latitude_absolute_range(latitude0, latitude1);
        let delta_far_lower =
            subtract_down(round_down(std::f64::consts::FRAC_PI_2), far_abs).max(0.0);
        if delta_far_lower == 0.0 {
            return f64::INFINITY;
        }
        let curvature_sphere = add_up(
            divide_up(
                GAUSSIAN_TANGENTIAL_CURVATURE,
                multiply_down(certificate.p_min, delta_far_lower),
            ),
            divide_up(
                multiply_up(GAUSSIAN_LATITUDE_ACCELERATION, certificate.p2_max),
                multiply_down(certificate.p_min, certificate.p_min),
            ),
        );
        let curvature = divide_up(
            add_up(curvature_sphere, gradient_upper),
            certificate.radius_lower,
        );
        let image_scale = divide_up(
            multiply_up(
                multiply_up(certificate.radius_upper, scale_upper),
                certificate.p_max,
            ),
            radii.min_radius,
        );
        let coefficient = divide_up(
            multiply_up(curvature, multiply_up(image_scale, image_scale)),
            8.0,
        );
        let cosine_latitude_upper =
            round_up(round_up(std::f64::consts::FRAC_PI_2) - near_abs).min(1.0);
        let east = multiply_up(
            multiply_up(
                multiply_up(radii.n_max, cosine_latitude_upper),
                round_up(longitude_delta.abs()),
            ),
            1.0,
        );
        let north = multiply_up(radii.m_max, absolute_difference_upper(latitude0, latitude1));
        add_up(
            multiply_up(coefficient, multiply_up(east, east)),
            multiply_up(coefficient, multiply_up(north, north)),
        )
    }

    pub(super) fn precise_ups_bound(
        radii: &EllipsoidRadii,
        certificate: UpsCertificate,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        let (latitude0, latitude1) = toward_pole(certificate.pole, latitude0, latitude1);
        let (chi_lower, chi_upper) =
            gaussian_latitude_interval(latitude0, latitude1, certificate.p_min, certificate.p_max);
        if chi_lower < 0.0 {
            return f64::INFINITY;
        }
        let denominator_lower = add_down(1.0, sine_lower(chi_lower));
        let denominator_upper = add_up(1.0, sine_upper(chi_upper));
        if denominator_lower <= 0.0 {
            return f64::INFINITY;
        }
        let scale_min_lower = divide_down(2.0, denominator_upper);
        let scale_max_upper = divide_up(2.0, denominator_lower);
        let gradient_upper = divide_up(cosine_upper(chi_lower), denominator_lower);
        let (_, chi_abs_upper) = absolute_interval_extrema(chi_lower, chi_upper);
        let (_, far_abs) = latitude_absolute_range(latitude0, latitude1);
        let delta_far_lower =
            subtract_down(round_down(std::f64::consts::FRAC_PI_2), far_abs).max(0.0);
        if delta_far_lower == 0.0 {
            return f64::INFINITY;
        }
        let curvature_sphere = longitude_curvature(
            certificate.p_min,
            certificate.p2_max,
            longitude_delta,
            latitude0,
            latitude1,
            chi_abs_upper,
        );
        let curvature = divide_up(
            add_up(curvature_sphere, gradient_upper),
            multiply_down(certificate.radius_lower, scale_min_lower),
        );
        let image_scale = divide_up(
            multiply_up(
                multiply_up(certificate.radius_upper, scale_max_upper),
                certificate.p_max,
            ),
            ellipsoid_interval_radii(radii, latitude0, latitude1).min_radius,
        );
        let length2 =
            source_length_upper_squared(radii, latitude0, latitude1, longitude_delta, false);
        divide_up(
            multiply_up(
                multiply_up(curvature, multiply_up(image_scale, image_scale)),
                length2,
            ),
            8.0,
        )
    }

    fn toward_pole(pole: Pole, latitude0: f64, latitude1: f64) -> (f64, f64) {
        match pole {
            Pole::North => (latitude0, latitude1),
            Pole::South => (-latitude0, -latitude1),
        }
    }

    fn longitude_curvature(
        p_min: f64,
        p2_max: f64,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
        chi_abs_upper: f64,
    ) -> f64 {
        if longitude_delta == 0.0 {
            return 0.0;
        }
        let tangent_upper = tangent_upper(chi_abs_upper);
        if !tangent_upper.is_finite() {
            return f64::INFINITY;
        }
        if latitude0 == latitude1 {
            return tangent_upper;
        }
        let v = absolute_difference_upper(latitude0, latitude1);
        let tangential = multiply_up(GAUSSIAN_TANGENTIAL_CURVATURE, tangent_upper);
        let orientation = divide_up(
            multiply_up(2.0, round_up(longitude_delta.abs())),
            multiply_down(p_min, v),
        );
        add_up(
            tangential.min(orientation),
            divide_up(
                multiply_up(GAUSSIAN_LATITUDE_ACCELERATION, p2_max),
                multiply_down(p_min, p_min),
            ),
        )
    }

    #[cfg(test)]
    mod tests {
        use super::super::{LocalProjectionCertificate, ProjectionCertificate};
        use super::*;

        #[test]
        fn long_parallel_certificate_bounds_offset_endpoint_direction() {
            let state = crate::crs::runtime_state_snapshot().unwrap();
            let frame = crate::crs::info_with_state("EPSG:4326", &state)
                .unwrap()
                .geographic_frame()
                .unwrap();
            let geodesic = crate::crs::geodesic_for_crs_at("EPSG:4326", &state).unwrap();
            let local = crate::crs::local::LocalCrsDefinition {
                definition: "EPSG:32631".to_owned(),
                display_name: None,
            };
            let certificate =
                LocalProjectionCertificate::from_selected_frame(&local, &geodesic, frame, &state)
                    .unwrap();
            let ProjectionCertificate::Tmerc(projection) = certificate.projection else {
                panic!("UTM uses Transverse Mercator");
            };
            let west = -0.463_147_336_714_631_5;
            let east = 6.463_147_336_714_631_5;
            let corners = [
                crate::geometry::Point::new_raw_xy(west, 39.0),
                crate::geometry::Point::new_raw_xy(east, 39.0),
                crate::geometry::Point::new_raw_xy(east, 39.2),
                crate::geometry::Point::new_raw_xy(west, 39.2),
                crate::geometry::Point::new_raw_xy(west, 39.0),
            ];
            for &[start, end] in corners.array_windows::<2>() {
                let edge = certificate.chart_edge(start, end).unwrap();
                let (curvature, image_scale) = tmerc_curvature_and_scale(
                    &certificate.radii,
                    &projection,
                    edge.longitude_from_central_meridian,
                    edge.longitude_delta,
                    edge.latitude0,
                    edge.latitude1,
                )
                .unwrap();
                // These analytic envelopes bound the independent public oracle:
                // K*l bounds tangent rotation, and K*l²/8 bounds chord error.
                assert!((0.0..=2.0e-7).contains(&curvature));
                assert!(image_scale <= 1.02);
                if start.y == end.y {
                    assert!(curvature >= 1.0e-7);
                    let length2 = source_length_upper_squared(
                        &certificate.radii,
                        edge.latitude0,
                        edge.latitude1,
                        edge.longitude_delta,
                        false,
                    );
                    for budget in [0.1, 1.0] {
                        let count =
                            certificate.piece_count(start, end, budget).unwrap().get() as f64;
                        let image_piece_length = image_scale * length2.sqrt() / count;
                        assert!(image_piece_length <= (8.0 * budget / 1.0e-7).sqrt());
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
struct LocalExtent {
    longitude: f64,
    latitude: f64,
    samples: Vec<(f64, f64)>,
}

impl LocalExtent {
    fn from_shapes(
        shapes: &[&Shape],
        source_crs: &str,
        state: &RuntimeState,
    ) -> Result<Option<Self>> {
        let mut sources = Vec::with_capacity(shapes.len());
        for &shape in shapes {
            if matches!(shape, Shape::GeometryCollection(_)) && shape.has_repeated_shared_owner() {
                sources.push(Cow::Owned(shape.distinct_point_cloud()?));
            } else {
                sources.push(Cow::Borrowed(shape));
            }
        }
        let mut counts = ShapeCountQuery::default();
        let coordinate_count =
            counts.coordinates_for_shapes(sources.iter().map(|shape| shape.as_ref()))?;
        drop(counts);
        let capacity = if coordinate_count == 0 {
            0
        } else {
            coordinate_count
                .checked_add(9)
                .ok_or(crate::error::ResourceError::SizeOverflow)?
        };
        let capacity = admit_shape_vector_capacity::<(f64, f64)>(capacity)?;
        let mut points = Vec::with_capacity(capacity);
        for shape in &sources {
            let shape = shape.as_ref();
            let lonlat: Cow<'_, Shape> = if is_wgs84_lonlat(source_crs) {
                Cow::Borrowed(shape)
            } else {
                Cow::Owned(transform_at(shape, source_crs, "EPSG:4326", state)?)
            };
            lonlat.for_each_point(|point| {
                points.push((normalize_longitude(point.x), point.y));
            });
        }

        if points.is_empty() {
            return Ok(None);
        }
        if points
            .iter()
            .any(|(lon, lat)| !lon.is_finite() || !lat.is_finite() || !(-90.0..=90.0).contains(lat))
        {
            return Err(CrsError::invalid(
                "local CRS estimation requires finite longitude/latitude coordinates within the geographic domain",
            ));
        }

        let mut longitudes: Vec<f64> = points.iter().map(|(lon, _)| *lon).collect();
        let (west, east, longitude) = circular_longitude_extent(&mut longitudes);
        let (min_lat, max_lat) = points.iter().fold(
            (f64::INFINITY, f64::NEG_INFINITY),
            |(min_lat, max_lat), (_, lat)| (min_lat.min(*lat), max_lat.max(*lat)),
        );
        let latitude = f64::midpoint(min_lat, max_lat);

        // Vertices catch the real data. Corners and edge midpoints catch the
        // interior extrema of sparse long segments and make the error promise
        // independent of vertex density.
        let longitude_edges = [west, longitude, east];
        let latitude_edges = [min_lat, latitude, max_lat];
        for lon in longitude_edges {
            for lat in latitude_edges {
                points.push((normalize_longitude(lon), lat));
            }
        }
        points.sort_unstable_by(|left, right| {
            left.0.total_cmp(&right.0).then(left.1.total_cmp(&right.1))
        });
        points.dedup_by(|left, right| {
            left.0.to_bits() == right.0.to_bits() && left.1.to_bits() == right.1.to_bits()
        });
        Ok(Some(Self {
            longitude,
            latitude,
            samples: points,
        }))
    }
}

fn circular_longitude_extent(longitudes: &mut Vec<f64>) -> (f64, f64, f64) {
    longitudes.sort_unstable_by(f64::total_cmp);
    longitudes.dedup_by(|left, right| left.total_cmp(right).is_eq());
    if let [longitude] = longitudes.as_slice() {
        return (*longitude, *longitude, *longitude);
    }
    let first = *longitudes
        .first()
        .expect("a multi-coordinate extent has a first longitude");
    let last = *longitudes
        .last()
        .expect("a multi-coordinate extent has a last longitude");
    let mut largest_gap = f64::NEG_INFINITY;
    let mut west = first;
    let mut east = last;
    for &[left, right] in longitudes.array_windows::<2>() {
        let gap = right - left;
        if gap > largest_gap {
            largest_gap = gap;
            west = right;
            east = left;
        }
    }
    let seam_next = first + 360.0;
    let seam_gap = seam_next - last;
    if seam_gap > largest_gap {
        west = first;
        east = last;
    }
    if east < west {
        east += 360.0;
    }
    let midpoint = f64::midpoint(west, east);
    (west, east, normalize_longitude(midpoint))
}

fn candidate_fits(candidate: &str, samples: &[(f64, f64)], state: &RuntimeState) -> Result<bool> {
    for &(longitude, latitude) in samples {
        let factor = factors_at(candidate, longitude, latitude, false, state)?;
        let error = (factor.meridional_scale - 1.0)
            .abs()
            .max((factor.parallel_scale - 1.0).abs());
        if !error.is_finite() || error > LOCAL_SCALE_ERROR_LIMIT {
            return Ok(false);
        }
    }
    Ok(true)
}

fn best_custom_candidate(
    source_crs: &str,
    extent: &LocalExtent,
    state: &RuntimeState,
) -> Result<LocalCrsDefinition> {
    let info = info_with_state(source_crs, state)?;
    let base = if info.kind.starts_with("geographic") {
        source_crs
    } else {
        info.geodetic_crs.as_ref().map_or(source_crs, |geodetic| {
            geodetic
                .definition
                .as_deref()
                .unwrap_or(geodetic.crs.as_str())
        })
    };
    let mut base_crs: serde_json::Value = serde_json::from_str(&to_projjson_with_options_at(
        base,
        &crate::crs::CrsProjJsonOptions::default(),
        state,
    )?)
    .map_err(|e| {
        CrsError::invalid(format!(
            "could not preserve the source datum in a local projected CRS: {e}"
        ))
    })?;
    base_crs
        .as_object_mut()
        .ok_or_else(|| CrsError::invalid("source geodetic CRS did not export as an object"))?
        .remove("$schema");

    let candidate = serde_json::json!({
        "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
        "type": "ProjectedCRS",
        "name": LOCAL_CRS_DISPLAY_NAME,
        "base_crs": base_crs,
        "conversion": {
            "name": "Local Transverse Mercator",
            "method": {"name": "Transverse Mercator", "id": {"authority": "EPSG", "code": 9807}},
            "parameters": [
                {"name": "Latitude of natural origin", "value": extent.latitude, "unit": "degree", "id": {"authority": "EPSG", "code": 8801}},
                {"name": "Longitude of natural origin", "value": extent.longitude, "unit": "degree", "id": {"authority": "EPSG", "code": 8802}},
                {"name": "Scale factor at natural origin", "value": 1.0, "unit": "unity", "id": {"authority": "EPSG", "code": 8805}},
                {"name": "False easting", "value": 0.0, "unit": "metre", "id": {"authority": "EPSG", "code": 8806}},
                {"name": "False northing", "value": 0.0, "unit": "metre", "id": {"authority": "EPSG", "code": 8807}}
            ]
        },
        "coordinate_system": {
            "subtype": "Cartesian",
            "axis": [
                {"name": "Easting", "abbreviation": "E", "direction": "east", "unit": "metre"},
                {"name": "Northing", "abbreviation": "N", "direction": "north", "unit": "metre"}
            ]
        }
    })
    .to_string();
    let candidates = [candidate];
    for candidate in candidates {
        if candidate_fits(&candidate, &extent.samples, state)? {
            return Ok(LocalCrsDefinition::custom(
                normalize(&candidate)?.into_owned(),
            ));
        }
    }
    Err(CrsError::invalid(
        "no safe local conformal projection found",
    ))
}

#[derive(Debug, Clone, Copy)]
struct LocalProjectionAnchor {
    longitude: f64,
    latitude: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UtmZone(u8);

impl UtmZone {
    pub(crate) fn try_new(zone: u32) -> Result<Self> {
        if (1..=60).contains(&zone) {
            Ok(Self(u8::try_from(zone).expect("UTM zone <= 60 fits u8")))
        } else {
            Err(CrsError::invalid(format!(
                "UTM zone must be between 1 and 60, got {zone}"
            )))
        }
    }

    pub(crate) const fn get(self) -> u8 {
        self.0
    }
}

fn best_utm_from_anchor(
    local: LocalProjectionAnchor,
    crs: &str,
    state: &RuntimeState,
) -> Result<Option<String>> {
    let zone = utm_zone(local.longitude, local.latitude);
    let pole = if local.latitude >= 0.0 {
        Pole::North
    } else {
        Pole::South
    };
    if has_wgs84_datum_fast_path(crs) {
        return Ok(Some(wgs84_utm_crs(zone, pole)));
    }
    let info = info_with_state(crs, state)?;
    let Some(source_datum) = datum_key(&info) else {
        return Ok(None);
    };
    best_utm_from_catalog(
        &source_datum,
        local.longitude,
        local.latitude,
        zone,
        pole,
        state,
    )
}

fn has_wgs84_datum_fast_path(crs: &str) -> bool {
    is_wgs84_lonlat(crs) || parse_epsg(crs) == Some(3857) || utm_crs(crs).is_some()
}

fn best_utm_from_catalog(
    source_datum: &DatumKey,
    lon: f64,
    lat: f64,
    zone: UtmZone,
    pole: Pole,
    state: &RuntimeState,
) -> Result<Option<String>> {
    let items = catalog_with_state(
        Some("EPSG"),
        &CrsCatalogOptions {
            kind: Some(CrsObjectKind::ProjectedCrs),
            area: Some(AreaOfInterest {
                west: lon,
                south: lat,
                east: lon,
                north: lat,
            }),
            contains_area: false,
            allow_deprecated: false,
            celestial_body: Some("Earth".to_owned()),
        },
        state,
    )?;
    let mut best: Option<String> = None;
    for item in items {
        if item.projection_method_name.as_deref() != Some("Transverse Mercator") {
            continue;
        }
        let info = info_with_state(&item.crs, state)?;
        if datum_key(&info).as_ref() != Some(source_datum) {
            continue;
        }
        if !is_east_north_metre_axes(&info) {
            continue;
        }
        if !is_utm_operation(&info, zone, pole) {
            continue;
        }
        if best
            .as_ref()
            .is_none_or(|current| utm_survivor_rank(&item.crs) < utm_survivor_rank(current))
        {
            best = Some(item.crs);
        }
    }
    Ok(best)
}

fn utm_survivor_rank(crs: &str) -> (u32, &str) {
    (parse_epsg(crs).unwrap_or(u32::MAX), crs)
}

#[expect(
    clippy::float_cmp,
    reason = "metre axes are the conversion-factor identity 1, not an approximate linear unit"
)]
fn is_east_north_metre_axes(info: &CrsInfo) -> bool {
    let [east, north] = info.axes.as_slice() else {
        return false;
    };
    east.direction.as_deref() == Some("east")
        && north.direction.as_deref() == Some("north")
        && east.unit_conversion_factor == 1.0
        && north.unit_conversion_factor == 1.0
}

#[expect(
    clippy::float_cmp,
    reason = "projected metre axes require the exact unit-conversion identity 1"
)]
fn is_projected_metre_cartesian_axes(info: &CrsInfo) -> bool {
    if !info.is_projected() || info.coordinate_system != Some("cartesian") {
        return false;
    }
    let [first, second] = info.axes.as_slice() else {
        return false;
    };
    first.unit_conversion_factor == 1.0 && second.unit_conversion_factor == 1.0
}

fn is_utm_operation(info: &CrsInfo, zone: UtmZone, pole: Pole) -> bool {
    let Some(operation) = info.coordinate_operation.as_ref() else {
        return false;
    };
    let Some(method) = operation.method.as_ref() else {
        return false;
    };
    if method.authority.as_deref() != Some("EPSG") || method.code.as_deref() != Some("9807") {
        return false;
    }
    let expected_lon0 = f64::from(zone.get()) * 6.0 - 183.0;
    let expected_northing = match pole {
        Pole::North => 0.0,
        Pole::South => 10_000_000.0,
    };
    angular_param_matches_degrees(operation, "8801", 0.0)
        && angular_param_matches_degrees(operation, "8802", expected_lon0)
        && linear_or_unity_param_matches(operation, "8805", ParamUnit::Unity, super::UTM_K0)
        && linear_or_unity_param_matches(operation, "8806", ParamUnit::Metre, 500_000.0)
        && linear_or_unity_param_matches(operation, "8807", ParamUnit::Metre, expected_northing)
}

#[expect(
    clippy::float_cmp,
    clippy::float_cmp_const,
    reason = "UTM origin identity is the stored degree spelling and the exact radian conversion factor, not a nearby angle"
)]
fn angular_param_matches_degrees(
    operation: &CrsCoordinateOperationInfo,
    code: &str,
    expected_degrees: f64,
) -> bool {
    let Some(param) = unique_epsg_param(operation, code) else {
        return false;
    };
    param.value.is_finite()
        && param.unit_conversion_factor == super::DEGREE_TO_RADIAN
        && param.value == expected_degrees
}

fn linear_or_unity_param_matches(
    operation: &CrsCoordinateOperationInfo,
    code: &str,
    unit: ParamUnit,
    expected: f64,
) -> bool {
    let Some(param) = unique_epsg_param(operation, code) else {
        return false;
    };
    parse_param_value(param, unit) == Some(expected)
}

fn unique_epsg_param<'a>(
    operation: &'a CrsCoordinateOperationInfo,
    code: &str,
) -> Option<&'a OperationParameterInfo> {
    let mut found = None;
    for param in &operation.parameters {
        if param.authority.as_deref() != Some("EPSG") || param.code.as_deref() != Some(code) {
            continue;
        }
        if found.is_some() {
            return None;
        }
        if !param.unit_conversion_factor.is_finite() || param.unit_conversion_factor <= 0.0 {
            return None;
        }
        found = Some(param);
    }
    found
}

fn local_projection_parameter(
    operation: &CrsCoordinateOperationInfo,
    code: &str,
    unit: ParamUnit,
) -> Option<f64> {
    parse_param_value(unique_epsg_param(operation, code)?, unit)
}

fn wgs84_utm_crs(zone: UtmZone, pole: Pole) -> String {
    let base: u32 = match pole {
        Pole::North => 32600,
        Pole::South => 32700,
    };
    format!("EPSG:{}", base.wrapping_add(u32::from(zone.get())))
}

/// Clockwise bearing in `[0, 360)` from an azimuth in `(-180, 180]`, the range
/// `atan2` and geographiclib produce. There the branch is bit-identical to
/// `rem_euclid(360)` without the `fmod`.
pub(super) fn bearing_from_azimuth(azimuth: f64) -> f64 {
    if azimuth < 0.0 {
        azimuth + 360.0
    } else {
        azimuth
    }
}

pub(super) fn normalize_longitude(longitude: f64) -> f64 {
    let shifted = longitude + 180.0;
    let normalized = if (0.0..360.0).contains(&shifted) {
        shifted - 180.0
    } else {
        shifted.rem_euclid(360.0) - 180.0
    };
    if normalized.to_bits() == (-180.0_f64).to_bits() {
        180.0
    } else {
        normalized
    }
}

fn utm_zone(lon: f64, lat: f64) -> UtmZone {
    if (56.0..64.0).contains(&lat) && (3.0..12.0).contains(&lon) {
        return UtmZone::try_new(32).expect("literal UTM zone is valid");
    }
    if (72.0..84.0).contains(&lat) {
        if (0.0..9.0).contains(&lon) {
            return UtmZone::try_new(31).expect("literal UTM zone is valid");
        }
        if (9.0..21.0).contains(&lon) {
            return UtmZone::try_new(33).expect("literal UTM zone is valid");
        }
        if (21.0..33.0).contains(&lon) {
            return UtmZone::try_new(35).expect("literal UTM zone is valid");
        }
        if (33.0..42.0).contains(&lon) {
            return UtmZone::try_new(37).expect("literal UTM zone is valid");
        }
    }
    let zone = (((lon + 180.0) / 6.0).floor().clamp(0.0, 59.0) as u32).wrapping_add(1);
    UtmZone::try_new(zone).expect("clamped UTM zone is valid")
}

#[cfg(test)]
mod metadata_tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};

    fn epsg_param(code: &str, value: f64, factor: f64) -> OperationParameterInfo {
        OperationParameterInfo {
            name: None,
            authority: Some("EPSG".to_owned()),
            code: Some(code.to_owned()),
            value,
            value_string: None,
            unit_conversion_factor: factor,
            unit_name: None,
            unit_authority: None,
            unit_code: None,
            unit_category: None,
        }
    }

    fn operation(parameters: Vec<OperationParameterInfo>) -> CrsCoordinateOperationInfo {
        CrsCoordinateOperationInfo {
            name: None,
            definition: None,
            description: None,
            accuracy: None,
            has_inverse: false,
            has_ballpark_transformation: false,
            requires_coordinate_epoch: false,
            instantiable: true,
            method: None,
            parameters,
            grids: Vec::new(),
            steps: Vec::new(),
            area_of_use: None,
        }
    }

    #[expect(
        clippy::float_cmp,
        reason = "the fixture hunts bit-adjacent degree spellings whose radian products collide; a tolerance would hide the identity the matcher must reject"
    )]
    #[test]
    fn stored_degree_equality_rejects_adjacent_floats_that_share_a_radian_product() {
        let factor = super::super::DEGREE_TO_RADIAN;
        let op = |code: &str, value: f64| operation(vec![epsg_param(code, value, factor)]);
        assert!(angular_param_matches_degrees(
            &op("8802", 15.0),
            "8802",
            15.0
        ));
        assert!(angular_param_matches_degrees(&op("8801", 0.0), "8801", 0.0));
        for expected in [-117.0_f64, -15.0, 15.0, 123.0] {
            let up = expected.next_up();
            let down = expected.next_down();
            let adjacent = if up * factor == expected * factor {
                up
            } else if down * factor == expected * factor {
                down
            } else {
                panic!("{expected} has no adjacent float with the same radian product");
            };
            assert_ne!(adjacent, expected);
            assert!(!angular_param_matches_degrees(
                &op("8802", adjacent),
                "8802",
                expected
            ));
        }
        let min_sub = f64::from_bits(1);
        assert_eq!(min_sub * factor, 0.0);
        assert!(!angular_param_matches_degrees(
            &op("8801", min_sub),
            "8801",
            0.0
        ));
    }

    #[test]
    fn invalid_matched_param_is_not_replaced_by_a_later_duplicate() {
        let op = operation(vec![
            epsg_param("8802", 9.0, f64::NAN),
            epsg_param("8802", 9.0, super::super::DEGREE_TO_RADIAN),
        ]);
        assert!(unique_epsg_param(&op, "8802").is_none());
        assert!(!angular_param_matches_degrees(&op, "8802", 9.0));
    }

    #[test]
    fn custom_candidate_mapping_preserves_resource_errors_before_formatting() {
        let error = Error::from(ResourceError::SizeOverflow);
        let mapped = best_custom_candidate_error(error);
        assert!(matches!(
            mapped.kind(),
            ErrorKind::Resource(ResourceError::SizeOverflow)
        ));
    }
}

#[cfg(test)]
mod datum_preserving_tests {
    use super::{
        LocalCrsDefinition, LocalProjectionCertificate, ensure_datum_preserving_local_operation,
        estimate_local_crs_at,
    };
    use crate::crs::runtime_state_snapshot;
    use crate::geographic::GeographicFrame;
    use crate::geometry::{CoordSeq, LineSeq, Point, Shape, XY};

    struct Case {
        name: &'static str,
        crs: &'static str,
        point: (f64, f64),
        expected_authority: Option<&'static str>,
    }

    #[test]
    fn selected_local_frames_preserve_the_source_datum() {
        let state = runtime_state_snapshot().unwrap();
        let cases = [
            Case {
                name: "WGS 84 UTM",
                crs: "EPSG:4326",
                point: (-74.0, 40.0),
                expected_authority: Some("EPSG:32618"),
            },
            Case {
                name: "WGS 84 3D horizontal projection",
                crs: "EPSG:4979",
                point: (-74.0, 40.0),
                expected_authority: Some("EPSG:32618"),
            },
            Case {
                name: "NAD83 datum-matched UTM",
                crs: "EPSG:4269",
                point: (-74.0, 41.0),
                expected_authority: Some("EPSG:26918"),
            },
            Case {
                name: "NAD83 polar custom TM",
                crs: "EPSG:4269",
                point: (0.0, -81.0),
                expected_authority: None,
            },
            Case {
                name: "NTF Paris grads custom TM",
                crs: "EPSG:4807",
                point: (0.0, 55.5),
                expected_authority: None,
            },
            Case {
                name: "ETRS89 datum-matched UTM",
                crs: "EPSG:4258",
                point: (10.0, 51.0),
                expected_authority: Some("EPSG:25832"),
            },
            Case {
                name: "GDA2020 datum-matched UTM",
                crs: "EPSG:7844",
                point: (151.2, -33.9),
                expected_authority: Some("EPSG:7856"),
            },
            Case {
                name: "authalic sphere custom TM",
                crs: "EPSG:4047",
                point: (0.0, 0.0),
                expected_authority: None,
            },
            Case {
                name: "WGS 84 polar UPS",
                crs: "EPSG:4326",
                point: (0.0, -81.0),
                expected_authority: Some("EPSG:5042"),
            },
        ];

        for case in cases {
            let shape = Shape::Point(Point::new_raw_xy(case.point.0, case.point.1));
            let local = estimate_local_crs_at(&[&shape], case.crs, &state)
                .unwrap_or_else(|error| panic!("{}: {error}", case.name));
            ensure_datum_preserving_local_operation(case.crs, &local.definition, &state)
                .unwrap_or_else(|error| {
                    panic!("{} selected {}: {error}", case.name, local.definition)
                });
            match case.expected_authority {
                Some(expected) => assert_eq!(local.definition, expected, "{}", case.name),
                None => assert_eq!(
                    local.display_name,
                    Some(super::LOCAL_CRS_DISPLAY_NAME),
                    "{}",
                    case.name,
                ),
            }
        }
    }

    #[test]
    fn pipeline_owner_rejects_the_previous_cross_datum_fallbacks() {
        let state = runtime_state_snapshot().unwrap();
        for (source, previous_target) in [("EPSG:4807", "EPSG:32631"), ("EPSG:4269", "EPSG:5042")] {
            assert!(
                ensure_datum_preserving_local_operation(source, previous_target, &state).is_err(),
                "{source} → {previous_target} must not hide a datum operation",
            );
        }
    }

    #[test]
    fn analytic_piece_counts_bound_the_600_km_parallel_at_both_budgets() {
        let west = -0.463_147_336_714_631_5;
        let east = 6.463_147_336_714_631_5;
        let points = CoordSeq::try_from_xy(&[
            XY::new(west, 39.0),
            XY::new(east, 39.0),
            XY::new(east, 39.2),
            XY::new(west, 39.2),
            XY::new(west, 39.0),
        ])
        .unwrap();
        let shape = Shape::LineString(LineSeq::try_new(points).unwrap());
        let state = runtime_state_snapshot().unwrap();
        let local = estimate_local_crs_at(&[&shape], "EPSG:4326", &state).unwrap();
        assert_eq!(local.definition, "EPSG:32631");
        let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
        let certificate = LocalProjectionCertificate::from_selected_frame(
            &local,
            &geodesic,
            GeographicFrame::crs84(),
            &state,
        )
        .unwrap();

        assert_eq!(
            certificate
                .piece_count(
                    Point::new_raw_xy(west, 39.0),
                    Point::new_raw_xy(east, 39.0),
                    0.1,
                )
                .unwrap()
                .get(),
            253
        );
        assert_eq!(
            certificate
                .piece_count(
                    Point::new_raw_xy(west, 39.0),
                    Point::new_raw_xy(east, 39.0),
                    1.0,
                )
                .unwrap()
                .get(),
            80
        );
    }

    #[test]
    fn arithmetic_screen_scans_straight_edges_without_allocating() {
        let points =
            CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(0.1, 0.0), XY::new(0.2, 0.0)])
                .unwrap();
        let shape = Shape::LineString(LineSeq::try_new(points.clone()).unwrap());
        let state = runtime_state_snapshot().unwrap();
        let local = estimate_local_crs_at(&[&shape], "EPSG:4326", &state).unwrap();
        let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
        let certificate = LocalProjectionCertificate::from_selected_frame(
            &local,
            &geodesic,
            GeographicFrame::crs84(),
            &state,
        )
        .unwrap();

        let (needs_refinement, allocations) = crate::test_support::allocation_count(|| {
            certificate.has_edges_to_refine(&points, 0.1).unwrap()
        });
        assert!(!needs_refinement);
        assert_eq!(allocations, 0);
    }

    #[test]
    fn spherical_transverse_mercator_has_finite_analytic_piece_counts() {
        let points = CoordSeq::try_from_xy(&[XY::new(-0.2, -0.1), XY::new(0.2, 0.2)]).unwrap();
        let shape = Shape::LineString(LineSeq::try_new(points).unwrap());
        let state = runtime_state_snapshot().unwrap();
        let local = estimate_local_crs_at(&[&shape], "EPSG:4047", &state).unwrap();
        let geodesic = geographiclib_rs::Geodesic::new(6_371_007.0, 0.0);
        let certificate = LocalProjectionCertificate::from_selected_frame(
            &local,
            &geodesic,
            GeographicFrame::crs84(),
            &state,
        )
        .unwrap();

        assert!(
            certificate
                .piece_count(
                    Point::new_raw_xy(-0.2, -0.1),
                    Point::new_raw_xy(0.2, 0.2),
                    0.1,
                )
                .unwrap()
                .get()
                > 1
        );
    }

    #[test]
    fn ups_pole_parallel_is_recognized_in_native_latitude_units() {
        let state = runtime_state_snapshot().unwrap();
        let local = LocalCrsDefinition::authority("EPSG:5042".to_owned());
        let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
        let certificate = LocalProjectionCertificate::from_selected_frame(
            &local,
            &geodesic,
            GeographicFrame::crs84(),
            &state,
        )
        .unwrap();

        assert_eq!(
            certificate
                .piece_count(
                    Point::new_raw_xy(0.0, -90.0),
                    Point::new_raw_xy(10.0, -90.0),
                    0.1,
                )
                .unwrap()
                .get(),
            1
        );
    }
}
