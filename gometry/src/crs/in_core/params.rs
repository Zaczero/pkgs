//! Param-complete framed projection admission — kernels declare every EPSG
//! operation parameter they consume; offsets live in [`ProjectionFrame`].

use crate::crs::in_core::adjlon;
use crate::crs::in_core::kernel::{DEG_TO_RAD, ProjectionKernel, RAD_TO_DEG};
use crate::crs::{CrsCoordinateOperationInfo, DEGREE_TO_RADIAN, OperationParameterInfo};
use crate::error::Result;
use crate::numeric::AxisScale;

pub(super) const EPS10: f64 = 1.0e-10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::crs) struct EpsgParam {
    pub code: &'static str,
}

pub(super) const fn epsg(code: &'static str) -> EpsgParam {
    EpsgParam { code }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::crs) enum ParamUnit {
    Degrees,
    Metre,
    Unity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::crs) enum Requirement {
    Required,
    Optional,
}

impl Requirement {
    pub(in crate::crs) const fn is_required(self) -> bool {
        matches!(self, Self::Required)
    }
}

#[derive(Clone, Copy, Debug)]
pub(in crate::crs) enum ParamAction<P: Copy + 'static> {
    Field(fn(&mut P, f64) -> bool),
    Assume { expected: f64, tol: f64 },
}

#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct ParamSpec<P: Copy + 'static> {
    pub id: EpsgParam,
    pub unit: ParamUnit,
    pub action: ParamAction<P>,
    pub required: Requirement,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct FrameSpec {
    pub lon_0: EpsgParam,
    pub x_0: EpsgParam,
    pub y_0: EpsgParam,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct MethodSpec {
    pub code: &'static str,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct OperationSpec<P: Copy + 'static> {
    pub method: MethodSpec,
    pub frame: FrameSpec,
    pub defaults: P,
    pub params: &'static [ParamSpec<P>],
    pub finalize: fn(P) -> Option<P>,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct ProjectionFrame {
    pub lon_0_rad: f64,
    pub x_0: f64,
    pub y_0: f64,
    pub axis_scale: AxisScale,
}

impl ProjectionFrame {
    pub(super) const ZERO: Self = Self {
        lon_0_rad: 0.0,
        x_0: 0.0,
        y_0: 0.0,
        axis_scale: AxisScale::IDENTITY,
    };
}

pub(super) const fn apply_false_origin(value: f64, false_origin: f64) -> f64 {
    if value == 0.0 && false_origin == 0.0 {
        value
    } else {
        value + false_origin
    }
}

#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct Projected<K: ProjectionKernel> {
    pub frame: ProjectionFrame,
    pub setup: K::Setup,
}

impl<K: ProjectionKernel> Projected<K> {
    pub(super) const fn with_axis_scale(mut self, axis_scale: AxisScale) -> Self {
        self.frame.axis_scale = axis_scale;
        self
    }

    pub(super) fn forward_xy(&self, lon_deg: f64, lat_deg: f64) -> Result<(f64, f64)> {
        super::validate_geographic_coordinate(lon_deg, lat_deg)?;
        let lam_rel = lon_deg * DEG_TO_RAD - self.frame.lon_0_rad;
        let (x, y) = K::forward_unframed(&self.setup, lam_rel, lat_deg * DEG_TO_RAD)?;
        Ok((
            apply_false_origin(x, self.frame.x_0) * self.frame.axis_scale.inverse(),
            apply_false_origin(y, self.frame.y_0) * self.frame.axis_scale.inverse(),
        ))
    }

    pub(super) fn inverse_xy(&self, x: f64, y: f64) -> Result<(f64, f64)> {
        let (lam_rel, lat_rad) = K::inverse_unframed(
            &self.setup,
            self.frame.axis_scale.unframe(x, self.frame.x_0),
            self.frame.axis_scale.unframe(y, self.frame.y_0),
        )?;
        let lon_deg = adjlon(self.frame.lon_0_rad + lam_rel) * RAD_TO_DEG;
        let lat_deg = lat_rad * RAD_TO_DEG;
        Ok((lon_deg, lat_deg))
    }
}

pub(super) const fn param_field<P: Copy + 'static>(
    id: EpsgParam,
    unit: ParamUnit,
    setter: fn(&mut P, f64) -> bool,
    required: Requirement,
) -> ParamSpec<P> {
    ParamSpec {
        id,
        unit,
        action: ParamAction::Field(setter),
        required,
    }
}

pub(super) const fn param_assume_degrees<P: Copy + 'static>(
    id: EpsgParam,
    expected: f64,
    tol: f64,
    required: Requirement,
) -> ParamSpec<P> {
    ParamSpec {
        id,
        unit: ParamUnit::Degrees,
        action: ParamAction::Assume { expected, tol },
        required,
    }
}

pub(super) const fn param_assume_unity<P: Copy + 'static>(
    id: EpsgParam,
    expected: f64,
    tol: f64,
    required: Requirement,
) -> ParamSpec<P> {
    ParamSpec {
        id,
        unit: ParamUnit::Unity,
        action: ParamAction::Assume { expected, tol },
        required,
    }
}

pub(super) fn admit_projection<K: ProjectionKernel>(
    operation: &CrsCoordinateOperationInfo,
    ellipsoid: super::kernel::Ellipsoid,
) -> Option<Projected<K>> {
    let method = operation.method.as_ref()?;
    let method_code = method.code.as_deref()?;
    let spec = K::SPECS.iter().find(|s| s.method.code == method_code)?;
    admit_projection_spec::<K>(operation, ellipsoid, spec)
}

fn admit_projection_spec<K: ProjectionKernel>(
    operation: &CrsCoordinateOperationInfo,
    ellipsoid: super::kernel::Ellipsoid,
    spec: &OperationSpec<K::MethodParams>,
) -> Option<Projected<K>> {
    let mut values: Vec<(&str, f64)> = Vec::new();
    for param in &operation.parameters {
        let authority = param.authority.as_deref()?;
        let code = param.code.as_deref()?;
        if authority.is_empty() || code.is_empty() {
            return None;
        }
        if values.iter().any(|(c, _)| *c == code) {
            return None;
        }
        let declared_unit = lookup_declared_unit(spec, code)?;
        let reported_unit = classify_param_unit(param)?;
        if declared_unit != reported_unit {
            return None;
        }
        let value = parse_param_value(param, declared_unit)?;
        values.push((code, value));
    }

    let declared_codes: Vec<&str> = spec
        .params
        .iter()
        .map(|p| p.id.code)
        .chain([
            spec.frame.lon_0.code,
            spec.frame.x_0.code,
            spec.frame.y_0.code,
        ])
        .collect();

    for (code, _) in &values {
        if !declared_codes.contains(code) {
            return None;
        }
    }

    let mut frame = ProjectionFrame::ZERO;
    let mut params = spec.defaults;

    for param_spec in spec.params {
        let present = values.iter().find(|(c, _)| *c == param_spec.id.code);
        match param_spec.action {
            ParamAction::Field(setter) => {
                if let Some((_, value)) = present {
                    if !setter(&mut params, *value) {
                        return None;
                    }
                } else if param_spec.required.is_required() {
                    return None;
                }
            },
            ParamAction::Assume { expected, tol } => {
                if let Some((_, value)) = present {
                    if (value - expected).abs() > tol {
                        return None;
                    }
                } else if param_spec.required.is_required() {
                    return None;
                }
            },
        }
    }

    let frame_lon = values
        .iter()
        .find(|(c, _)| *c == spec.frame.lon_0.code)
        .map(|(_, v)| *v);
    let frame_x = values
        .iter()
        .find(|(c, _)| *c == spec.frame.x_0.code)
        .map(|(_, v)| *v);
    let frame_y = values
        .iter()
        .find(|(c, _)| *c == spec.frame.y_0.code)
        .map(|(_, v)| *v);

    frame.lon_0_rad = frame_lon? * DEG_TO_RAD;
    frame.x_0 = frame_x?;
    frame.y_0 = frame_y?;

    let params = (spec.finalize)(params)?;
    let setup = K::setup(ellipsoid, params).ok()?;
    Some(Projected { frame, setup })
}

fn lookup_declared_unit<P: Copy + 'static>(
    spec: &OperationSpec<P>,
    code: &str,
) -> Option<ParamUnit> {
    if spec.frame.lon_0.code == code {
        return Some(ParamUnit::Degrees);
    }
    if spec.frame.x_0.code == code || spec.frame.y_0.code == code {
        return Some(ParamUnit::Metre);
    }
    spec.params
        .iter()
        .find(|p| p.id.code == code)
        .map(|p| p.unit)
}

fn classify_param_unit(param: &OperationParameterInfo) -> Option<ParamUnit> {
    if param.unit_conversion_factor.is_finite()
        && (param.unit_conversion_factor - DEGREE_TO_RADIAN).abs() <= 1e-12
    {
        return Some(ParamUnit::Degrees);
    }
    if param.unit_conversion_factor.is_finite()
        && param.unit_conversion_factor > 0.0
        && (matches!(param.unit_category.as_deref(), Some("length" | "linear"))
            || param.unit_code.as_deref() == Some("9001"))
    {
        return Some(ParamUnit::Metre);
    }
    if param.unit_conversion_factor.is_finite()
        && (param.unit_conversion_factor - 1.0).abs() <= 1e-9
    {
        if matches!(
            param.unit_category.as_deref(),
            Some("length" | "linear" | "scale" | "unity" | "unitless")
        ) || param.unit_code.as_deref() == Some("9001")
            || param.unit_code.as_deref() == Some("9201")
        {
            return Some(
                if matches!(param.unit_category.as_deref(), Some("length" | "linear"))
                    || param.unit_code.as_deref() == Some("9001")
                {
                    ParamUnit::Metre
                } else {
                    ParamUnit::Unity
                },
            );
        }
        return Some(ParamUnit::Metre);
    }
    None
}

fn parse_param_value(param: &OperationParameterInfo, unit: ParamUnit) -> Option<f64> {
    if !param.value.is_finite() {
        return None;
    }
    let value = match unit {
        ParamUnit::Degrees => param.value * param.unit_conversion_factor / DEGREE_TO_RADIAN,
        ParamUnit::Metre | ParamUnit::Unity => param.value * param.unit_conversion_factor,
    };
    value.is_finite().then_some(value)
}

#[cfg(test)]
mod tests {
    use super::super::kernel::Ellipsoid;
    use super::*;

    #[derive(Clone, Copy)]
    struct IdentityKernel;

    impl ProjectionKernel for IdentityKernel {
        type Setup = ();
        type MethodParams = ();

        const SPECS: &'static [OperationSpec<Self::MethodParams>] = &[];

        fn setup(_ellipsoid: Ellipsoid, _params: Self::MethodParams) -> Result<Self::Setup> {
            Ok(())
        }

        fn forward_unframed(
            _setup: &Self::Setup,
            lam_rad: f64,
            lat_rad: f64,
        ) -> Result<(f64, f64)> {
            Ok((lam_rad, lat_rad))
        }

        fn inverse_unframed(_setup: &Self::Setup, x: f64, y: f64) -> Result<(f64, f64)> {
            Ok((x, y))
        }
    }

    #[test]
    fn non_metre_zero_origin_frame_preserves_signed_zero() {
        let scale = AxisScale::from_unit_to_metre(0.304_800_609_601_219_24).unwrap();
        let projected = Projected::<IdentityKernel> {
            frame: ProjectionFrame::ZERO,
            setup: (),
        }
        .with_axis_scale(scale);
        let (x, y) = projected.forward_xy(-0.0, -0.0).unwrap();
        assert_eq!(x.to_bits(), (-0.0_f64).to_bits());
        assert_eq!(y.to_bits(), (-0.0_f64).to_bits());
    }
}
