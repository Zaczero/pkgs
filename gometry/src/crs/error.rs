//! The CRS error kind. Variants carry their fields directly — the crate-wide
//! [`Error`](crate::error::Error) boxes the whole kind, so nothing here needs
//! per-variant payload boxing. Constructors return [`Error`] so call sites
//! build the boxed crate error in one step.

use thiserror::Error;

use crate::error::Error as CrateError;

#[derive(Debug, Error)]
pub enum CrsError {
    #[error("cannot create CRS {crs:?}: {message}")]
    Create { crs: Box<str>, message: Box<str> },
    #[error("cannot export CRS {crs:?} as {format}: {message}")]
    Export {
        crs: Box<str>,
        format: &'static str,
        message: Box<str>,
    },
    #[error("cannot identify CRS {crs:?}: {message}")]
    Identify { crs: Box<str>, message: Box<str> },
    #[error("cannot create CRS transform {from:?} -> {to:?}: {message}")]
    TransformCreate {
        from: Box<str>,
        to: Box<str>,
        message: Box<str>,
    },
    #[error("CRS transform {from:?} -> {to:?} failed: {message}")]
    Transform {
        from: Box<str>,
        to: Box<str>,
        message: Box<str>,
    },
    #[error("cannot choose UTM CRS for an empty geometry")]
    EmptyGeometry,
    #[error("3D metric requires consistent linear axis units on CRS {crs:?}: {message}")]
    VerticalUnits { crs: Box<str>, message: Box<str> },
    #[error("planar metric requires linear axis units on CRS {crs:?}: {message}")]
    MetricUnits { crs: Box<str>, message: Box<str> },
    #[error("geodesic metric requires degree angular axis units on CRS {crs:?}: {message}")]
    GeodesicUnits { crs: Box<str>, message: Box<str> },
    #[error("{0}")]
    Message(Box<str>),
}

impl CrsError {
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn invalid(message: impl Into<Box<str>>) -> CrateError {
        Self::message(message)
    }

    /// A pre-formatted, self-contained message (token rejections, domain
    /// violations) surfaced verbatim — no `invalid CRS` envelope.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn message(message: impl Into<Box<str>>) -> CrateError {
        Self::Message(message.into()).into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn crs_create(crs: impl Into<Box<str>>, message: impl Into<Box<str>>) -> CrateError {
        Self::Create {
            crs: crs.into(),
            message: message.into(),
        }
        .into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn export(
        crs: impl Into<Box<str>>,
        format: &'static str,
        message: impl Into<Box<str>>,
    ) -> CrateError {
        Self::Export {
            crs: crs.into(),
            format,
            message: message.into(),
        }
        .into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn identify(crs: impl Into<Box<str>>, message: impl Into<Box<str>>) -> CrateError {
        Self::Identify {
            crs: crs.into(),
            message: message.into(),
        }
        .into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn transform_create(
        from: impl Into<Box<str>>,
        to: impl Into<Box<str>>,
        message: impl Into<Box<str>>,
    ) -> CrateError {
        Self::TransformCreate {
            from: from.into(),
            to: to.into(),
            message: message.into(),
        }
        .into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn transform(
        from: impl Into<Box<str>>,
        to: impl Into<Box<str>>,
        message: impl Into<Box<str>>,
    ) -> CrateError {
        Self::Transform {
            from: from.into(),
            to: to.into(),
            message: message.into(),
        }
        .into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn vertical_units(crs: impl Into<Box<str>>, message: impl Into<Box<str>>) -> CrateError {
        Self::VerticalUnits {
            crs: crs.into(),
            message: message.into(),
        }
        .into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn metric_units(crs: impl Into<Box<str>>, message: impl Into<Box<str>>) -> CrateError {
        Self::MetricUnits {
            crs: crs.into(),
            message: message.into(),
        }
        .into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn geodesic_units(crs: impl Into<Box<str>>, message: impl Into<Box<str>>) -> CrateError {
        Self::GeodesicUnits {
            crs: crs.into(),
            message: message.into(),
        }
        .into()
    }
}
