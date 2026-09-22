//! The CRS error kind. Variants carry their fields directly — the crate-wide
//! [`Error`](crate::error::Error) boxes the whole kind, so nothing here needs
//! per-variant payload boxing. Constructors return [`Error`] so call sites
//! build the boxed crate error in one step.

use thiserror::Error;

use crate::error::{Error as CrateError, ErrorKind};

#[derive(Debug, Error)]
pub enum CrsError {
    #[error(
        "cannot create CRS {:?}: {message}",
        .display_name.as_deref().unwrap_or_else(|| crs.as_ref())
    )]
    Create {
        crs: Box<str>,
        display_name: Option<&'static str>,
        message: Box<str>,
    },
    #[error("cannot export CRS {crs:?} as {format}: {message}")]
    Export {
        crs: Box<str>,
        format: &'static str,
        message: Box<str>,
    },
    #[error("cannot identify CRS {crs:?}: {message}")]
    Identify { crs: Box<str>, message: Box<str> },
    #[error(
        "cannot create CRS transform {:?} -> {:?}: {message}",
        .source_display.as_deref().unwrap_or_else(|| from.as_ref()),
        .target_display.as_deref().unwrap_or_else(|| to.as_ref())
    )]
    TransformCreate {
        from: Box<str>,
        to: Box<str>,
        source_display: Option<&'static str>,
        target_display: Option<&'static str>,
        message: Box<str>,
    },
    #[error(
        "CRS transform {:?} -> {:?} failed: {message}",
        .source_display.as_deref().unwrap_or_else(|| from.as_ref()),
        .target_display.as_deref().unwrap_or_else(|| to.as_ref())
    )]
    Transform {
        from: Box<str>,
        to: Box<str>,
        source_display: Option<&'static str>,
        target_display: Option<&'static str>,
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
    #[error("certified geodesic exponent range overflowed")]
    GeodesicExponentOverflow,
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
            display_name: None,
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
            source_display: None,
            target_display: None,
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
            source_display: None,
            target_display: None,
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

    pub(crate) fn annotate_display(&mut self, definition: &str, display_name: &'static str) {
        match self {
            Self::Create {
                crs,
                display_name: current,
                ..
            } if crs.as_ref() == definition => *current = Some(display_name),
            Self::TransformCreate {
                from,
                to,
                source_display,
                target_display,
                ..
            }
            | Self::Transform {
                from,
                to,
                source_display,
                target_display,
                ..
            } => {
                if from.as_ref() == definition {
                    *source_display = Some(display_name);
                }
                if to.as_ref() == definition {
                    *target_display = Some(display_name);
                }
            },
            _ => {},
        }
    }
}

/// Rewrap an ordinary CRS failure without losing a typed resource failure.
/// Callers supply the operation-specific CRS envelope; this helper only
/// preserves the resource variants and never passes ordinary errors through.
pub(crate) fn preserve_resource_error(
    error: CrateError,
    rewrap: impl FnOnce(&CrateError) -> CrateError,
) -> CrateError {
    if matches!(error.kind(), ErrorKind::Resource(_)) {
        return error;
    }
    rewrap(&error)
}

#[cfg(test)]
mod tests {
    use super::CrsError;
    use crate::error::{Error, ErrorKind, ResourceError};

    #[test]
    fn display_annotation_matches_each_transform_stage() {
        let mut create = CrsError::Create {
            crs: "local-definition".into(),
            display_name: None,
            message: "metadata failed".into(),
        };
        create.annotate_display("local-definition", "Local conformal CRS");
        assert_eq!(
            create.to_string(),
            "cannot create CRS \"Local conformal CRS\": metadata failed"
        );
        let mut ordinary_create = CrsError::Create {
            crs: "EPSG:4326".into(),
            display_name: None,
            message: "metadata failed".into(),
        };
        ordinary_create.annotate_display("local-definition", "Local conformal CRS");
        assert_eq!(
            ordinary_create.to_string(),
            "cannot create CRS \"EPSG:4326\": metadata failed"
        );

        let mut transform_create = CrsError::TransformCreate {
            from: "local-definition".into(),
            to: "EPSG:4326".into(),
            source_display: None,
            target_display: None,
            message: "pipeline failed".into(),
        };
        transform_create.annotate_display("local-definition", "Local conformal CRS");
        assert_eq!(
            transform_create.to_string(),
            "cannot create CRS transform \"Local conformal CRS\" -> \"EPSG:4326\": pipeline failed"
        );
        let mut ordinary_transform_create = CrsError::TransformCreate {
            from: "EPSG:4326".into(),
            to: "EPSG:3857".into(),
            source_display: None,
            target_display: None,
            message: "pipeline failed".into(),
        };
        ordinary_transform_create.annotate_display("local-definition", "Local conformal CRS");
        assert_eq!(
            ordinary_transform_create.to_string(),
            "cannot create CRS transform \"EPSG:4326\" -> \"EPSG:3857\": pipeline failed"
        );

        let mut transform = CrsError::Transform {
            from: "EPSG:4326".into(),
            to: "local-definition".into(),
            source_display: None,
            target_display: None,
            message: "execution failed".into(),
        };
        transform.annotate_display("local-definition", "Local conformal CRS");
        assert_eq!(
            transform.to_string(),
            "CRS transform \"EPSG:4326\" -> \"Local conformal CRS\" failed: execution failed"
        );
    }

    #[test]
    fn display_annotation_ignores_nonmatching_endpoints() {
        let mut transform = CrsError::Transform {
            from: "EPSG:4326".into(),
            to: "EPSG:3857".into(),
            source_display: None,
            target_display: None,
            message: "execution failed".into(),
        };
        transform.annotate_display("local-definition", "Local conformal CRS");
        assert_eq!(
            transform.to_string(),
            "CRS transform \"EPSG:4326\" -> \"EPSG:3857\" failed: execution failed"
        );
    }

    #[test]
    fn display_annotation_reuses_error_storage_without_allocation() {
        const DEFINITION: &str = "{\"type\":\"ProjectedCRS\"}";
        let mut error: Error = CrsError::transform(DEFINITION, "EPSG:4326", "projection failed");
        crate::test_support::assert_no_allocations(|| {
            error.annotate_crs_display(DEFINITION, Some("Local conformal CRS"));
        });

        let ErrorKind::Crs(CrsError::Transform {
            from,
            to,
            source_display,
            target_display,
            ..
        }) = error.kind()
        else {
            panic!("display annotation changed the error kind");
        };
        assert_eq!(from.as_ref(), DEFINITION);
        assert_eq!(to.as_ref(), "EPSG:4326");
        assert_eq!(*source_display, Some("Local conformal CRS"));
        assert_eq!(*target_display, None);
        assert_eq!(
            error.to_string(),
            "CRS transform \"Local conformal CRS\" -> \"EPSG:4326\" failed: projection failed"
        );

        let mut resource = Error::from(ResourceError::SizeOverflow);
        crate::test_support::assert_no_allocations(|| {
            resource.annotate_crs_display(DEFINITION, Some("Local conformal CRS"));
        });
        assert!(matches!(
            resource.kind(),
            ErrorKind::Resource(ResourceError::SizeOverflow)
        ));
    }
}
