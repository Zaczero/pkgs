use super::*;

/// Optional CDT mesh-refinement controls. Active when either quality constraint
/// is set. Ruppert-style refinement is bounded; incomplete meshes are rejected
/// rather than returned as if the requested quality had been satisfied.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) enum CdtRefinement {
    #[default]
    Off,
    On {
        min_angle: Option<crate::Positive>,
        max_area: Option<crate::Positive>,
    },
}

impl CdtRefinement {
    pub(crate) const fn active(self) -> bool {
        matches!(self, Self::On { .. })
    }
}

/// A `max_area` no larger than the source's exact total area can constrain a
/// triangulation; a limit at least that total cannot. Every retained CDT face
/// lies inside the source and therefore has no more area than the source as a
/// whole, so that request is inert and is dropped before refinement starts.
pub(super) fn effective_refinement(
    area: f64,
    exact_chart_area: Option<&exact::ExactDyadic>,
    refinement: CdtRefinement,
) -> CdtRefinement {
    let CdtRefinement::On {
        min_angle,
        max_area: Some(max_area),
    } = refinement
    else {
        return refinement;
    };
    let inert = exact_chart_area.map_or_else(
        || area.is_finite() && max_area.get() >= area.abs(),
        |area| {
            exact::ExactDyadic::from_f64(max_area.get())
                .cmp(&area.clone().abs())
                .is_ge()
        },
    );
    if inert {
        min_angle.map_or(CdtRefinement::Off, |min_angle| CdtRefinement::On {
            min_angle: Some(min_angle),
            max_area: None,
        })
    } else {
        refinement
    }
}

pub(super) fn validate_refinement_request(
    area: f64,
    exact_chart_area: Option<&exact::ExactDyadic>,
    refinement: CdtRefinement,
) -> Result<()> {
    if let CdtRefinement::On {
        min_angle: Some(angle),
        ..
    } = refinement
        && angle.get() > 30.0
    {
        return Err(GeometryErrorKind::triangulation(format!(
            "min_angle must be at most 30 degrees for terminating constrained refinement, got {}",
            angle.get()
        )));
    }
    if let CdtRefinement::On {
        max_area: Some(max_area),
        ..
    } = refinement
    {
        // The public polygon area is the only magnitude authority: it owns
        // the common shell/hole frame and its exact fallback. Summing a second
        // per-ring approximation here made cyclic rotations disagree about
        // the same max-area admission.
        let minimum_faces = if let Some(area) = exact_chart_area {
            exact_minimum_faces(area, max_area.get())?
        } else {
            let minimum_faces = (area.abs() / max_area.get()).ceil();
            if !minimum_faces.is_finite() || minimum_faces > (GENERATED_ITEM_LIMIT / 4) as f64 {
                return Err(refinement_area_limit_error());
            }
            minimum_faces as usize
        };
        if minimum_faces > GENERATED_ITEM_LIMIT / 4 {
            return Err(refinement_area_limit_error());
        }
        ExpansionBudget::product("triangulate", "max_area", minimum_faces, 4)?;
    }
    Ok(())
}

fn refinement_area_limit_error() -> crate::error::Error {
    GeometryErrorKind::GeneratedOutputTooLarge {
        operation: "triangulate",
        parameter: "max_area",
        produced: GENERATED_ITEM_LIMIT + 1,
        limit: GENERATED_ITEM_LIMIT,
    }
    .into()
}

pub(super) fn exact_minimum_faces(exact_area: &exact::ExactDyadic, max_area: f64) -> Result<usize> {
    let area = exact_area.clone().abs();
    let max_area = exact::ExactDyadic::from_f64(max_area);
    let limit = GENERATED_ITEM_LIMIT / 4;
    let limit_i64 = i64::try_from(limit).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    let maximum_area = max_area.try_product(&exact::ExactDyadic::from_i64(limit_i64))?;
    if area.cmp(&maximum_area).is_gt() {
        return Err(refinement_area_limit_error());
    }
    let (mut low, mut high) = (0_usize, limit);
    while low < high {
        let span = high
            .checked_sub(low)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let middle = low
            .checked_add(span / 2)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let multiple = max_area.try_product(&exact::ExactDyadic::from_i64(
            i64::try_from(middle).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        ))?;
        if area.cmp(&multiple).is_le() {
            high = middle;
        } else {
            low = middle
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
    }
    Ok(low)
}
