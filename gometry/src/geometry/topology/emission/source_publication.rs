use super::{
    GeometryErrorKind, NodeOrdinates, PublicationInventory, QEmissionPlan, ResourceError, Result,
    Work,
};
use crate::geometry::XY;

#[derive(Clone, Copy)]
pub(in crate::geometry::topology) struct SourcePublication<'a> {
    pub(in crate::geometry::topology) inventory: &'a PublicationInventory,
    pub(in crate::geometry::topology) points: &'a [XY],
    pub(in crate::geometry::topology) ordinates: &'a [NodeOrdinates],
    pub(in crate::geometry::topology) live_samples: &'a [bool],
}

pub(in crate::geometry::topology) fn source_live_publication_samples(
    plan: &QEmissionPlan,
    publication: &PublicationInventory,
    work: &mut impl Work,
) -> Result<Vec<bool>> {
    work.charge_into_error(
        u64::try_from(publication.samples.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut live = vec![false; publication.samples.len()];
    for walk in plan.fragment_recipes.walks() {
        for &recipe in &walk.fragments {
            work.charge_into_error(1)?;
            let fragment = publication
                .fragments
                .get(recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source producer walk has no published fragment".into(),
                    )
                })?;
            if fragment.recipe != recipe
                || fragment.sample_range.start > fragment.sample_range.end
                || fragment.sample_range.end > publication.samples.len()
            {
                return Err(GeometryErrorKind::Invalid(
                    "source published fragment has invalid sample ownership".into(),
                )
                .into());
            }
            for sample_index in fragment.sample_range.clone() {
                work.charge_into_error(1)?;
                let sample = publication.samples.get(sample_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source published fragment sample index is unknown".into(),
                    )
                })?;
                if sample.family.fragment_recipe() != Some(recipe) {
                    return Err(GeometryErrorKind::Invalid(
                        "source publication sample belongs to another recipe".into(),
                    )
                    .into());
                }
                *live.get_mut(sample_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source publication sample is outside its liveness table".into(),
                    )
                })? = true;
            }
        }
    }
    Ok(live)
}
