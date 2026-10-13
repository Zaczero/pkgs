use std::collections::{BTreeMap, BTreeSet};

use crate::error::{ResourceError, Result};
use crate::geometry::GeometryErrorKind;
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::work::Work;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry) struct LogicalArealWalk {
    pub(in crate::geometry) output: u32,
    pub(in crate::geometry) walk: u32,
    pub(in crate::geometry) vertices: Vec<u32>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct DirectedArealUse {
    pub(super) atom: u32,
    pub(super) reversed: bool,
}

pub(super) struct AdmittedArealWalk {
    pub(super) output: u32,
    pub(super) walk: u32,
    pub(super) uses: Vec<DirectedArealUse>,
}

/// Dimensions are admitted from the actual output walks after the producer's
/// canonical atom inventory exists.
pub(super) struct LogicalLayout {
    pub(super) areal_walks: Vec<AdmittedArealWalk>,
}

impl LogicalLayout {
    pub(super) fn admit(
        walks: Vec<LogicalArealWalk>,
        vertex_count: usize,
        edge_nodes: &[[u32; 2]],
        work: &mut impl Work,
    ) -> Result<Self> {
        if walks.is_empty() {
            return Ok(Self {
                areal_walks: Vec::new(),
            });
        }
        work.charge_into_error(
            u64::try_from(edge_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut atom_by_endpoints = BTreeMap::new();
        for (atom, &[left, right]) in edge_nodes.iter().enumerate() {
            work.charge_into_error(1)?;
            let key = if left < right {
                [left, right]
            } else {
                [right, left]
            };
            if atom_by_endpoints
                .insert(
                    key,
                    u32::try_from(atom).map_err(|_| ResourceError::SizeOverflow)?,
                )
                .is_some()
            {
                return Err(GeometryErrorKind::Invalid(
                    "logical layout received duplicate geometric atoms".into(),
                )
                .into());
            }
        }

        work.charge_into_error(
            u64::try_from(walks.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut identities = BTreeSet::new();
        let mut areal_walks = Vec::with_capacity(walks.len());
        for walk in walks {
            work.charge_into_error(1)?;
            if !identities.insert((walk.output, walk.walk))
                || walk.vertices.len() < 4
                || walk.vertices.first() != walk.vertices.last()
            {
                return Err(GeometryErrorKind::Invalid(
                    "logical areal walk identity or closure is invalid".into(),
                )
                .into());
            }
            let edge_count = walk
                .vertices
                .len()
                .checked_sub(1)
                .ok_or(ResourceError::SizeOverflow)?;
            work.charge_into_error(
                u64::try_from(edge_count).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut uses = Vec::with_capacity(edge_count);
            for &[start, end] in walk.vertices.array_windows::<2>() {
                work.charge_into_error(1)?;
                if start as usize >= vertex_count || end as usize >= vertex_count {
                    return Err(GeometryErrorKind::Invalid(
                        "logical areal walk references an unknown graph vertex".into(),
                    )
                    .into());
                }
                let endpoints = if start < end {
                    [start, end]
                } else {
                    [end, start]
                };
                let atom = *atom_by_endpoints.get(&endpoints).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "logical areal walk uses an atom outside the admitted graph".into(),
                    )
                })?;
                let canonical = edge_nodes.get(atom as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("logical areal atom ID is unknown".into())
                })?;
                uses.push(DirectedArealUse {
                    atom,
                    reversed: [end, start] == *canonical,
                });
            }
            areal_walks.push(AdmittedArealWalk {
                output: walk.output,
                walk: walk.walk,
                uses,
            });
        }
        try_sort_by_with_work(&mut areal_walks, work, |left, right, work| {
            work.charge_into_error(1)?;
            Ok((left.output, left.walk).cmp(&(right.output, right.walk)))
        })?;
        Ok(Self { areal_walks })
    }

    pub(super) const fn has_areal_walks(&self) -> bool {
        !self.areal_walks.is_empty()
    }
}
