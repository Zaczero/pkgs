//! Reusable source-envelope selection for literal and periodic tile frames.

use rstar::{AABB, RTree, RTreeObject};

use crate::error::Result;
use crate::geometry::{ExactRational, floor_exact};

#[derive(Clone, Copy)]
struct Entry {
    row: usize,
    envelope: AABB<[f64; 2]>,
}

impl RTreeObject for Entry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

pub(super) struct SourceIndex {
    literal: RTree<Entry>,
    periodic: Option<RTree<Entry>>,
}

impl SourceIndex {
    pub(super) fn new(rows: Vec<(usize, [f64; 4])>, periodic: bool) -> Result<Self> {
        let mut literal = Vec::with_capacity(rows.len());
        let mut wrapped = Vec::new();
        for (row, [minx, miny, maxx, maxy]) in rows {
            literal.push(Entry {
                row,
                envelope: AABB::from_corners([minx, miny], [maxx, maxy]),
            });
            if periodic {
                for [left, right] in canonical_intervals(minx, maxx)? {
                    wrapped.push(Entry {
                        row,
                        envelope: AABB::from_corners([left, miny], [right, maxy]),
                    });
                }
            }
        }
        Ok(Self {
            literal: RTree::bulk_load(literal),
            periodic: periodic.then(|| RTree::bulk_load(wrapped)),
        })
    }

    pub(super) fn candidates(&self, regions: &[[f64; 4]], periodic: bool) -> Vec<usize> {
        let index = if periodic {
            self.periodic
                .as_ref()
                .expect("periodic queries require a periodic source index")
        } else {
            &self.literal
        };
        let mut rows = Vec::new();
        for &[minx, miny, maxx, maxy] in regions {
            let envelope = AABB::from_corners([minx, miny], [maxx, maxy]);
            rows.extend(
                index
                    .locate_in_envelope_intersecting(envelope)
                    .map(|entry| entry.row),
            );
        }
        rows.sort_unstable();
        rows.dedup();
        rows
    }
}

fn canonical_intervals(minx: f64, maxx: f64) -> Result<Vec<[f64; 2]>> {
    let world_float = std::f64::consts::PI * 6_378_137.0;
    let world = ExactRational::from_f64(world_float);
    let negative_world = ExactRational::from_f64(-world_float);
    let period = world.try_add(&world)?;
    let minimum = ExactRational::from_f64(minx);
    let width = ExactRational::from_f64(maxx).try_subtract(&minimum)?;
    if width.try_cmp(&period)?.is_ge() {
        return Ok(vec![[-world_float, world_float]]);
    }
    let turns = floor_exact(&minimum.try_add(&world)?.try_quotient(&period)?)?;
    let start = minimum.try_subtract(&ExactRational::from_dyadic(turns).try_product(&period)?)?;
    let end = start.try_add(&width)?;
    let lower = |value: &ExactRational| -> Result<f64> { Ok(value.outward_bounds()?.0) };
    let upper = |value: &ExactRational| -> Result<f64> { Ok(value.outward_bounds()?.1) };
    if end.try_cmp(&world)?.is_gt() {
        Ok(vec![[lower(&start)?, world_float], [
            -world_float,
            upper(&end.try_subtract(&period)?)?,
        ]])
    } else {
        let mut intervals = vec![[lower(&start)?, upper(&end)?]];
        if start.try_cmp(&negative_world)?.is_eq() {
            intervals.push([world_float, world_float]);
        } else if end.try_cmp(&world)?.is_eq() {
            intervals.push([-world_float, -world_float]);
        }
        Ok(intervals)
    }
}
