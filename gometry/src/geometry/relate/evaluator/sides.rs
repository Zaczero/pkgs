use super::atoms::{is_cross_event_node, same_atom_point};
use super::*;
use crate::geometry::source_support::ExactProbe;

#[derive(Clone, Copy)]
pub(super) struct AtomSides {
    pub(super) left: bool,
    pub(super) right: bool,
}

impl AtomSides {
    pub(super) const fn new(left: bool, right: bool) -> Self {
        Self { left, right }
    }

    pub(super) const fn swap(self) -> Self {
        Self {
            left: self.right,
            right: self.left,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct RunCache {
    pub(super) component: u32,
    pub(super) end: AtomPoint,
    pub(super) label: Loc,
}

pub(super) fn atom_owner_label<'a>(
    operand: &Operand<'_>,
    component: &Component,
    atom: &'a ExactAtom<'_>,
    probe: &OnceCell<ExactProbe<'a>>,
    coordinates: &mut ExactCoordinates,
    forest: &ExactPointForest<'_>,
) -> Result<Loc> {
    if !component.dirty {
        return match component.source.ring_kind {
            Some(RingKind::Winding) => component
                .seed
                .map(SourcePointLocation::edge_atom_location)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "a clean winding ring has no representative location".into(),
                    )
                    .into()
                }),
            Some(RingKind::Collapsed) => Err(GeometryErrorKind::Invalid(
                "collapsed rings cannot use a clean atom label".into(),
            )
            .into()),
            None => Ok(Loc::Interior),
        };
    }
    let (start, direction) = atom.offset(coordinates, forest)?;
    let probe = probe.get_or_init(|| ExactProbe::new(start));
    Ok(operand.locate_probe_offset(probe, direction)?.location)
}

pub(super) fn atom_sides<'a>(
    operand: &Operand<'_>,
    component: &Component,
    has_area: bool,
    atom: &'a ExactAtom<'_>,
    probe: &OnceCell<ExactProbe<'a>>,
    coordinates: &mut ExactCoordinates,
    forest: &ExactPointForest<'_>,
) -> Result<AtomSides> {
    if !has_area {
        return Ok(AtomSides::new(false, false));
    }
    if !component.dirty {
        match component.source.ring_kind {
            Some(RingKind::Winding) => {
                let seed = component.seed.ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "a clean winding ring has no representative location".into(),
                    )
                })?;
                return match seed.edge_atom_location() {
                    Loc::Interior => Ok(AtomSides::new(true, true)),
                    Loc::Exterior => Ok(AtomSides::new(false, false)),
                    Loc::Boundary => {
                        let left = component.left_area.ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "a clean winding ring has no left-face label".into(),
                            )
                        })?;
                        Ok(AtomSides::new(left, !left))
                    },
                };
            },
            Some(RingKind::Collapsed) => {
                return Err(GeometryErrorKind::Invalid(
                    "collapsed rings cannot use clean side labels".into(),
                )
                .into());
            },
            None => {
                let area = component.left_area.ok_or_else(|| {
                    GeometryErrorKind::Invalid("a clean line has no areal side label".into())
                })?;
                return Ok(AtomSides::new(area, area));
            },
        }
    }
    let (midpoint, direction) = atom.side_probe(coordinates, forest)?;
    let left = segment_left_normal_from_direction(direction);
    let right = [left[0].clone().neg(), left[1].clone().neg()];
    let probe = probe.get_or_init(|| ExactProbe::new(midpoint));
    Ok(AtomSides::new(
        occupied_area_probe_side(operand, probe, &left)?,
        occupied_area_probe_side(operand, probe, &right)?,
    ))
}

fn occupied_area_probe_side(
    operand: &Operand<'_>,
    probe: &ExactProbe<'_>,
    direction: &[ExactDyadic; 2],
) -> Result<bool> {
    match operand.occupied_toward_probe(probe, direction)? {
        OccupiedToward::Yes => Ok(true),
        OccupiedToward::No => Ok(false),
        OccupiedToward::Tangent => Err(GeometryErrorKind::Invalid(
            "an exhaustive atom-side probe is tangent to source support".into(),
        )
        .into()),
    }
}

pub(super) fn segment_left_normal_from_direction(direction: &[ExactDyadic; 2]) -> [ExactDyadic; 2] {
    [direction[1].clone().neg(), direction[0].clone()]
}

pub(super) fn run_label<'a>(
    operand: &Operand<'_>,
    component: u32,
    atom: &'a ExactAtom<'_>,
    probe: &OnceCell<ExactProbe<'a>>,
    coordinates: &mut ExactCoordinates,
    cache: Option<&RunCache>,
    forest: &mut ExactPointForest<'_>,
    cross_stored: &HashSet<PointKey>,
    cross_constructed: &HashSet<u32>,
) -> Result<Loc> {
    if !is_cross_event_node(atom.start, forest, cross_stored, cross_constructed)
        && let Some(cache) = cache
        && cache.component == component
        && same_atom_point(cache.end, atom.start, forest, coordinates)?
    {
        return Ok(cache.label);
    }
    let (start_exact, direction) = atom.offset(coordinates, forest)?;
    let probe = probe.get_or_init(|| ExactProbe::new(start_exact));
    match operand.occupied_toward_probe(probe, direction)? {
        OccupiedToward::Yes => Ok(Loc::Interior),
        OccupiedToward::No => Ok(Loc::Exterior),
        OccupiedToward::Tangent => Err(GeometryErrorKind::Invalid(
            "an uncovered source atom has a tangent face probe".into(),
        )
        .into()),
    }
}

pub(super) fn ring_left_area(
    operand: &Operand<'_>,
    segment: Segment,
    coordinates: &mut ExactCoordinates,
) -> Result<bool> {
    let start = coordinates.stored(segment.start).clone();
    let end = coordinates.stored(segment.end);
    let midpoint = homogeneous_midpoint(&start, end)?;
    let direction = segment_left_normal_from_direction(&coordinates.direction(segment)?);
    let probe = ExactProbe::new(&midpoint);
    match operand.occupied_toward_probe(&probe, &direction)? {
        OccupiedToward::Yes => Ok(true),
        OccupiedToward::No => Ok(false),
        OccupiedToward::Tangent => Err(GeometryErrorKind::Invalid(
            "a clean ring side probe is tangent to source support".into(),
        )
        .into()),
    }
}

pub(super) fn initialize_clean_ring_sides(
    operands: &[Operand<'_>; 2],
    sources: &StagedExactSources,
    components: &mut [Component],
    coordinates: &mut ExactCoordinates,
) -> Result<()> {
    for component in components {
        if component.dirty
            || component.source.ring_kind != Some(RingKind::Winding)
            || component.left_area.is_some()
        {
            continue;
        }
        let segment = component
            .source
            .segments
            .clone()
            .filter_map(|index| sources.segments().get(index).copied())
            .find(|segment| !same_point(segment.start, segment.end))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("a clean winding ring has no nonzero segment".into())
            })?;
        component.left_area = Some(ring_left_area(
            by_operand(operands, component.side),
            segment,
            coordinates,
        )?);
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn segment_direction(segment: Segment) -> Result<[ExactDyadic; 2]> {
    Ok([
        ExactDyadic::from_f64(segment.end.x)
            .try_subtract(ExactDyadic::from_f64(segment.start.x))?,
        ExactDyadic::from_f64(segment.end.y)
            .try_subtract(ExactDyadic::from_f64(segment.start.y))?,
    ])
}
