use super::*;

pub(in crate::geometry::topology::q) fn consider_cut_gap(
    low: &ExactRational,
    high: &ExactRational,
    half: &ExactRational,
    negative_half: &ExactRational,
    period: &ExactRational,
    largest_gap: &mut Option<ExactRational>,
    cut: &mut Option<ExactRational>,
) -> Result<()> {
    let width = high.try_subtract(low)?;
    let is_larger = if let Some(largest) = largest_gap {
        width.try_cmp(largest)?.is_gt()
    } else {
        true
    };
    if is_larger {
        let midpoint =
            canonicalize_cut_longitude(&low.try_midpoint(high)?, half, negative_half, period)?;
        *largest_gap = Some(width);
        *cut = Some(midpoint);
    }
    Ok(())
}

pub(in crate::geometry::topology::q) fn canonicalize_cut_longitude(
    longitude: &ExactRational,
    half: &ExactRational,
    negative_half: &ExactRational,
    period: &ExactRational,
) -> Result<ExactRational> {
    let mut canonical = longitude.clone();
    if canonical.try_cmp(half)?.is_ge() {
        canonical = canonical.try_subtract(period)?;
    }
    if canonical.try_cmp(negative_half)?.is_lt() {
        canonical = canonical.try_add(period)?;
    }
    Ok(canonical)
}

pub(in crate::geometry::topology::q) type LongitudeInterval = (ExactRational, ExactRational);

pub(in crate::geometry::topology::q) fn chart_edge_longitude_intervals(
    frame: GeographicFrame,
    chart: &NodedExactChart,
) -> Result<Vec<LongitudeInterval>> {
    let north = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North));
    let south = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::South));
    let half = ExactRational::from_f64(frame.longitude().half_period());
    let negative_half = ExactRational::from_f64(-frame.longitude().half_period());
    let mut intervals = Vec::with_capacity(chart.atoms.len());
    for atom in &chart.atoms {
        let [start_id, end_id] = atom.ports;
        let start = chart
            .ports
            .get(start_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q atom has an unknown start port".into()))?;
        let end = chart
            .ports
            .get(end_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q atom has an unknown end port".into()))?;
        let start_at_north = start.point.y.try_cmp(&north)?.is_eq();
        let end_at_north = end.point.y.try_cmp(&north)?.is_eq();
        let start_at_south = start.point.y.try_cmp(&south)?.is_eq();
        let end_at_south = end.point.y.try_cmp(&south)?.is_eq();
        if (start_at_north && end_at_north) || (start_at_south && end_at_south) {
            continue;
        }
        let longitude_order = start.point.x.try_cmp(&end.point.x)?;
        let (low, high) = if longitude_order.is_gt() {
            (end.point.x.clone(), start.point.x.clone())
        } else {
            (start.point.x.clone(), end.point.x.clone())
        };
        intervals.push((low, high));
    }
    let mut has_point_support = false;
    for &point_id in &chart.source_point_ports {
        let point = chart
            .ports
            .get(point_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q point has an unknown port".into()))?;
        if point.point.y.try_cmp(&north)?.is_eq() || point.point.y.try_cmp(&south)?.is_eq() {
            continue;
        }
        let longitude = if point.point.x.try_cmp(&half)?.is_eq() {
            negative_half.clone()
        } else {
            point.point.x.clone()
        };
        intervals.push((longitude.clone(), longitude));
        has_point_support = true;
    }
    // Noded atoms follow lexicographic endpoint order. Source-point supports
    // are appended separately and can disturb that interval order.
    if has_point_support {
        try_sort_by(&mut intervals, |left, right| {
            let longitude_order = left.0.try_cmp(&right.0)?;
            if longitude_order.is_eq() {
                left.1.try_cmp(&right.1)
            } else {
                Ok(longitude_order)
            }
        })?;
    }

    let mut union: Vec<LongitudeInterval> = Vec::with_capacity(intervals.len());
    for (low, high) in intervals {
        if let Some((_, union_high)) = union.last_mut()
            && low.try_cmp(union_high)?.is_le()
        {
            if high.try_cmp(union_high)?.is_gt() {
                *union_high = high;
            }
            continue;
        }
        union.push((low, high));
    }
    Ok(union)
}

pub(in crate::geometry::topology::q) struct EdgeFreeGap {
    pub(in crate::geometry::topology::q) low: ExactRational,
    pub(in crate::geometry::topology::q) high: ExactRational,
    pub(in crate::geometry::topology::q) width: ExactRational,
    pub(in crate::geometry::topology::q) cut: ExactRational,
}

pub(in crate::geometry::topology::q) fn consider_edge_free_gap(
    low: &ExactRational,
    high: &ExactRational,
    half: &ExactRational,
    negative_half: &ExactRational,
    period: &ExactRational,
    widest: &mut Option<EdgeFreeGap>,
) -> Result<()> {
    let width = high.try_subtract(low)?;
    if width.is_zero() {
        return Ok(());
    }

    let cut = canonicalize_cut_longitude(&low.try_midpoint(high)?, half, negative_half, period)?;

    let replace = if let Some(widest_gap) = widest.as_ref() {
        match width.try_cmp(&widest_gap.width)? {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => cut.try_cmp(&widest_gap.cut)?.is_lt(),
        }
    } else {
        true
    };
    if replace {
        *widest = Some(EdgeFreeGap {
            low: low.clone(),
            high: high.clone(),
            width,
            cut,
        });
    }
    Ok(())
}

// The noder assigns port IDs in lexicographic (x, y) order.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "binary-search bounds keep middle and middle + 1 below ports.len()"
)]
pub(in crate::geometry::topology::q) fn chart_has_port_at_longitude(
    chart: &NodedExactChart,
    longitude: &ExactRational,
) -> Result<bool> {
    let mut low = 0;
    let mut high = chart.ports.len();
    while low < high {
        let middle = low + (high - low) / 2;
        let port = chart
            .ports
            .get(middle)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q port index is out of range".into()))?;
        match port.point.x.try_cmp(longitude)? {
            Ordering::Less => low = middle + 1,
            Ordering::Greater => high = middle,
            Ordering::Equal => return Ok(true),
        }
    }
    Ok(false)
}

pub(in crate::geometry::topology::q) fn chart_has_cut_port(
    frame: GeographicFrame,
    chart: &NodedExactChart,
    cut: &ExactRational,
) -> Result<bool> {
    if chart_has_port_at_longitude(chart, cut)? {
        return Ok(true);
    }
    let axis = frame.longitude();
    let negative_half = ExactRational::from_f64(-axis.half_period());
    if cut.try_cmp(&negative_half)?.is_eq() {
        return chart_has_port_at_longitude(chart, &ExactRational::from_f64(axis.half_period()));
    }
    Ok(false)
}

pub(in crate::geometry::topology::q) fn select_port_free_cut(
    frame: GeographicFrame,
    chart: &NodedExactChart,
    low: &ExactRational,
    high: &ExactRational,
) -> Result<ExactRational> {
    let axis = frame.longitude();
    let half = ExactRational::from_f64(axis.half_period());
    let negative_half = ExactRational::from_f64(-axis.half_period());
    let period = ExactRational::from_f64(axis.period());
    let mut port_longitudes = Vec::with_capacity(chart.ports.len());
    for port in &chart.ports {
        let longitude = if port.point.x.try_cmp(low)?.is_le() {
            port.point.x.try_add(&period)?
        } else {
            port.point.x.clone()
        };
        if longitude.try_cmp(low)?.is_gt() && longitude.try_cmp(high)?.is_lt() {
            port_longitudes.push(longitude);
        }
    }
    try_sort_by(&mut port_longitudes, ExactRational::try_cmp)?;

    let mut widest = None;
    let mut gap_low = low.clone();
    let mut previous: Option<ExactRational> = None;
    for longitude in port_longitudes {
        if let Some(previous) = &previous
            && longitude.try_cmp(previous)?.is_eq()
        {
            continue;
        }
        consider_edge_free_gap(
            &gap_low,
            &longitude,
            &half,
            &negative_half,
            &period,
            &mut widest,
        )?;
        gap_low = longitude.clone();
        previous = Some(longitude);
    }
    consider_edge_free_gap(&gap_low, high, &half, &negative_half, &period, &mut widest)?;
    widest.map(|gap: EdgeFreeGap| gap.cut).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q edge-free gap has no port-free cut".into()).into()
    })
}

pub(in crate::geometry::topology::q) fn select_edge_free_cut(
    frame: GeographicFrame,
    chart: &NodedExactChart,
) -> Result<Option<ExactRational>> {
    let intervals = chart_edge_longitude_intervals(frame, chart)?;
    if intervals.is_empty() {
        let zero = ExactRational::zero();
        if chart_has_cut_port(frame, chart, &zero)? {
            let axis = frame.longitude();
            let low = ExactRational::from_f64(-axis.half_period());
            let high = ExactRational::from_f64(axis.half_period());
            return Ok(Some(select_port_free_cut(frame, chart, &low, &high)?));
        }
        return Ok(Some(zero));
    }

    let axis = frame.longitude();
    let half = ExactRational::from_f64(axis.half_period());
    let negative_half = ExactRational::from_f64(-axis.half_period());
    let period = ExactRational::from_f64(axis.period());
    let mut widest = None;
    for adjacent in intervals.windows(2) {
        let [(.., low), (high, ..)] = adjacent else {
            unreachable!("windows(2) yields adjacent longitude intervals");
        };
        consider_edge_free_gap(low, high, &half, &negative_half, &period, &mut widest)?;
    }
    let (first_low, _) = intervals
        .first()
        .expect("a nonempty interval vector has a first interval");
    let (_, last_high) = intervals
        .last()
        .expect("a nonempty interval vector has a last interval");
    let wrap_high = first_low.try_add(&period)?;
    consider_edge_free_gap(
        last_high,
        &wrap_high,
        &half,
        &negative_half,
        &period,
        &mut widest,
    )?;
    if let Some(gap) = widest {
        if chart_has_cut_port(frame, chart, &gap.cut)? {
            return Ok(Some(select_port_free_cut(
                frame, chart, &gap.low, &gap.high,
            )?));
        }
        return Ok(Some(gap.cut));
    }
    Ok(None)
}

pub(in crate::geometry::topology::q) fn select_cut(
    frame: GeographicFrame,
    chart: &NodedExactChart,
) -> Result<ExactRational> {
    // A port gap can still be crossed by a straight chart edge; same-pole
    // spans are quotient points and do not occupy longitude intervals.
    if let Some(cut) = select_edge_free_cut(frame, chart)? {
        return Ok(cut);
    }
    // When source edges cover the full longitude circle, preserve the prior
    // port-gap choice because no edge-free cut exists.
    select_cut_from_ports(frame, chart)
}

pub(in crate::geometry::topology::q) fn select_cut_from_ports(
    frame: GeographicFrame,
    chart: &NodedExactChart,
) -> Result<ExactRational> {
    let axis = frame.longitude();
    let half = ExactRational::from_f64(axis.half_period());
    let negative_half = ExactRational::from_f64(-axis.half_period());
    let period = ExactRational::from_f64(axis.period());
    let mut longitudes = Vec::with_capacity(chart.ports.len());
    for port in &chart.ports {
        longitudes.push(canonicalize_cut_longitude(
            &port.point.x,
            &half,
            &negative_half,
            &period,
        )?);
    }
    try_sort_by(&mut longitudes, ExactRational::try_cmp)?;
    let mut unique: Vec<ExactRational> = Vec::with_capacity(longitudes.len());
    for longitude in longitudes {
        if let Some(previous) = unique.last()
            && previous.try_cmp(&longitude)?.is_eq()
        {
            continue;
        }
        unique.push(longitude);
    }
    if unique.is_empty() {
        return Ok(ExactRational::zero());
    }

    let mut largest_gap: Option<ExactRational> = None;
    let mut cut: Option<ExactRational> = None;
    for adjacent in unique.windows(2) {
        let [low, high] = adjacent else {
            unreachable!("windows(2) yields adjacent longitudes");
        };
        consider_cut_gap(
            low,
            high,
            &half,
            &negative_half,
            &period,
            &mut largest_gap,
            &mut cut,
        )?;
    }
    let low = unique
        .last()
        .expect("nonempty longitude list has a last value");
    let high = unique
        .first()
        .expect("nonempty longitude list has a first value")
        .try_add(&period)?;
    consider_cut_gap(
        low,
        &high,
        &half,
        &negative_half,
        &period,
        &mut largest_gap,
        &mut cut,
    )?;
    Ok(cut.expect("a nonempty longitude set has a periodic gap"))
}
