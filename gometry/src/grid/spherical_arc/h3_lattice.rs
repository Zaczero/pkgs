//! Certified bounds for the H3 lattice.\n//!\n//! A cell's descendants fit inside a spherical cap whose radius follows from\n//! the lattice profile at that resolution. These routines certify that radius\n//! rather than sampling it, so a cap test can refuse a cell outright.

use super::{
    Bound, CapLatticeProfile, HALF_PI_HI, HALF_PI_LO, Hex2Int, LongitudeSpan, PI_HI, PI_LO,
    ProjectionSeed, Resolution, Vec3Bound, certified_vector_latitude, certified_vector_longitude,
    integer_power_bound, project_h3_seed, project_h3_seed_unnormalized, sin_cos,
};

/// Certified radian bounds at the one permitted conversion boundary between
/// spherical H3 work and the lon/lat source carrier.  Longitude remains a
/// continuous branch until this conversion decides its exact degree seam.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CertifiedCircularBoundsRad {
    pub(super) latitude: Bound,
    pub(super) longitude: CertifiedLongitudeRad,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum CertifiedLongitudeRad {
    Full,
    /// A directed full turn enclosing the north pole. Longitude is still
    /// global, but the latitude certificate must retain this identity before
    /// it is allowed to establish a negative bbox relation.
    FullThroughNorthPole,
    /// The corresponding south-pole full turn.
    FullThroughSouthPole,
    Span(LongitudeSpan),
}

/// One or two canonical closed degree windows.  `Full` deliberately has no
/// seam spelling, preventing a future consumer from accidentally treating
/// +180 and -180 as distinct interior longitude values.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CertifiedDegreeWindows {
    pub(crate) latitude: Bound,
    pub(crate) longitude: CertifiedLongitudeDegrees,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum CertifiedLongitudeDegrees {
    Full,
    One(Bound),
    Two([Bound; 2]),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum DegreeWindowResult {
    Windows(CertifiedDegreeWindows),
    Boundary,
}

/// The finite target-specific native lattice data after every numeric norm has
/// been enclosed outward.  This is deliberately only the vendor half of A3:
/// it cannot classify a cell until the projection and seam certificates have
/// also been assembled into a descendant cap.
#[derive(Clone, Debug)]
pub(crate) struct CertifiedTargetLatticeProfile {
    pub(super) raw: CapLatticeProfile,
    pub(super) suffix_max: [Bound; 15],
    pub(super) vertex_max: Bound,
    pub(super) rotation_norm: Bound,
}

pub(super) const H3_RESOLUTION_COUNT: usize = 16;

/// The complete target-specific angular cap plan.  It is constructed only
/// when every ancestor depth certifies; consumers never observe a partial
/// radius table.
#[derive(Clone, Debug)]
pub(crate) struct CertifiedH3DescendantCap {
    pub(super) target: Resolution,
    pub(super) adjusted_resolution: u8,
    pub(super) max_dim: i64,
    pub(super) angular_radii: [Bound; H3_RESOLUTION_COUNT],
}

/// A projected centre that can only be obtained from the target child in the
/// profiled substrate lattice.  The ordinary parent centre is also a
/// `substrate` point, so carrying a boolean would not make the invalid cap
/// fallback unrepresentable.
#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub(super) struct CertifiedCapCenter(Vec3Bound);

impl CertifiedCapCenter {
    pub(super) fn for_target_cell(
        cell: h3o::CellIndex,
        target: Resolution,
        adjusted_resolution: u8,
        max_dim: i64,
    ) -> Option<Self> {
        let seed = cell.target_substrate_center_seed(target)?;
        let ProjectionSeed::Point {
            point,
            resolution,
            substrate,
            ..
        } = seed
        else {
            return None;
        };
        certified_center_seed_is_admissible(
            point,
            resolution,
            substrate,
            adjusted_resolution,
            max_dim,
        )
        .then(|| project_h3_seed(&seed).map(Self))?
    }
}

impl CertifiedH3DescendantCap {
    pub(crate) fn for_target(target: Resolution) -> Option<Self> {
        let lattice = CertifiedTargetLatticeProfile::for_target(target)?;
        let adjusted_resolution = lattice.raw().adjusted_resolution();
        let expected_dim =
            2_i64.checked_mul(7_i64.checked_pow(u32::from(adjusted_resolution / 2))?)?;
        if !certified_profile_is_coupled(
            lattice.raw().target(),
            adjusted_resolution,
            lattice.raw().max_dim(),
            lattice.raw().seams().len(),
            target,
        ) {
            return None;
        }
        let scale = certified_profile_scale(lattice.raw())?;
        let chart_stretch = certified_chart_stretch(lattice.raw(), scale)?;
        let seam_cost = certified_seam_cost(lattice.raw(), scale)?;
        let rotation_cubed = lattice
            .rotation_norm()
            .mul_nonnegative(lattice.rotation_norm())?
            .mul_nonnegative(lattice.rotation_norm())?;
        let zero = Bound::exact(0.0)?;
        let mut angular_radii = [zero; H3_RESOLUTION_COUNT];
        for depth in Resolution::range(Resolution::Zero, target) {
            certified_ideal_face_ratio(target, depth)?;
            let native = lattice.native_descendant_radius(depth)?;
            let radius =
                certified_angular_radius(scale, chart_stretch, rotation_cubed, native, seam_cost)?;
            if radius.hi >= HALF_PI_LO {
                return None;
            }
            angular_radii[usize::from(depth)] = radius;
        }
        Some(Self {
            target,
            adjusted_resolution,
            max_dim: expected_dim,
            angular_radii,
        })
    }

    #[cfg(test)]
    pub(crate) const fn target(&self) -> Resolution {
        self.target
    }

    pub(super) fn angular_radius(&self, depth: Resolution) -> Option<Bound> {
        (depth <= self.target).then(|| self.angular_radii[usize::from(depth)])
    }

    pub(crate) fn descendant_windows(&self, cell: h3o::CellIndex) -> DegreeWindowResult {
        let Some(result) = (|| {
            if cell.resolution() > self.target {
                return None;
            }
            let center = CertifiedCapCenter::for_target_cell(
                cell,
                self.target,
                self.adjusted_resolution,
                self.max_dim,
            )?;
            let radius = self.angular_radius(cell.resolution())?;
            let latitude = certified_vector_latitude(center.0)?;
            let (south, north, absolute) = certified_cap_latitude_bounds(latitude, radius)?;
            if south.lo > north.hi {
                return None;
            }
            if absolute.hi >= HALF_PI_LO {
                return Some((
                    CertifiedCircularBoundsRad {
                        latitude: Bound::new(south.lo, north.hi)?,
                        longitude: CertifiedLongitudeRad::Full,
                    },
                    center,
                ));
            }
            let longitude = certified_vector_longitude(center.0)?;
            let (_, cosine) = sin_cos(absolute)?;
            let delta = radius.div_nonnegative(cosine.bound()?)?;
            if delta.hi >= PI_LO {
                return Some((
                    CertifiedCircularBoundsRad {
                        latitude: Bound::new(south.lo, north.hi)?,
                        longitude: CertifiedLongitudeRad::Full,
                    },
                    center,
                ));
            }
            let longitude = cap_longitude_span(longitude, delta)?;
            Some((
                CertifiedCircularBoundsRad {
                    latitude: Bound::new(south.lo, north.hi)?,
                    longitude,
                },
                center,
            ))
        })() else {
            return DegreeWindowResult::Boundary;
        };
        let (bounds, center) = result;
        bounds.to_pruning_degree_windows(center)
    }
}

/// Outward cap latitude arithmetic.  This is a pruning certificate, so every
/// operation stays in `Bound`; raw endpoint arithmetic is not authority.
pub(super) fn certified_cap_latitude_bounds(
    latitude: Bound,
    radius: Bound,
) -> Option<(Bound, Bound, Bound)> {
    let world = Bound::new(-HALF_PI_HI, HALF_PI_HI)?;
    let south = latitude.sub(radius)?.intersection(world)?;
    let north = latitude.add(radius)?.intersection(world)?;
    let absolute = latitude.abs().add_nonnegative(radius)?;
    Some((south, north, absolute))
}

/// A cap radius is a correlated `[0, R]` enclosure, not two independent arc
/// endpoints.  Its upper half-width alone gives the outward longitude span;
/// a later seam ambiguity is safely the full longitude range.
pub(super) fn cap_longitude_span(longitude: Bound, delta: Bound) -> Option<CertifiedLongitudeRad> {
    if delta.lo < 0.0 {
        return None;
    }
    let outer = Bound::exact(delta.hi)?;
    Some(
        LongitudeSpan::ordered(longitude.sub(outer)?, longitude.add(outer)?)
            .map_or(CertifiedLongitudeRad::Full, CertifiedLongitudeRad::Span),
    )
}

/// Validates the finite vendor profile before it can contribute even one
/// descendant radius.  Keeping this scalar check separate lets its exact
/// coupling be tested without manufacturing malformed vendor sidecars.
pub(super) fn certified_profile_is_coupled(
    raw_target: Resolution,
    adjusted_resolution: u8,
    max_dim: i64,
    seam_count: usize,
    target: Resolution,
) -> bool {
    let Some(expected_adjusted) =
        target_value(target).and_then(|value| value.checked_add(u8::from(target.is_class3())))
    else {
        return false;
    };
    let Some(power) = 7_i64.checked_pow(u32::from(adjusted_resolution / 2)) else {
        return false;
    };
    let Some(expected_dim) = 2_i64.checked_mul(power) else {
        return false;
    };
    raw_target == target
        && adjusted_resolution == expected_adjusted
        && max_dim == expected_dim
        && seam_count == 60
}

/// Rejects a target-center carrier unless it is exactly the substrate point
/// represented by the profiled target lattice.  A failure makes the caller's
/// bbox `Boundary`; it can never prune a cell.
pub(super) fn certified_center_seed_is_admissible(
    point: Hex2Int,
    resolution: u8,
    substrate: bool,
    adjusted_resolution: u8,
    max_dim: i64,
) -> bool {
    let Some(limit) = 3_i64.checked_mul(max_dim) else {
        return false;
    };
    resolution == adjusted_resolution
        && substrate
        && point.normalized_ijk_sum().is_some_and(|sum| sum <= limit)
}

/// The certified cap composition.  `rotation_cubed` is already `K³`; keeping
/// it explicit makes the three-crossing charge impossible to accidentally
/// collapse into a one-rotation estimate.
pub(super) fn certified_angular_radius(
    scale: Bound,
    chart_stretch: Bound,
    rotation_cubed: Bound,
    native_radius: Bound,
    seam_cost: Bound,
) -> Option<Bound> {
    scale
        .mul_nonnegative(chart_stretch)?
        .mul_nonnegative(rotation_cubed)?
        .mul_nonnegative(native_radius)?
        .add_nonnegative(Bound::exact(3.0)?.mul_nonnegative(seam_cost)?)
}

impl CertifiedTargetLatticeProfile {
    pub(crate) fn for_target(target: Resolution) -> Option<Self> {
        let raw = CapLatticeProfile::for_target(target);
        let adjusted = target_value(target)? + u8::from(target.is_class3());
        if raw.target() != target
            || raw.adjusted_resolution() != adjusted
            || raw.max_dim() <= 0
            || raw.seams().len() != 60
        {
            return None;
        }

        let sqrt3_2 = certified_profile_sqrt3_2(&raw, adjusted)?;
        let zero = Bound::exact(0.0)?;
        let mut suffix_max = [zero; 15];
        for raw_resolution in 1..=target_value(target)? {
            let resolution = Resolution::try_from(raw_resolution).ok()?;
            let offsets = raw.suffix_offsets(resolution)?;
            suffix_max[usize::from(raw_resolution - 1)] = certified_lattice_max(offsets, sqrt3_2)?;
        }
        let vertex_max = certified_lattice_max(raw.vertex_offsets(), sqrt3_2)?;
        let rotation_norm = certified_native_rotation_norm(sqrt3_2)?;
        Some(Self {
            raw,
            suffix_max,
            vertex_max,
            rotation_norm,
        })
    }

    pub(crate) const fn raw(&self) -> &CapLatticeProfile {
        &self.raw
    }

    pub(crate) const fn rotation_norm(&self) -> Bound {
        self.rotation_norm
    }

    /// Returns the native-plane enclosure for every target boundary endpoint
    /// under an ancestor at `depth`, before chart overage is charged.
    pub(crate) fn native_descendant_radius(&self, depth: Resolution) -> Option<Bound> {
        if depth > self.raw.target() {
            return None;
        }
        let mut radius = self.vertex_max;
        for raw_resolution in (target_value(depth)? + 1)..=target_value(self.raw.target())? {
            radius = radius.add_nonnegative(self.suffix_max[usize::from(raw_resolution - 1)])?;
        }
        Some(radius)
    }
}

pub(super) fn target_value(value: Resolution) -> Option<u8> {
    let value = u8::from(value);
    (value <= 15).then_some(value)
}

/// Every profile seam must remain a paired raw substrate carrier at the
/// adjusted target resolution.  This check intentionally uses bit identity
/// only for vendor constants; no rounded geographic coordinate participates.
pub(super) fn certified_profile_sqrt3_2(raw: &CapLatticeProfile, adjusted: u8) -> Option<Bound> {
    let first = raw.seams().first()?.left()[0];
    let frame = first.projection_frame();
    if !frame.sqrt3_2.is_finite() || frame.sqrt3_2 <= 0.0 {
        return None;
    }
    let bits = frame.sqrt3_2.to_bits();
    for seam in raw.seams() {
        for seed in seam.left().into_iter().chain(seam.right()) {
            let ProjectionSeed::Point {
                resolution,
                substrate,
                ..
            } = seed
            else {
                return None;
            };
            if resolution != adjusted
                || !substrate
                || seed.projection_frame().sqrt3_2.to_bits() != bits
            {
                return None;
            }
        }
    }
    Bound::exact(frame.sqrt3_2)
}

pub(super) fn certified_profile_scale(raw: &CapLatticeProfile) -> Option<Bound> {
    let first = raw.seams().first()?.left()[0].projection_frame();
    for seam in raw.seams() {
        for seed in seam.left().into_iter().chain(seam.right()) {
            let frame = seed.projection_frame();
            if frame.res0_u_gnomonic.to_bits() != first.res0_u_gnomonic.to_bits()
                || frame.inv_sqrt7.to_bits() != first.inv_sqrt7.to_bits()
                || frame.one_third.to_bits() != first.one_third.to_bits()
            {
                return None;
            }
        }
    }
    Bound::exact(first.res0_u_gnomonic)?
        .mul_nonnegative(Bound::exact(first.inv_sqrt7)?)?
        .mul_nonnegative(Bound::exact(first.one_third)?)
}

#[derive(Clone, Copy)]
pub(super) struct FaceProjectionCertificate {
    /// A strictly positive lower enclosure for the unnormalised carrier norm
    /// across the raw face triangle.  It is a lower bound by construction,
    /// so dividing by it only enlarges a later Lipschitz/seam charge.
    pub(super) minimum_denominator: Bound,
    pub(super) basis_norm: Bound,
    #[cfg(test)]
    pub(super) raw_c_radicand: Bound,
}

pub(super) fn certified_chart_stretch(raw: &CapLatticeProfile, scale: Bound) -> Option<Bound> {
    let mut maximum = Bound::exact(0.0)?;
    for seam in raw.seams() {
        for seed in seam.left().into_iter().chain(seam.right()) {
            let certificate = certified_face_projection(&seed, raw.max_dim(), scale)?;
            let stretch = certificate
                .basis_norm
                .div_nonnegative(certificate.minimum_denominator)?;
            maximum = Bound::new(0.0, maximum.hi.max(stretch.hi))?;
        }
    }
    Some(maximum)
}

pub(super) fn certified_face_projection(
    seed: &ProjectionSeed,
    max_dim: i64,
    scale: Bound,
) -> Option<FaceProjectionCertificate> {
    let frame = seed.projection_frame();
    let center = Vec3Bound::exact(frame.center)?;
    let north_axis = Vec3Bound::exact([0.0, 0.0, 1.0])?;
    let north = north_axis.sub(center.scale(center.z)?)?.normalized()?;
    let east = north.cross(center)?;
    let angle = Bound::exact(frame.axis_az_rads_cii)?;
    let (sin, cos) = sin_cos(angle)?;
    let sin = sin.bound()?;
    let cos = cos.bound()?;
    let first = north.scale(cos)?.add(east.scale(sin)?)?;
    let second = north.scale(sin)?.sub(east.scale(cos)?)?;
    let center_norm = center.dot(center)?;
    // Keep `h = ||B^T C||` in the same rotated B basis used for the
    // Lipschitz norm below.  Although Q is a native rotation, spelling it
    // directly avoids making that orthogonality an unstated proof premise.
    let center_projection = first
        .dot(center)?
        .square()?
        .add_nonnegative(second.dot(center)?.square()?)?
        .sqrt()?;
    let face_radius = certified_face_radius(max_dim, Bound::exact(frame.sqrt3_2)?)?;
    let radicand = center_norm.sub(
        Bound::exact(2.0)?
            .mul_nonnegative(scale)?
            .mul_nonnegative(face_radius)?
            .mul_nonnegative(center_projection)?,
    )?;
    if radicand.lo <= 0.0 {
        return None;
    }
    let minimum_denominator = Bound::exact(radicand.lo)?.sqrt()?;
    if !minimum_denominator.strictly_positive() {
        return None;
    }
    let basis_norm = certified_two_column_norm(first, second)?;
    Some(FaceProjectionCertificate {
        minimum_denominator,
        basis_norm,
        #[cfg(test)]
        raw_c_radicand: radicand,
    })
}

pub(super) fn certified_two_column_norm(first: Vec3Bound, second: Vec3Bound) -> Option<Bound> {
    let g00 = first.dot(first)?;
    let g01 = first.dot(second)?;
    let g11 = second.dot(second)?;
    certified_gram_norm(g00, g01, g11)
}

pub(super) fn certified_gram_norm(g00: Bound, g01: Bound, g11: Bound) -> Option<Bound> {
    if g00.lo < 0.0 || g11.lo < 0.0 {
        return None;
    }
    let trace = g00.add_nonnegative(g11)?;
    let discriminant = g00
        .sub(g11)?
        .square()?
        .add_nonnegative(Bound::exact(4.0)?.mul_nonnegative(g01.square()?)?)?
        .sqrt()?;
    trace
        .add_nonnegative(discriminant)?
        .div_nonnegative(Bound::exact(2.0)?)?
        .sqrt()
}

pub(super) fn certified_seam_cost(raw: &CapLatticeProfile, scale: Bound) -> Option<Bound> {
    let mut maximum_charge = Bound::exact(0.0)?;
    for (left_seed, right_seed) in certified_matching_seam_pairs(raw) {
        let seam_charge = certified_seam_charge(&left_seed, &right_seed, raw.max_dim(), scale)?;
        maximum_charge = Bound::new(0.0, maximum_charge.hi.max(seam_charge.hi))?;
    }
    Some(maximum_charge)
}

pub(super) fn certified_matching_seam_pairs(
    raw: &CapLatticeProfile,
) -> impl Iterator<Item = (ProjectionSeed, ProjectionSeed)> + '_ {
    raw.seams()
        .iter()
        .flat_map(|seam| seam.left().into_iter().zip(seam.right()))
}

/// One directed seam's normalization charge.  The delta is deliberately
/// formed in the two raw face carriers; normalized vectors would erase the
/// chart mismatch that the cap must pay for.
pub(super) fn certified_seam_charge(
    left_seed: &ProjectionSeed,
    right_seed: &ProjectionSeed,
    max_dim: i64,
    scale: Bound,
) -> Option<Bound> {
    let left_certificate = certified_face_projection(left_seed, max_dim, scale)?;
    let right_certificate = certified_face_projection(right_seed, max_dim, scale)?;
    let delta = certified_seam_delta(left_seed, right_seed)?;
    let denominator = Bound::exact(
        left_certificate
            .minimum_denominator
            .lo
            .min(right_certificate.minimum_denominator.lo),
    )?;
    Bound::new(PI_LO, PI_HI)?
        .mul_nonnegative(delta)?
        .div_nonnegative(denominator)
}

/// Distance between the two raw, unnormalised representations of one seam
/// endpoint.  Matching directed endpoints are almost identical; reversing
/// them yields the opposite edge endpoint and is macroscopically distinct.
pub(super) fn certified_seam_delta(
    left_seed: &ProjectionSeed,
    right_seed: &ProjectionSeed,
) -> Option<Bound> {
    project_h3_seed_unnormalized(left_seed)?
        .sub(project_h3_seed_unnormalized(right_seed)?)?
        .squared_norm()?
        .sqrt()
}

/// The maximum native-plane norm of the three raw vertices of a Class-II
/// icosahedron face at `max_dim`.  All three are enumerated: binary
/// `SQRT3_2` is intentionally not treated as mathematical sqrt(3)/2.
pub(super) fn certified_face_radius(max_dim: i64, sqrt3_2: Bound) -> Option<Bound> {
    let max_dim = i128::from(max_dim);
    let coordinates = [
        (6_i128.checked_mul(max_dim)?, 0),
        (-3_i128.checked_mul(max_dim)?, 3_i128.checked_mul(max_dim)?),
        (-3_i128.checked_mul(max_dim)?, -3_i128.checked_mul(max_dim)?),
    ];
    let mut maximum = Bound::exact(0.0)?;
    for (x2, y_sqrt3) in coordinates {
        let point = Hex2Int::new(i64::try_from(x2).ok()?, i64::try_from(y_sqrt3).ok()?);
        let norm = certified_lattice_norm(point, sqrt3_2)?;
        maximum = Bound::new(0.0, maximum.hi.max(norm.hi))?;
    }
    Some(maximum)
}

/// Proves the finite unfolded-face crossing lemma for one ancestor depth.
///
/// These are mathematical `sqrt(3)`/`sqrt(7)` bounds, deliberately separate
/// from the vendor's binary `SQRT3_2` arithmetic used by `D_native`.  The
/// strict result licenses exactly three, rather than an empirical or padded,
/// chart-transition charges in [`CertifiedH3DescendantCap::radius`].
pub(super) fn certified_ideal_face_ratio(target: Resolution, depth: Resolution) -> Option<Bound> {
    if depth > target {
        return None;
    }
    let target_resolution = target_value(target)?;
    let depth_resolution = target_value(depth)?;
    let class_iii = u8::from(target.is_class3());
    let sqrt7 = Bound::exact(7.0)?.sqrt()?;
    let mut ideal = if class_iii == 1 {
        Bound::exact(21.0)?.sqrt()?
    } else {
        Bound::exact(3.0)?.sqrt()?
    };
    for k in (depth_resolution + 1)..=target_resolution {
        let exponent = target_resolution.checked_sub(k)?.checked_add(class_iii)?;
        let suffix = Bound::exact(3.0)?.mul_nonnegative(integer_power_bound(sqrt7, exponent)?)?;
        ideal = ideal.add_nonnegative(suffix)?;
    }
    let adjusted = target_resolution.checked_add(class_iii)?;
    let face_dimension = Bound::exact(2.0)?
        .mul_nonnegative(integer_power_bound(Bound::exact(7.0)?, adjusted / 2)?)?;
    let ratio = ideal.div_nonnegative(face_dimension)?;
    certified_strict_face_ratio(ratio)
}

/// The face transition count is licensed only by a strict ratio.  Equality is
/// a boundary event and must leave the cap unavailable.
pub(super) fn certified_strict_face_ratio(ratio: Bound) -> Option<Bound> {
    (ratio.hi < 1.0).then_some(ratio)
}

pub(super) fn certified_lattice_max(points: &[Hex2Int], sqrt3_2: Bound) -> Option<Bound> {
    let mut maximum = Bound::exact(0.0)?;
    for point in points {
        let norm = certified_lattice_norm(*point, sqrt3_2)?;
        maximum = Bound::new(0.0, maximum.hi.max(norm.hi))?;
    }
    Some(maximum)
}

pub(super) fn certified_lattice_norm(point: Hex2Int, sqrt3_2: Bound) -> Option<Bound> {
    let x = Bound::from_i128(i128::from(point.x2))?.mul(Bound::exact(0.5)?)?;
    let y = Bound::from_i128(i128::from(point.y_sqrt3))?.mul(sqrt3_2)?;
    x.square()?.add_nonnegative(y.square()?)?.sqrt()
}

/// Outward `K`: the maximum spectral norm of all six powers of the vendor's
/// binary 60-degree lattice rotation.  The upper Gram eigenvalue is used
/// rather than a Frobenius relaxation; a single rotation is insufficient
/// because the vendor's binary `SQRT3_2` makes the powers non-orthogonal.
pub(super) fn certified_native_rotation_norm(sqrt3_2: Bound) -> Option<Bound> {
    let powers = certified_native_rotation_power_norms(sqrt3_2)?;
    let mut maximum = Bound::exact(0.0)?;
    for norm in powers {
        maximum = Bound::new(0.0, maximum.hi.max(norm.hi))?;
    }
    Some(maximum)
}

pub(super) fn certified_native_rotation_power_norms(sqrt3_2: Bound) -> Option<[Bound; 6]> {
    let rotation = [
        Bound::exact(0.5)?,
        Bound::exact(-3.0)?.div(Bound::exact(4.0)?.mul(sqrt3_2)?)?,
        sqrt3_2,
        Bound::exact(0.5)?,
    ];
    let mut power = [
        Bound::exact(1.0)?,
        Bound::exact(0.0)?,
        Bound::exact(0.0)?,
        Bound::exact(1.0)?,
    ];
    let mut norms = [Bound::exact(0.0)?; 6];
    for slot in &mut norms {
        *slot = certified_two_by_two_norm(power)?;
        power = certified_two_by_two_product(power, rotation)?;
    }
    Some(norms)
}

pub(super) fn certified_two_by_two_product(
    left: [Bound; 4],
    right: [Bound; 4],
) -> Option<[Bound; 4]> {
    Some([
        left[0].mul(right[0])?.add(left[1].mul(right[2])?)?,
        left[0].mul(right[1])?.add(left[1].mul(right[3])?)?,
        left[2].mul(right[0])?.add(left[3].mul(right[2])?)?,
        left[2].mul(right[1])?.add(left[3].mul(right[3])?)?,
    ])
}

pub(super) fn certified_two_by_two_norm(entries: [Bound; 4]) -> Option<Bound> {
    let [a, b, c, d] = entries;
    let g00 = a.square()?.add_nonnegative(c.square()?)?;
    let g01 = a.mul(b)?.add(c.mul(d)?)?;
    let g11 = b.square()?.add_nonnegative(d.square()?)?;
    certified_gram_norm(g00, g01, g11)
}

impl CertifiedCircularBoundsRad {
    /// Cap-only conversion: once [`CertifiedCapCenter`] has established the
    /// target-child provenance, an ambiguous longitude seam may widen to the
    /// complete longitude range while retaining its certified latitude.
    /// Logical H3 bboxes use [`Self::to_degree_windows`] instead and preserve
    /// `Boundary` for the same ambiguity.
    pub(super) fn to_pruning_degree_windows(
        self,
        _center: CertifiedCapCenter,
    ) -> DegreeWindowResult {
        let Some(latitude) = radians_to_degrees(self.latitude)
            .and_then(|latitude| Bound::new(latitude.lo.max(-90.0), latitude.hi.min(90.0)))
        else {
            return DegreeWindowResult::Boundary;
        };
        match self.to_degree_windows() {
            DegreeWindowResult::Windows(CertifiedDegreeWindows { longitude, .. }) => {
                DegreeWindowResult::Windows(CertifiedDegreeWindows {
                    latitude,
                    longitude,
                })
            },
            DegreeWindowResult::Boundary => DegreeWindowResult::Windows(CertifiedDegreeWindows {
                latitude,
                longitude: CertifiedLongitudeDegrees::Full,
            }),
        }
    }

    pub(super) fn to_degree_windows(self) -> DegreeWindowResult {
        let Some(latitude) = radians_to_degrees(self.latitude)
            .and_then(|latitude| Bound::new(latitude.lo.max(-90.0), latitude.hi.min(90.0)))
        else {
            return DegreeWindowResult::Boundary;
        };
        let longitude = match self.longitude {
            CertifiedLongitudeRad::Full
            | CertifiedLongitudeRad::FullThroughNorthPole
            | CertifiedLongitudeRad::FullThroughSouthPole => CertifiedLongitudeDegrees::Full,
            CertifiedLongitudeRad::Span(span) => {
                let (Some(lower), Some(upper)) = (
                    radians_to_degrees(span.lower),
                    radians_to_degrees(span.upper),
                ) else {
                    return DegreeWindowResult::Boundary;
                };
                // A branch wholly in the canonical sheet needs no seam cut.
                if lower.lo >= -180.0 && upper.hi <= 180.0 {
                    let Some(window) = Bound::new(lower.lo, upper.hi) else {
                        return DegreeWindowResult::Boundary;
                    };
                    CertifiedLongitudeDegrees::One(window)
                // A positive-sheet branch crosses exactly +180.  Both endpoint
                // orders must be strict, otherwise a rounded seam spelling
                // could discard a true contact.
                } else if lower.hi < 180.0 && upper.lo > 180.0 && upper.hi < 540.0 {
                    let Some(shifted_upper) =
                        upper.sub(Bound::exact(360.0).expect("finite degree"))
                    else {
                        return DegreeWindowResult::Boundary;
                    };
                    let (Some(west), Some(east)) = (
                        Bound::new(lower.lo, 180.0),
                        Bound::new(-180.0, shifted_upper.hi),
                    ) else {
                        return DegreeWindowResult::Boundary;
                    };
                    CertifiedLongitudeDegrees::Two([west, east])
                // The equivalent negative-sheet branch crosses -180.
                } else if lower.hi < -180.0 && upper.lo > -180.0 && lower.lo > -540.0 {
                    let Some(shifted_lower) =
                        lower.add(Bound::exact(360.0).expect("finite degree"))
                    else {
                        return DegreeWindowResult::Boundary;
                    };
                    let (Some(west), Some(east)) = (
                        Bound::new(shifted_lower.lo, 180.0),
                        Bound::new(-180.0, upper.hi),
                    ) else {
                        return DegreeWindowResult::Boundary;
                    };
                    CertifiedLongitudeDegrees::Two([west, east])
                } else {
                    return DegreeWindowResult::Boundary;
                }
            },
        };
        DegreeWindowResult::Windows(CertifiedDegreeWindows {
            latitude,
            longitude,
        })
    }
}

/// The single outward radian/degree conversion.  This must remain the owner:
/// `to_degrees`, a rounded pi denominator, or duplicated callers would turn a
/// degree seam into an unproved negative.
pub(super) fn radians_to_degrees(radians: Bound) -> Option<Bound> {
    radians
        .mul(Bound::exact(180.0)?)?
        .div(Bound::new(PI_LO, PI_HI)?)
}
