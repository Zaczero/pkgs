use std::ptr;

use crate::crs::catalog::{
    CF_LINEAR_INTERNATIONAL_FOOT, CF_LINEAR_METRE, CF_LINEAR_US_SURVEY_FOOT,
    CF_TRANSVERSE_MERCATOR_PARAMETERS, add_cf_lambert_azimuthal_equal_area_parameters,
    add_cf_lambert_conformal_conic_1sp_parameters, add_cf_lambert_conformal_conic_parameters,
    add_cf_lambert_cylindrical_equal_area_parameters, add_cf_mercator_variant_a_parameters,
    add_cf_mercator_variant_b_parameters, add_cf_polar_stereographic_parameters,
    add_cf_projection_parameters, cf_lcc_1sp_admits_mapping, cf_mercator_b_has_scale,
    cf_prime_meridian_longitude_degrees, cf_projected_axis_to_metre, export, is_operation_method,
    is_transverse_mercator,
};
use crate::crs::{
    AuthorityObjectInfo, AxisInfo, CRS_AUTHORITY_MATCH_CACHE, CRS_AUTHORITY_MATCH_CACHE_CAPACITY,
    CRS_NAME_MATCH_CACHE, CRS_NAME_MATCH_CACHE_CAPACITY, CachedAuthorityObjects,
    CachedCrsAuthorityMatches, CachedCrsNameMatch, CfValue, Confidence, CrsError, CrsInfo,
    CrsNameMatchOptions, CrsObjectKind, CrsWktOptions, IdentifyCandidate, NON_DEPRECATED_CACHE,
    NON_DEPRECATED_CACHE_CAPACITY, ProjObjList, ProjObject, RuntimeState, WktVersion,
    authority_object_info, cstring, ensure_thread_caches_current, info_with_state, lru_insert,
    lru_promote, lru_resolve, normalize, proj_context_error_message, runtime_state_snapshot,
    validate_min_confidence, with_proj_context_at,
};
use crate::error::Result;

/// ULP allowance for identified EPSG:9003 only.
///
/// `1200/3937` is the definition (`pj_units[].factor`, `+units=us-ft`). The
/// EPSG SQL REAL `3.04800609601219241184e-01` sits 1 ULP from that ratio;
/// the 15-digit decimal `"0.304800609601219"` (WKT/PROJJSON round-trip)
/// sits 3 ULP away. The bound is the maximum distance from the ratio along
/// those routes, not an endpoint-to-endpoint span. Unidentified custom
/// factors must match the ratio exactly.
const US_SURVEY_FOOT_IDENTIFIED_MAX_ULP: u64 = 3;

fn cf_axis_is_horizontal(axis: &AxisInfo) -> bool {
    !matches!(
        axis.direction.as_deref(),
        Some(direction) if direction.eq_ignore_ascii_case("up")
            || direction.eq_ignore_ascii_case("down")
            || direction.eq_ignore_ascii_case("future")
            || direction.eq_ignore_ascii_case("past")
            || direction.eq_ignore_ascii_case("geocentricz")
    )
}

const fn us_survey_foot_identified_consistent(factor: f64) -> bool {
    factor
        .to_bits()
        .abs_diff(CF_LINEAR_US_SURVEY_FOOT.to_bits())
        <= US_SURVEY_FOOT_IDENTIFIED_MAX_ULP
}

/// Admit a CF/`+units=` token from axis identity and conversion factor.
///
/// Names never participate. Invalid factors are rejected before ULP distance.
#[expect(
    clippy::float_cmp_const,
    reason = "CF unit tokens are catalog identities: the factor either is the EPSG metre/foot constant or it is not"
)]
fn cf_proj_units_token(axis: &AxisInfo) -> Option<&'static str> {
    let factor = axis.unit_conversion_factor;
    if !factor.is_finite() || factor <= 0.0 {
        return None;
    }
    let identified_epsg = axis.unit_authority.as_deref() == Some("EPSG");
    match (identified_epsg, axis.unit_code.as_deref()) {
        (true, Some("9001")) if factor == CF_LINEAR_METRE => return Some("m"),
        (true, Some("9002")) if factor == CF_LINEAR_INTERNATIONAL_FOOT => return Some("ft"),
        (true, Some("9003")) if us_survey_foot_identified_consistent(factor) => {
            return Some("us-ft");
        },
        (true, Some("9001" | "9002" | "9003")) => return None,
        _ => {},
    }
    if factor == CF_LINEAR_METRE {
        Some("m")
    } else if factor == CF_LINEAR_INTERNATIONAL_FOOT {
        Some("ft")
    } else if factor == CF_LINEAR_US_SURVEY_FOOT {
        Some("us-ft")
    } else {
        None
    }
}

/// Same admitted token on every horizontal axis, or `None` (omit mapping).
fn projected_cf_units_token(info: &CrsInfo) -> Option<&'static str> {
    let mut admitted = None;
    for axis in &info.axes {
        if !cf_axis_is_horizontal(axis) {
            continue;
        }
        let token = cf_proj_units_token(axis)?;
        match admitted {
            None => admitted = Some(token),
            Some(previous) if previous == token => {},
            Some(_) => return None,
        }
    }
    admitted
}

pub(crate) fn identify_with_state(
    value: &str,
    authority: Option<&str>,
    state: &RuntimeState,
) -> Result<Vec<IdentifyCandidate>> {
    let normalized = normalize(value)?;
    ProjObject::new_with_state(&normalized, state)?.identify(authority, &normalized)
}

pub(crate) fn to_authority_with_state(
    value: &str,
    authority: Option<&str>,
    min_confidence: Confidence,
    state: &RuntimeState,
) -> Result<Option<(String, String)>> {
    with_authority_matches(value, authority, min_confidence, state, best_authority)
}

fn best_authority(items: &[IdentifyCandidate]) -> Option<(String, String)> {
    items.iter().find_map(|candidate| {
        candidate
            .authority
            .as_ref()
            .zip(candidate.code.as_ref())
            .map(|(authority, code)| (authority.clone(), code.clone()))
    })
}

#[cfg(test)]
pub(crate) fn authority_matches(
    value: &str,
    authority: Option<&str>,
    min_confidence: Confidence,
) -> Result<Vec<IdentifyCandidate>> {
    let state = runtime_state_snapshot()?;
    with_authority_matches(value, authority, min_confidence, &state, <[_]>::to_vec)
}

/// Resolve the cached identify candidates and read them in place: a caller
/// that wants one field pays for that field, not for a copy of the row set.
fn with_authority_matches<R>(
    value: &str,
    authority: Option<&str>,
    min_confidence: Confidence,
    state: &RuntimeState,
    read: impl FnOnce(&[IdentifyCandidate]) -> R,
) -> Result<R> {
    validate_min_confidence(min_confidence);
    ensure_thread_caches_current(state);
    let normalized = normalize(value)?;
    CRS_AUTHORITY_MATCH_CACHE.with(|items| {
        let mut cache = items.borrow_mut();
        if !lru_promote(&mut cache, |item| {
            item.matches(&normalized, authority, min_confidence)
        }) {
            let candidates = identify_with_state(&normalized, authority, state)?;
            let items: Vec<_> = candidates
                .into_iter()
                .filter(|candidate| {
                    candidate.confidence >= min_confidence
                        && candidate.authority.is_some()
                        && candidate.code.is_some()
                })
                .collect();
            lru_insert(
                &mut cache,
                CRS_AUTHORITY_MATCH_CACHE_CAPACITY,
                CachedCrsAuthorityMatches {
                    definition: normalized.clone().into(),
                    authority: authority.map(str::to_owned),
                    min_confidence,
                    items,
                },
            );
        }
        let entry = cache.last().expect("resolved authority matches");
        Ok(read(&entry.items))
    })
}

impl CachedCrsAuthorityMatches {
    fn matches(&self, value: &str, authority: Option<&str>, min_confidence: Confidence) -> bool {
        self.definition == value
            && self.authority.as_deref() == authority
            && self.min_confidence == min_confidence
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AuthorityLookup {
    Uncached,
    Cached(Option<(String, String)>),
}

pub(crate) fn authority_if_cached(
    value: &str,
    authority: Option<&str>,
    min_confidence: Confidence,
    state: &RuntimeState,
) -> Result<AuthorityLookup> {
    let normalized = normalize(value)?;
    if !crate::crs::thread_caches_are_current(state) {
        return Ok(AuthorityLookup::Uncached);
    }
    CRS_AUTHORITY_MATCH_CACHE.with(|items| {
        let mut cache = items.borrow_mut();
        if !lru_promote(&mut cache, |item| {
            item.matches(&normalized, authority, min_confidence)
        }) {
            return Ok(AuthorityLookup::Uncached);
        }
        Ok(AuthorityLookup::Cached(best_authority(
            &cache.last().expect("promoted authority matches").items,
        )))
    })
}

pub(crate) fn to_cf(value: &str, wkt_version: WktVersion) -> Result<Vec<(&'static str, CfValue)>> {
    let state = runtime_state_snapshot()?;
    to_cf_with_state(value, wkt_version, &state)
}

fn to_cf_with_state(
    value: &str,
    wkt_version: WktVersion,
    state: &RuntimeState,
) -> Result<Vec<(&'static str, CfValue)>> {
    let normalized = normalize(value)?;
    let info = info_with_state(&normalized, state)?;
    // Compound CRSs are not a single CF grid_mapping; reject rather than
    // silently truncating to the horizontal ellipsoid metadata alone.
    if info.is_compound() {
        return Err(CrsError::invalid(format!(
            "CRS.to_cf() does not support compound CRS {normalized}; convert components separately"
        )));
    }
    let wkt =
        export::to_wkt_with_options_at(&normalized, wkt_version, &CrsWktOptions::default(), state)?;
    let mut items = Vec::with_capacity(16);
    items.push(("crs_wkt", CfValue::String(wkt)));
    if let Some(ellipsoid) = &info.ellipsoid {
        items.push((
            "semi_major_axis",
            CfValue::Float(ellipsoid.semi_major_metre),
        ));
        items.push((
            "semi_minor_axis",
            CfValue::Float(ellipsoid.semi_minor_metre),
        ));
        items.push((
            "inverse_flattening",
            CfValue::Float(ellipsoid.inverse_flattening),
        ));
        if let Some(name) = &ellipsoid.name {
            items.push(("reference_ellipsoid_name", CfValue::String(name.clone())));
        }
    }
    if let Some(prime_meridian) = &info.prime_meridian {
        items.push((
            "longitude_of_prime_meridian",
            CfValue::Float(cf_prime_meridian_longitude_degrees(
                &normalized,
                prime_meridian,
            )?),
        ));
        if let Some(name) = &prime_meridian.name {
            items.push(("prime_meridian_name", CfValue::String(name.clone())));
        }
    }
    let projected_units = (info.kind == "projected")
        .then(|| projected_cf_units_token(&info))
        .flatten();
    push_cf_crs_names(&mut items, &info);
    if let Some(token) = projected_units {
        items.push(("units", CfValue::String(token.to_owned())));
    }
    if let Some(name) = info.datum.as_ref().and_then(|datum| datum.name.clone()) {
        items.push(("horizontal_datum_name", CfValue::String(name)));
    }
    if info.kind != "projected" || projected_units.is_some() {
        let axis_to_metre = cf_projected_axis_to_metre(&normalized, &info)?;
        push_cf_grid_mapping(&normalized, &mut items, &info, axis_to_metre)?;
    }
    Ok(items)
}

fn push_cf_crs_names(items: &mut Vec<(&'static str, CfValue)>, info: &CrsInfo) {
    if matches!(info.kind, "geographic" | "geographic_2d" | "geographic_3d") {
        if let Some(name) = &info.name {
            items.push(("geographic_crs_name", CfValue::String(name.clone())));
        }
        return;
    }
    if info.kind != "projected" {
        return;
    }
    if let Some(name) = info.geodetic_crs.as_ref().and_then(|crs| crs.name.clone()) {
        items.push(("geographic_crs_name", CfValue::String(name)));
    }
    if let Some(name) = &info.name {
        items.push(("projected_crs_name", CfValue::String(name.clone())));
    }
}

fn push_cf_grid_mapping(
    crs: &str,
    items: &mut Vec<(&'static str, CfValue)>,
    info: &CrsInfo,
    axis_to_metre: Option<f64>,
) -> Result<()> {
    if matches!(info.kind, "geographic" | "geographic_2d" | "geographic_3d") {
        items.push((
            "grid_mapping_name",
            CfValue::String("latitude_longitude".to_owned()),
        ));
        return Ok(());
    }
    let Some(operation) = &info.coordinate_operation else {
        return Ok(());
    };
    if is_transverse_mercator(operation) {
        items.push((
            "grid_mapping_name",
            CfValue::String("transverse_mercator".to_owned()),
        ));
        add_cf_projection_parameters(
            crs,
            items,
            &operation.parameters,
            CF_TRANSVERSE_MERCATOR_PARAMETERS,
            axis_to_metre,
        )?;
    } else if is_operation_method(operation, "9820", "Lambert Azimuthal Equal Area") {
        items.push((
            "grid_mapping_name",
            CfValue::String("lambert_azimuthal_equal_area".to_owned()),
        ));
        add_cf_lambert_azimuthal_equal_area_parameters(
            crs,
            items,
            &operation.parameters,
            axis_to_metre,
        )?;
    } else if is_operation_method(operation, "9802", "Lambert Conic Conformal (2SP)") {
        items.push((
            "grid_mapping_name",
            CfValue::String("lambert_conformal_conic".to_owned()),
        ));
        add_cf_lambert_conformal_conic_parameters(
            crs,
            items,
            &operation.parameters,
            axis_to_metre,
        )?;
    } else if is_operation_method(operation, "9801", "Lambert Conic Conformal (1SP)")
        && cf_lcc_1sp_admits_mapping(&operation.parameters)
    {
        items.push((
            "grid_mapping_name",
            CfValue::String("lambert_conformal_conic".to_owned()),
        ));
        add_cf_lambert_conformal_conic_1sp_parameters(
            crs,
            items,
            &operation.parameters,
            axis_to_metre,
        )?;
    } else if is_operation_method(operation, "9804", "Mercator (variant A)") {
        items.push(("grid_mapping_name", CfValue::String("mercator".to_owned())));
        add_cf_mercator_variant_a_parameters(crs, items, &operation.parameters, axis_to_metre)?;
    } else if is_operation_method(operation, "9805", "Mercator (variant B)")
        && !cf_mercator_b_has_scale(&operation.parameters)
    {
        items.push(("grid_mapping_name", CfValue::String("mercator".to_owned())));
        add_cf_mercator_variant_b_parameters(crs, items, &operation.parameters, axis_to_metre)?;
    } else if is_operation_method(operation, "9829", "Polar Stereographic (variant B)")
        || is_operation_method(operation, "9810", "Polar Stereographic (variant A)")
    {
        items.push((
            "grid_mapping_name",
            CfValue::String("polar_stereographic".to_owned()),
        ));
        add_cf_polar_stereographic_parameters(crs, items, &operation.parameters, axis_to_metre)?;
    } else if is_operation_method(operation, "9835", "Lambert Cylindrical Equal Area") {
        items.push((
            "grid_mapping_name",
            CfValue::String("lambert_cylindrical_equal_area".to_owned()),
        ));
        add_cf_lambert_cylindrical_equal_area_parameters(
            crs,
            items,
            &operation.parameters,
            axis_to_metre,
        )?;
    }
    Ok(())
}

pub(crate) fn non_deprecated(value: &str) -> Result<Vec<AuthorityObjectInfo>> {
    let state = runtime_state_snapshot()?;
    non_deprecated_with_state(value, &state)
}

pub(crate) fn non_deprecated_with_state(
    value: &str,
    state: &RuntimeState,
) -> Result<Vec<AuthorityObjectInfo>> {
    ensure_thread_caches_current(state);
    let normalized = normalize(value)?;
    NON_DEPRECATED_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let entry = lru_resolve(
            &mut cache,
            NON_DEPRECATED_CACHE_CAPACITY,
            |item| item.definition.as_str() == normalized.as_ref(),
            || {
                let items = ProjObject::new_with_state(&normalized, state)?.non_deprecated()?;
                Ok(CachedAuthorityObjects {
                    definition: normalized.clone().into(),
                    items,
                })
            },
        )?;
        Ok(entry.items.clone())
    })
}

pub(crate) fn catalog_name_matches_with_state(
    name: &str,
    options: &CrsNameMatchOptions,
    state: &RuntimeState,
) -> Result<Vec<String>> {
    ensure_thread_caches_current(state);
    options.validate()?;
    let name = name.trim();
    if name.is_empty() {
        return Err(CrsError::invalid(
            "CRS catalog name must be a non-empty string".to_owned(),
        ));
    }
    CRS_NAME_MATCH_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let entry = lru_resolve(
            &mut cache,
            CRS_NAME_MATCH_CACHE_CAPACITY,
            |item| item.name == name && item.options == *options,
            || {
                Ok(CachedCrsNameMatch {
                    name: name.to_owned(),
                    options: options.clone(),
                    items: catalog_name_matches_uncached(name, options, state)?,
                })
            },
        )?;
        Ok(entry.items.clone())
    })
}

fn catalog_name_matches_uncached(
    name: &str,
    options: &CrsNameMatchOptions,
    state: &RuntimeState,
) -> Result<Vec<String>> {
    let name = cstring(name)?;
    let authority = options.authority.as_deref().map(cstring).transpose()?;
    let types = options
        .kind
        .map(CrsObjectKind::to_proj)
        .into_iter()
        .collect::<Vec<_>>();
    with_proj_context_at(state, |context| {
        // SAFETY: DOC-H. Typed context; strings and optional type slice live for
        // the call; returns caller-owned object list or null.
        let list = unsafe {
            proj_sys::proj_create_from_name(
                context.as_ptr(),
                authority
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                name.as_ptr(),
                if types.is_empty() {
                    ptr::null()
                } else {
                    types.as_ptr()
                },
                types.len(),
                i32::from(options.approximate),
                usize::MAX,
                ptr::null(),
            )
        };
        // SAFETY: non-null returns are uniquely owned by the caller.
        let Some(list) = (unsafe { ProjObjList::try_from_owned(list) }) else {
            if context.errno() == 0 {
                return Ok(Vec::new());
            }
            return Err(CrsError::invalid(proj_context_error_message(context)));
        };
        let count = list.count();
        let mut items = Vec::with_capacity(count.max(0) as usize);
        for index in 0..count {
            if let Some(object) = list.get(context, index) {
                items.push(authority_object_info(context, &object).crs);
            }
        }
        Ok(items)
    })?
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn authority_lookup_keeps_the_resolved_match_until_native_invalidation() {
        let state = runtime_state_snapshot().expect("runtime state");
        let confidence = Confidence::try_new("min_confidence", 70).expect("confidence");
        let expected = to_authority_with_state("EPSG:4326", Some("EPSG"), confidence, &state)
            .expect("authority lookup");
        assert_eq!(expected, Some(("EPSG".to_owned(), "4326".to_owned())));
        assert_eq!(
            authority_if_cached("EPSG:4326", Some("EPSG"), confidence, &state)
                .expect("cached lookup"),
            AuthorityLookup::Cached(expected.clone()),
        );
        let next = RuntimeState {
            generation: state.generation.wrapping_add(1),
            config: std::sync::Arc::clone(&state.config),
        };
        assert_eq!(
            authority_if_cached("EPSG:4326", Some("EPSG"), confidence, &next)
                .expect("stale lookup"),
            AuthorityLookup::Uncached,
        );
        assert_eq!(
            authority_if_cached("EPSG:4326", Some("EPSG"), confidence, &state)
                .expect("original lookup"),
            AuthorityLookup::Cached(expected),
        );
    }
}

#[cfg(test)]
mod unit_token_tests {
    use super::{AxisInfo, cf_proj_units_token};

    const METRE: f64 = 1.0;
    const INTERNATIONAL_FOOT: f64 = 0.3048;
    const US_SURVEY_FOOT: f64 = 1200.0 / 3937.0;
    /// EPSG SQL REAL for 9003 (`unit_of_measure.sql`).
    #[expect(
        clippy::excessive_precision,
        reason = "EPSG SQL REAL spelling for 9003; extra digits document that source rather than a different value"
    )]
    const US_SURVEY_FOOT_SQL: f64 = 3.048_006_096_012_192_411_84e-1;
    /// 15-digit decimal used by WKT/PROJJSON round-trip of 9003.
    const US_SURVEY_FOOT_DECIMAL15: f64 = 0.304_800_609_601_219;

    fn axis(factor: f64, authority: Option<&str>, code: Option<&str>, direction: &str) -> AxisInfo {
        AxisInfo {
            name: Some("Easting".to_owned()),
            abbreviation: Some("E".to_owned()),
            direction: Some(direction.to_owned()),
            unit_name: Some("banana".to_owned()),
            unit_conversion_factor: factor,
            unit_authority: authority.map(str::to_owned),
            unit_code: code.map(str::to_owned),
        }
    }

    #[test]
    fn identified_epsg_units_ignore_names() {
        assert_eq!(
            cf_proj_units_token(&axis(METRE, Some("EPSG"), Some("9001"), "east")),
            Some("m")
        );
        assert_eq!(
            cf_proj_units_token(&axis(
                INTERNATIONAL_FOOT,
                Some("EPSG"),
                Some("9002"),
                "east"
            )),
            Some("ft")
        );
        assert_eq!(
            cf_proj_units_token(&axis(US_SURVEY_FOOT, Some("EPSG"), Some("9003"), "east")),
            Some("us-ft")
        );
    }

    #[test]
    fn identified_us_survey_foot_admits_sql_and_decimal_routes() {
        assert_eq!(
            US_SURVEY_FOOT_SQL
                .to_bits()
                .abs_diff(US_SURVEY_FOOT.to_bits()),
            1
        );
        assert_eq!(
            US_SURVEY_FOOT_DECIMAL15
                .to_bits()
                .abs_diff(US_SURVEY_FOOT.to_bits()),
            3
        );
        assert_eq!(
            cf_proj_units_token(&axis(
                US_SURVEY_FOOT_SQL,
                Some("EPSG"),
                Some("9003"),
                "east"
            )),
            Some("us-ft")
        );
        assert_eq!(
            cf_proj_units_token(&axis(
                US_SURVEY_FOOT_DECIMAL15,
                Some("EPSG"),
                Some("9003"),
                "east"
            )),
            Some("us-ft")
        );
        let beyond = f64::from_bits(US_SURVEY_FOOT.to_bits() + 4);
        assert_eq!(
            cf_proj_units_token(&axis(beyond, Some("EPSG"), Some("9003"), "east")),
            None
        );
    }

    #[test]
    fn unidentified_factors_are_exact_and_ignore_names() {
        assert_eq!(
            cf_proj_units_token(&axis(METRE, None, None, "east")),
            Some("m")
        );
        assert_eq!(
            cf_proj_units_token(&axis(INTERNATIONAL_FOOT, None, None, "east")),
            Some("ft")
        );
        assert_eq!(
            cf_proj_units_token(&axis(US_SURVEY_FOOT, None, None, "east")),
            Some("us-ft")
        );
        assert_eq!(
            cf_proj_units_token(&axis(1.000_000_000_000_5, None, None, "east")),
            None
        );
        assert_eq!(cf_proj_units_token(&axis(2.5, None, None, "east")), None);
        assert_eq!(
            cf_proj_units_token(&axis(f64::NAN, None, None, "east")),
            None
        );
        assert_eq!(cf_proj_units_token(&axis(0.0, None, None, "east")), None);
        assert_eq!(
            cf_proj_units_token(&axis(-1.0, Some("EPSG"), Some("9001"), "east")),
            None
        );
        assert_eq!(
            cf_proj_units_token(&axis(US_SURVEY_FOOT_SQL, None, None, "east")),
            None
        );
        assert_eq!(
            cf_proj_units_token(&axis(US_SURVEY_FOOT_DECIMAL15, None, None, "east")),
            None
        );
    }

    #[test]
    fn inconsistent_identified_code_is_rejected() {
        assert_eq!(
            cf_proj_units_token(&axis(2.5, Some("EPSG"), Some("9003"), "east")),
            None
        );
        assert_eq!(
            cf_proj_units_token(&axis(
                INTERNATIONAL_FOOT,
                Some("EPSG"),
                Some("9001"),
                "east"
            )),
            None
        );
    }
}
