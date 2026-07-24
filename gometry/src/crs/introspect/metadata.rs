use crate::crs::*;

pub(crate) fn area_of_use(
    context: *mut proj_sys::PJ_CONTEXT,
    object: *const proj_sys::PJ,
) -> Option<AreaOfUse> {
    let mut west = 0.0;
    let mut south = 0.0;
    let mut east = 0.0;
    let mut north = 0.0;
    let mut name = ptr::null();
    // SAFETY: output pointers reference initialized local storage and object is a
    // valid PROJ object.
    let ok = unsafe {
        proj_sys::proj_get_area_of_use(
            context,
            object,
            &raw mut west,
            &raw mut south,
            &raw mut east,
            &raw mut north,
            &raw mut name,
        )
    };
    let sentinel = (-1000.0_f64).to_bits();
    if ok == 0
        || [west, south, east, north]
            .iter()
            .all(|value| value.to_bits() == sentinel)
    {
        return None;
    }
    Some(AreaOfUse {
        west,
        south,
        east,
        north,
        name: string_from_ptr(name),
    })
}

pub(crate) fn domain_infos(
    context: *mut proj_sys::PJ_CONTEXT,
    object: *const proj_sys::PJ,
) -> Vec<DomainInfo> {
    // SAFETY: object is valid for the duration of metadata inspection.
    let count = unsafe { proj_sys::proj_get_domain_count(object) };
    if count <= 0 {
        return Vec::new();
    }
    (0..count)
        .map(|index| DomainInfo {
            // SAFETY: object and domain index are valid for the metadata call.
            scope: string_from_ptr(unsafe { proj_sys::proj_get_scope_ex(object, index) }),
            area_of_use: area_of_use_ex(context, object, index),
        })
        .collect()
}

pub(crate) fn area_of_use_ex(
    context: *mut proj_sys::PJ_CONTEXT,
    object: *const proj_sys::PJ,
    domain_index: i32,
) -> Option<AreaOfUse> {
    let mut west = 0.0;
    let mut south = 0.0;
    let mut east = 0.0;
    let mut north = 0.0;
    let mut name = ptr::null();
    // SAFETY: output pointers reference initialized local storage and object is a
    // valid PROJ object.
    let ok = unsafe {
        proj_sys::proj_get_area_of_use_ex(
            context,
            object,
            domain_index,
            &raw mut west,
            &raw mut south,
            &raw mut east,
            &raw mut north,
            &raw mut name,
        )
    };
    (ok != 0).then(|| AreaOfUse {
        west,
        south,
        east,
        north,
        name: string_from_ptr(name),
    })
}

pub(crate) fn coordinate_system_type(
    context: *mut proj_sys::PJ_CONTEXT,
    object: *const proj_sys::PJ,
) -> Option<&'static str> {
    // SAFETY: object is a valid PROJ CRS object. PROJ returns an owned coordinate
    // system object or null when unavailable.
    let coordinate_system = unsafe { proj_sys::proj_crs_get_coordinate_system(context, object) };
    if coordinate_system.is_null() {
        return None;
    }
    // SAFETY: coordinate_system is valid until destroyed below.
    let type_ = unsafe { proj_sys::proj_cs_get_type(context, coordinate_system) };
    // SAFETY: coordinate_system was returned by PROJ and is destroyed exactly once.
    unsafe {
        proj_sys::proj_destroy(coordinate_system);
    }
    Some(coordinate_system_type_name(type_))
}

pub(crate) fn axes(
    context: *mut proj_sys::PJ_CONTEXT,
    object: *const proj_sys::PJ,
) -> Vec<AxisInfo> {
    // SAFETY: object is a valid PROJ CRS object. PROJ returns an owned coordinate
    // system object or null when unavailable.
    let coordinate_system = unsafe { proj_sys::proj_crs_get_coordinate_system(context, object) };
    if coordinate_system.is_null() {
        return Vec::new();
    }
    // SAFETY: coordinate_system is valid until destroyed below.
    let count = unsafe { proj_sys::proj_cs_get_axis_count(context, coordinate_system) };
    if count <= 0 {
        // SAFETY: coordinate_system was returned by PROJ and is destroyed exactly once.
        unsafe {
            proj_sys::proj_destroy(coordinate_system);
        }
        return Vec::new();
    }
    let mut axes = Vec::with_capacity(count as usize);
    for index in 0..count {
        let mut name = ptr::null();
        let mut abbreviation = ptr::null();
        let mut direction = ptr::null();
        let mut unit_conversion_factor = f64::NAN;
        let mut unit_name = ptr::null();
        let mut unit_auth_name = ptr::null();
        let mut unit_code = ptr::null();
        // SAFETY: output pointers reference initialized local storage and the
        // coordinate system object remains valid during the call.
        let ok = unsafe {
            proj_sys::proj_cs_get_axis_info(
                context,
                coordinate_system,
                index,
                &raw mut name,
                &raw mut abbreviation,
                &raw mut direction,
                &raw mut unit_conversion_factor,
                &raw mut unit_name,
                &raw mut unit_auth_name,
                &raw mut unit_code,
            )
        };
        if ok != 0 {
            axes.push(AxisInfo {
                name: string_from_ptr(name),
                abbreviation: string_from_ptr(abbreviation),
                direction: string_from_ptr(direction),
                unit_name: string_from_ptr(unit_name),
                unit_conversion_factor,
            });
        }
    }
    // SAFETY: coordinate_system was returned by PROJ and is destroyed exactly once.
    unsafe {
        proj_sys::proj_destroy(coordinate_system);
    }
    axes
}

pub(crate) fn compound_axis_metadata(
    context: *mut proj_sys::PJ_CONTEXT,
    crs: *const proj_sys::PJ,
) -> Option<(Vec<AxisInfo>, Vec<&'static str>)> {
    let mut axes = Vec::new();
    let mut axis_order = Vec::new();
    for index in 0..32 {
        // SAFETY: crs is a valid CRS object. PROJ returns an owned sub-CRS
        // object or null when the index is out of range/not applicable.
        let sub_crs = unsafe { proj_sys::proj_crs_get_sub_crs(context, crs, index) };
        if sub_crs.is_null() {
            break;
        }
        let coordinate_system = coordinate_system_type(context, sub_crs);
        let sub_axes = self::axes(context, sub_crs);
        axis_order.extend(
            sub_axes
                .iter()
                .map(|axis| axis_role(coordinate_system, axis)),
        );
        axes.extend(sub_axes);
        // SAFETY: sub_crs is owned by this loop iteration and no longer used.
        unsafe {
            proj_sys::proj_destroy(sub_crs);
        }
    }
    (!axes.is_empty()).then_some((axes, axis_order))
}

pub(crate) const fn coordinate_system_type_name(
    type_: proj_sys::PJ_COORDINATE_SYSTEM_TYPE,
) -> &'static str {
    match type_ {
        proj_sys::PJ_COORDINATE_SYSTEM_TYPE_PJ_CS_TYPE_CARTESIAN => "cartesian",
        proj_sys::PJ_COORDINATE_SYSTEM_TYPE_PJ_CS_TYPE_ELLIPSOIDAL => "ellipsoidal",
        proj_sys::PJ_COORDINATE_SYSTEM_TYPE_PJ_CS_TYPE_VERTICAL => "vertical",
        proj_sys::PJ_COORDINATE_SYSTEM_TYPE_PJ_CS_TYPE_SPHERICAL => "spherical",
        proj_sys::PJ_COORDINATE_SYSTEM_TYPE_PJ_CS_TYPE_ORDINAL => "ordinal",
        proj_sys::PJ_COORDINATE_SYSTEM_TYPE_PJ_CS_TYPE_PARAMETRIC => "parametric",
        proj_sys::PJ_COORDINATE_SYSTEM_TYPE_PJ_CS_TYPE_DATETIMETEMPORAL => "datetime_temporal",
        proj_sys::PJ_COORDINATE_SYSTEM_TYPE_PJ_CS_TYPE_TEMPORALCOUNT => "temporal_count",
        proj_sys::PJ_COORDINATE_SYSTEM_TYPE_PJ_CS_TYPE_TEMPORALMEASURE => "temporal_measure",
        _ => "unknown",
    }
}

pub(crate) fn axis_role(coordinate_system: Option<&'static str>, axis: &AxisInfo) -> &'static str {
    let direction = axis.direction.as_deref().unwrap_or_default();
    match coordinate_system {
        Some("ellipsoidal") => match direction {
            "north" | "south" => "lat",
            "east" | "west" => "lon",
            "up" | "down" => "height",
            _ => "other",
        },
        Some("cartesian") => {
            cartesian_axis_role_from_abbreviation(axis).unwrap_or(match direction {
                "east" | "west" => "x",
                "north" | "south" => "y",
                "up" | "down" => "z",
                _ => "other",
            })
        },
        Some("vertical") => "height",
        _ => match direction {
            "east" | "west" => "x",
            "north" | "south" => "y",
            "up" | "down" => "z",
            _ => "other",
        },
    }
}

pub(crate) fn cartesian_axis_role_from_abbreviation(axis: &AxisInfo) -> Option<&'static str> {
    match axis.abbreviation.as_deref() {
        Some("X" | "x" | "E" | "e") => Some("x"),
        Some("Y" | "y" | "N" | "n") => Some("y"),
        Some("Z" | "z" | "H" | "h") => Some("z"),
        _ => None,
    }
}
