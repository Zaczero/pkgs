use std::ptr;

use crate::crs::{AreaOfUse, AxisInfo, DomainInfo, OwnedPj, ProjContext, copy_proj_c_string};

pub(crate) fn area_of_use(context: &ProjContext, object: &OwnedPj) -> Option<AreaOfUse> {
    let mut west = 0.0;
    let mut south = 0.0;
    let mut east = 0.0;
    let mut north = 0.0;
    let mut name = ptr::null();
    // SAFETY: DOC-H. Typed live context/object on creating thread; output
    // pointers reference initialized local storage exclusive for the call.
    // Returned name is object-lifetime; copied immediately. PROJ invokes no
    // Python callback.
    let ok = unsafe {
        proj_sys::proj_get_area_of_use(
            context.as_ptr(),
            object.as_ptr(),
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
        name: proj_c_string!(name),
    })
}

pub(crate) fn domain_infos(context: &ProjContext, object: &OwnedPj) -> Vec<DomainInfo> {
    // SAFETY: DOC-H. Typed live object on creating thread.
    let count = unsafe { proj_sys::proj_get_domain_count(object.as_ptr()) };
    if count <= 0 {
        return Vec::new();
    }
    (0..count)
        .map(|index| DomainInfo {
            // SAFETY: DOC-H. Object live; index within domain_count.
            scope: unsafe {
                copy_proj_c_string(proj_sys::proj_get_scope_ex(object.as_ptr(), index))
            },
            area_of_use: area_of_use_ex(context, object, index),
        })
        .collect()
}

pub(crate) fn area_of_use_ex(
    context: &ProjContext,
    object: &OwnedPj,
    domain_index: i32,
) -> Option<AreaOfUse> {
    let mut west = 0.0;
    let mut south = 0.0;
    let mut east = 0.0;
    let mut north = 0.0;
    let mut name = ptr::null();
    // SAFETY: DOC-H. Typed owners; domain_index from reported count; OUT slots
    // exclusive locals; name copied immediately.
    let ok = unsafe {
        proj_sys::proj_get_area_of_use_ex(
            context.as_ptr(),
            object.as_ptr(),
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
        name: proj_c_string!(name),
    })
}

pub(crate) fn coordinate_system_type(
    context: &ProjContext,
    object: &OwnedPj,
) -> Option<&'static str> {
    // SAFETY: DOC-H. Typed owners; returns uniquely owned CS or null.
    let coordinate_system =
        unsafe { proj_sys::proj_crs_get_coordinate_system(context.as_ptr(), object.as_ptr()) };
    // SAFETY: non-null returns are uniquely owned by the caller.
    let coordinate_system = unsafe { OwnedPj::try_from_owned(coordinate_system)? };
    // SAFETY: DOC-H. Typed context + owned CS live on creating thread.
    let type_ = unsafe { proj_sys::proj_cs_get_type(context.as_ptr(), coordinate_system.as_ptr()) };
    Some(coordinate_system_type_name(type_))
}

pub(crate) fn axes(context: &ProjContext, object: &OwnedPj) -> Vec<AxisInfo> {
    // SAFETY: DOC-H. Typed owners; returns uniquely owned CS or null.
    let coordinate_system =
        unsafe { proj_sys::proj_crs_get_coordinate_system(context.as_ptr(), object.as_ptr()) };
    // SAFETY: non-null returns are uniquely owned.
    let Some(coordinate_system) = (unsafe { OwnedPj::try_from_owned(coordinate_system) }) else {
        return Vec::new();
    };
    // SAFETY: DOC-H. Live owned CS + typed context.
    let count =
        unsafe { proj_sys::proj_cs_get_axis_count(context.as_ptr(), coordinate_system.as_ptr()) };
    if count <= 0 {
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
        // SAFETY: DOC-H. Live CS; index in 0..count; OUT slots exclusive locals;
        // returned C strings are object-lifetime and copied immediately.
        let ok = unsafe {
            proj_sys::proj_cs_get_axis_info(
                context.as_ptr(),
                coordinate_system.as_ptr(),
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
                name: proj_c_string!(name),
                abbreviation: proj_c_string!(abbreviation),
                direction: proj_c_string!(direction),
                unit_name: proj_c_string!(unit_name),
                unit_conversion_factor,
            });
        }
    }
    axes
}

pub(crate) fn compound_axis_metadata(
    context: &ProjContext,
    crs: &OwnedPj,
) -> Option<(Vec<AxisInfo>, Vec<&'static str>)> {
    let mut axes = Vec::new();
    let mut axis_order = Vec::new();
    for index in 0..32 {
        // SAFETY: DOC-H. Typed owners; returns uniquely owned sub-CRS or null.
        let sub_crs =
            unsafe { proj_sys::proj_crs_get_sub_crs(context.as_ptr(), crs.as_ptr(), index) };
        // SAFETY: non-null returns are uniquely owned; Drop cleans each iteration.
        let Some(sub_crs) = (unsafe { OwnedPj::try_from_owned(sub_crs) }) else {
            break;
        };
        let coordinate_system = coordinate_system_type(context, &sub_crs);
        let sub_axes = self::axes(context, &sub_crs);
        axis_order.extend(
            sub_axes
                .iter()
                .map(|axis| axis_role(coordinate_system, axis)),
        );
        axes.extend(sub_axes);
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
