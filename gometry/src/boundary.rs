pub(crate) mod buffer_endian;
pub(crate) mod convert;
pub(crate) mod coordinate_input;
pub(crate) mod coordinates;
pub(crate) mod geographic;
pub(crate) mod input;
pub(crate) mod metadata;
pub(crate) mod reserve;

pub(crate) use buffer_endian::{buffer_copy_to_slice_u64, buffer_to_vec_u64};
pub(crate) use convert::{coordinates_object, json_text_to_py, json_to_py, point_tuple};
pub(crate) use coordinate_input::{
    CoordinateInput, CrsCoordinateArgs, DefaultedF64Input, DefaultedI64Input, F64Param, I64Param,
    Zt, ZtLaneRefs, ZtLanes, ZtValues, broadcast_coordinate_group, broadcast_coordinate_input,
    broadcast_crs_coordinate_inputs, coordinate_column_values, coordinate_column_values_exact,
    coordinate_column_values_from_buffer, coordinate_float, coordinate_input_with_expected,
    coordinate_inputs_are_scalar, coordinate_sequence_len_hint, coordinate_values_at_most,
    coordinate_values_exact, optional_coordinate_column_values,
    optional_coordinate_column_values_from_buffer, optional_coordinate_input_with_expected,
    parameter_input_with_expected, parameter_values, parameter_values_at_most,
    parameter_values_exact, reject_multidimensional_coordinate_array, try_coordseq_from_nd_buffer,
};
pub(crate) use geographic::{point_xy, validate_lonlat_shape};
pub(crate) use input::{
    GridOrigin, OriginSpec, TransformOptionArgs, angle_radians, cdt_refinement_values,
    coordinate_epoch_option, f64_required, f64_scalar_named, finite_coordinate_required,
    finite_f64_required, non_negative_int, parse_affine_matrix, parse_area, parse_cdt_refinement,
    parse_grid_size, parse_sample_count, parse_sample_seed, positive_int, py_i64_bounded,
    py_i64_required, py_index_check, py_index_object, py_isize_required, py_u64_if_index,
    py_u64_required, py_usize_required, strict_i32, strict_i32_overflow, strict_i64,
    strict_i64_overflow, strict_i128, strict_optional_i64, strict_u64,
    validate_subdivide_max_vertices,
};
#[cfg(test)]
pub(crate) use metadata::crs_arc_static;
pub(crate) use metadata::{
    Frame, FrameAdoption, FrameEdit, FrameError, GeometryTransformFrame, crs_arc,
    crs_operationally_equal, epoch_label, wgs84_crs,
};
pub(crate) use reserve::{
    CsrBuilder, advisory_len, check_python_signals, collect_bool_mask, collect_bytes_rows,
    collect_i64_sequence, collect_py_iter, collect_py_iter_at_most_from_iter,
    collect_py_iter_exact, collect_py_iter_exact_from_iter, collect_py_iter_from_iter,
    collect_py_strings, collect_sequence_items, collect_u64_sequence, collect_usize_sequence,
    copy_one_byte_buffer, is_one_byte_buffer, py_iter_or_error, try_boxed_str, try_collect_results,
    try_push, try_push_heap, try_push_pair, try_push_vec, try_reserve_heap, try_reserve_hint,
    try_reserve_pair, try_reserve_vec, try_string_from_str, try_vec_with_capacity,
    try_vec_with_capacity_hint, with_one_byte_buffer,
};
