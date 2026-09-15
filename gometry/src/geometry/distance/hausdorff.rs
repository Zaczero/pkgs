//! Continuous planar Hausdorff distance with a certified error bound.

mod bounded;

pub(crate) use bounded::{
    distance as hausdorff_distance, line_columns as hausdorff_distance_line_columns,
    line_columns_batch as hausdorff_distance_line_columns_batch,
};
