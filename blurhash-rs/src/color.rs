use std::simd::Simd;

pub const RED: usize = 0;
pub const GREEN: usize = 1;
pub const BLUE: usize = 2;

const CHANNELS: usize = 3;
const COMPONENT_VECTOR_SLOTS: usize = 27;
const ROW_BLOCK_SLOTS: usize = 3;

pub type V4 = Simd<f32, 4>;
pub type ComponentVectors = [[V4; COMPONENT_VECTOR_SLOTS]; CHANNELS];
pub type RowVectors = [[V4; ROW_BLOCK_SLOTS]; CHANNELS];

pub const fn component_vectors() -> ComponentVectors {
    [[V4::splat(0.0); COMPONENT_VECTOR_SLOTS]; CHANNELS]
}

pub const fn row_vectors() -> RowVectors {
    [[V4::splat(0.0); ROW_BLOCK_SLOTS]; CHANNELS]
}
