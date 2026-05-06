use std::simd::Simd;

pub const RED: usize = 0;
pub const GREEN: usize = 1;
pub const BLUE: usize = 2;
pub const LANES: usize = 4;

const CHANNELS: usize = 3;
pub const MAX_COMPONENTS: usize = 9;
const ROW_BLOCK_SLOTS: usize = MAX_COMPONENTS.div_ceil(LANES);
const COMPONENT_VECTOR_SLOTS: usize = MAX_COMPONENTS * ROW_BLOCK_SLOTS;

pub type V4 = Simd<f32, LANES>;
pub type ComponentVectors = [[V4; COMPONENT_VECTOR_SLOTS]; CHANNELS];
pub type RowVectors = [[V4; ROW_BLOCK_SLOTS]; CHANNELS];

pub const fn component_vectors() -> ComponentVectors {
    [[V4::splat(0.0); COMPONENT_VECTOR_SLOTS]; CHANNELS]
}

pub const fn row_vectors() -> RowVectors {
    [[V4::splat(0.0); ROW_BLOCK_SLOTS]; CHANNELS]
}
