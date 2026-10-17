use super::count_valid;

#[kani::proof]
#[kani::unwind(3)]
fn bitmap_window_counts_match_the_selected_bits() {
    let bytes: [u8; 3] = kani::any();
    let start = usize::from(kani::any::<u8>());
    let end = usize::from(kani::any::<u8>());
    kani::assume(start <= end && end <= 24);
    let bits = u32::from(bytes[0]) | (u32::from(bytes[1]) << 8) | (u32::from(bytes[2]) << 16);
    let selected = (bits >> start) & ((1_u32 << (end - start)) - 1);
    assert_eq!(
        count_valid(&bytes, start, end - start),
        selected.count_ones() as usize
    );
}
