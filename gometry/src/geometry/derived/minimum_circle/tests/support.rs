fn realization_budget() -> ExpansionBudget {
    ExpansionBudget::new("minimum_bounding_circle", "realization")
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "the Fisher-Yates upper bound iterates from length down through two"
)]
fn shuffle_with_seed(length: usize, seed: u64) -> Vec<usize> {
    let mut state = seed ^ length as u64;
    let mut order: Vec<usize> = (0..length).collect();
    for index in (1..length).rev() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let upper = index
            .checked_add(1)
            .expect("Fisher-Yates index is below the order length");
        order.swap(index, (state >> 33) as usize % upper);
    }
    order
}

fn fixed_shuffle(length: usize) -> Vec<usize> {
    shuffle_with_seed(length, 0x9E37_79B9_7F4A_7C15)
}
