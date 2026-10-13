use super::*;

fn exact_affine_origin_row(row: [f64; 7]) -> f64 {
    let [
        coefficient_x,
        coefficient_y,
        output_origin,
        origin_x,
        origin_y,
        value_x,
        value_y,
    ] = row;
    let mut sum = ExactSum::zero();
    sum.add_product(coefficient_x, value_x);
    sum.add_product(coefficient_y, value_y);
    sum.add_product(output_origin, 1.0);
    sum.subtract_product(coefficient_x, origin_x);
    sum.subtract_product(coefficient_y, origin_y);
    sum.round_to_binary64()
}

fn assert_affine_origin_row_matches_exact(row: [f64; 7], case: &str) -> bool {
    let expected = exact_affine_origin_row(row);
    let [
        coefficient_x,
        coefficient_y,
        output_origin,
        origin_x,
        origin_y,
        value_x,
        value_y,
    ] = row;
    let fast = super::rounding::fast_affine_about_origin_ordinate(
        coefficient_x,
        coefficient_y,
        output_origin,
        origin_x,
        origin_y,
        value_x,
        value_y,
    );
    if let Some(fast) = fast {
        assert_eq!(
            fast.to_bits(),
            expected.to_bits(),
            "fast affine row misrounded {case}"
        );
    }
    assert_eq!(
        super::rounding::affine_about_origin_ordinate(
            coefficient_x,
            coefficient_y,
            output_origin,
            origin_x,
            origin_y,
            value_x,
            value_y,
        )
        .unwrap()
        .to_bits(),
        expected.to_bits(),
        "affine row disagrees with ExactSum for {case}"
    );
    fast.is_some()
}

#[test]
fn affine_scale_keeps_the_independent_reciprocal_axis() {
    let source = Shape::Point(Point::new(1e308, 1e-300).unwrap());
    let transformed = source.scale(0.5, 1.0, (-1e308, 0.0)).unwrap();
    let Shape::Point(point) = transformed else {
        panic!("point scaling must preserve its kind")
    };
    assert_eq!(point.x.to_bits(), 0.0_f64.to_bits());
    assert_eq!(point.y.to_bits(), 1e-300_f64.to_bits());
}
#[test]
fn affine_origin_row_uses_both_input_pivots() {
    assert_eq!(
        super::rounding::affine_about_origin_ordinate(1.0, 0.0, 7.0, 2.0, 7.0, 3.0, 0.0).unwrap(),
        8.0
    );
}
#[test]
fn affine_origin_prepared_rows_match_exact_five_product_rows() {
    let [coefficient_x, coefficient_y, coefficient_z, coefficient_w] = [2.0, -0.5, 0.25, 1.5];
    let (origin_x, origin_y) = (7.0, -5.0);
    let transform =
        super::AffineLinear::new([coefficient_x, coefficient_y, coefficient_z, coefficient_w])
            .unwrap()
            .at((origin_x, origin_y))
            .unwrap();

    for [value_x, value_y] in [
        [11.0, 13.0],
        [origin_x.next_up(), origin_y.next_down()],
        [2.0_f64.powi(500), -(2.0_f64.powi(499))],
        [f64::from_bits(1), -f64::from_bits(1)],
    ] {
        let expected_x = exact_affine_origin_row([
            coefficient_x,
            coefficient_y,
            origin_x,
            origin_x,
            origin_y,
            value_x,
            value_y,
        ]);
        let expected_y = exact_affine_origin_row([
            coefficient_z,
            coefficient_w,
            origin_y,
            origin_x,
            origin_y,
            value_x,
            value_y,
        ]);
        let (actual_x, actual_y) = transform.map_xy(value_x, value_y).unwrap();
        assert_eq!(actual_x.to_bits(), expected_x.to_bits());
        assert_eq!(actual_y.to_bits(), expected_y.to_bits());
    }

    assert!(
        super::rounding::fast_affine_about_origin_ordinate(
            coefficient_x,
            coefficient_y,
            origin_x,
            origin_x,
            origin_y,
            11.0,
            13.0,
        )
        .is_some(),
        "ordinary prepared affine rows should exercise the certified tier"
    );
}
#[test]
fn affine_origin_fast_filter_matches_exact_five_product_rows() {
    // Captured with seed 0x9e3779b97f4a7c15 at reciprocal exponent strata
    // spanning the normal range. Keep the operands as bits for exact replay.
    const RANDOM_ROWS: [[u64; 7]; 16] = [
        [
            0xFFD8_4FB1_7AF4_FD6F,
            0x8014_805E_6EA0_7EE9,
            0x42BD_990E_0FC1_A065,
            0x801E_78A1_9F40_959C,
            0x7FEE_3CFF_3EE1_F126,
            0x801F_2956_7AC5_9520,
            0x7FEF_E218_C577_1AA3,
        ],
        [
            0x783D_C9B8_883C_11B6,
            0x801B_C685_7C93_F490,
            0x393F_59B4_1424_8874,
            0x87B3_CBCF_732D_E3AA,
            0xFFD0_B4BB_6D4C_DE70,
            0x87B7_0A80_F4E3_B996,
            0xFFD1_FA12_D088_9A08,
        ],
        [
            0xEBB0_53B9_2C61_7FD5,
            0x07B5_6501_48BC_7631,
            0x2D87_7C25_5C40_5BD8,
            0x1437_8443_3201_2B5B,
            0xF838_14EB_797F_DC9D,
            0x143E_1020_B940_19B3,
            0xF833_46C3_F611_669F,
        ],
        [
            0x5F32_48F1_A65B_6958,
            0x1432_8F8F_633B_6AE7,
            0xAE73_E0DE_5253_2BC4,
            0xA0B5_8BBD_1FB5_F7D4,
            0x6BBD_6D2C_30F1_7600,
            0xA0BF_8550_4A76_7ACE,
            0x6BB6_AD30_CB6E_15E6,
        ],
        [
            0xD2BD_B7A6_4E66_6019,
            0x20B9_6C99_53FB_F140,
            0xC7C4_4D31_C55B_A94D,
            0xAD3C_074E_8D32_7690,
            0x5F3D_5714_8149_52F2,
            0xAD39_F615_5717_F3B9,
            0x5F30_1CFB_E813_60AA,
        ],
        [
            0xC639_1999_4E70_62AA,
            0x2D30_B835_169D_E770,
            0x3B9F_DE60_1ADC_9055,
            0x39B3_AA27_6D03_4DD1,
            0xD2BB_1118_E7CC_A720,
            0x39BE_12E6_F0A9_0B42,
            0x52BE_A84E_FCAC_9BC0,
        ],
        [
            0x4001_8ED4_D780_ECA4,
            0x39B7_0D1B_5DCD_9B83,
            0xAF83_7AAB_D82D_57BF,
            0x3FE7_98ED_91F0_4982,
            0xC631_2335_3E18_A87C,
            0x3FE2_EC95_BD65_3821,
            0x4638_2C52_9DC8_0E67,
        ],
        [
            0x3FF9_75D1_7EE6_D2C7,
            0xBFE0_8C2F_CD46_3CE9,
            0xCF12_37FD_DA72_0179,
            0x3FF7_B0CF_F391_59F4,
            0x4009_BD02_DD37_9F19,
            0x3FF1_03AB_D742_084F,
            0xC00D_CDD5_546E_424D,
        ],
        [
            0xBFED_8D85_E94F_580B,
            0x3FF9_49A8_0D8A_D901,
            0xBA53_84AD_7709_AEF4,
            0xC00B_D6E8_0622_7885,
            0xBFFA_D757_5A8D_EDC1,
            0x4004_6C56_3EC0_842B,
            0x3FFA_E2F4_FFDF_C6A4,
        ],
        [
            0x39B8_38FA_7C25_E2CB,
            0x4001_B058_3758_395C,
            0x443A_1E4A_FAE6_D0AA,
            0xC631_7C00_3BA9_0071,
            0x3FE6_4450_8DF9_8F59,
            0x4630_A6AB_8DA1_7FC2,
            0xBFE2_77F0_B91E_9239,
        ],
        [
            0xAD3D_F287_DEC6_9FDB,
            0x4630_8F0C_328C_4163,
            0xB4E9_32FB_F8E0_1189,
            0xD2B0_54EC_1AF7_CB2C,
            0x39B2_0A93_50FA_9579,
            0xD2B5_7C73_A5A4_C2A5,
            0x39B2_E6AE_20B6_9887,
        ],
        [
            0xA0B0_08CE_AB2D_A312,
            0xD2B7_EC25_E3A4_34D3,
            0xC95E_364A_F1A7_178C,
            0xDF31_5D6E_F8C2_344B,
            0xAD30_A662_B53C_C6D8,
            0xDF36_04BF_2CB5_EA7D,
            0xAD30_D238_C473_644C,
        ],
        [
            0x9439_88CF_ED18_8B26,
            0xDF32_A8BD_9FF1_98A2,
            0x421A_FEA0_7BF5_74CD,
            0xEBB4_43D4_5679_2A33,
            0xA0B3_3C8F_913C_3E94,
            0x6BB3_80C3_1CC1_D46E,
            0xA0BD_3F3D_413A_35B3,
        ],
        [
            0x07B6_CAD5_7C9A_564C,
            0xEBBB_34CC_DEB9_B095,
            0x493F_653C_0CD0_1EFD,
            0x783B_0138_BE29_5B63,
            0x9430_16F8_6144_669F,
            0xF83F_A13F_C81B_86C5,
            0x1435_B7B3_3B2F_3B44,
        ],
        [
            0x0017_06A7_7196_42E4,
            0x783F_C2A0_80CD_16D0,
            0xAEBB_774B_E6D7_C4C6,
            0x7FD6_8C9B_CECB_5F79,
            0x07BD_A608_8256_CC4C,
            0x7FD5_8CD6_30FF_8060,
            0x07B4_BFA9_2A35_D698,
        ],
        [
            0x801B_AB09_7775_93B4,
            0xFFDA_6E63_F8FE_B755,
            0x2F79_A59F_C1D0_74FD,
            0x7FE7_2BE5_C939_15B3,
            0x0014_CAD7_5892_7226,
            0xFFE6_C686_B048_52E4,
            0x001D_907D_1EE2_C436,
        ],
    ];
    let ulp_at_one = 1.0_f64.next_up().to_bits();
    let term = 3.0 * 2.0_f64.powi(-109);
    let above_tie_by_lost_residual = [
        1.0,
        2.0_f64.powi(-53),
        -(2.0_f64.powi(-107) + 2.0_f64.powi(-117)),
        -term,
        -3.0 * 2.0_f64.powi(-56),
        1.0,
        1.0,
    ];
    assert_eq!(
        exact_affine_origin_row(above_tie_by_lost_residual).to_bits(),
        ulp_at_one,
        "the exact pivot-relative expression lies just above the halfway value"
    );
    assert_affine_origin_row_matches_exact(above_tie_by_lost_residual, "near-tie pivot");

    let ordinary = [2.0, -0.5, 7.0, 3.0, -5.0, 11.0, 13.0];
    assert_eq!(
        exact_affine_origin_row(ordinary).to_bits(),
        14.0_f64.to_bits()
    );
    assert!(assert_affine_origin_row_matches_exact(
        ordinary,
        "two pivots"
    ));

    let catastrophic_cancellation = [
        1.0 + 2.0_f64.powi(-52),
        -1.0,
        0.0,
        0.0,
        0.0,
        2.0_f64.powi(500),
        2.0_f64.powi(500),
    ];
    assert_eq!(
        exact_affine_origin_row(catastrophic_cancellation).to_bits(),
        2.0_f64.powi(448).to_bits()
    );
    assert_affine_origin_row_matches_exact(catastrophic_cancellation, "cancellation");

    for (case, output_origin, expected) in [
        ("exact tie", 0.0, 1.0),
        (
            "just above tie",
            2.0_f64.powi(-106),
            f64::from_bits(ulp_at_one),
        ),
        ("just below tie", -(2.0_f64.powi(-106)), 1.0),
    ] {
        let row = [1.0, 2.0_f64.powi(-53), output_origin, 0.0, 0.0, 1.0, 1.0];
        assert_eq!(exact_affine_origin_row(row).to_bits(), expected.to_bits());
        assert_affine_origin_row_matches_exact(row, case);
    }

    let minimum_subnormal = f64::from_bits(1);
    let subnormal_row = [1.0, 0.0, 0.0, 0.0, 0.0, minimum_subnormal, 0.0];
    assert_eq!(
        exact_affine_origin_row(subnormal_row).to_bits(),
        minimum_subnormal.to_bits()
    );
    assert_affine_origin_row_matches_exact(subnormal_row, "subnormal input and result");

    let near_overflow = [
        2.0_f64.powi(500),
        0.0,
        0.0,
        0.0,
        0.0,
        2.0_f64.powi(500),
        0.0,
    ];
    assert_eq!(
        exact_affine_origin_row(near_overflow).to_bits(),
        2.0_f64.powi(1000).to_bits()
    );
    assert_affine_origin_row_matches_exact(near_overflow, "near-overflow product");

    let splitter_overflow = [
        2.0_f64.powi(1000),
        0.0,
        0.0,
        0.0,
        0.0,
        2.0_f64.powi(-900),
        0.0,
    ];
    assert_eq!(
        exact_affine_origin_row(splitter_overflow).to_bits(),
        2.0_f64.powi(100).to_bits()
    );
    assert_affine_origin_row_matches_exact(
        splitter_overflow,
        "splitter overflow with a finite product",
    );

    let mut certified = 0_usize;
    for (index, bits) in RANDOM_ROWS.into_iter().enumerate() {
        let row = bits.map(f64::from_bits);
        if assert_affine_origin_row_matches_exact(row, &format!("random full-range row {index}")) {
            certified = certified.saturating_add(1);
        }
    }
    assert!(
        certified > 0,
        "the normal-coordinate corpus exercises the fast tier"
    );
}
