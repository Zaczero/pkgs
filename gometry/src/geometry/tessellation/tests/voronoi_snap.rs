fn vertex_key(point: Point) -> (u64, u64, Option<u64>, Option<u64>) {
    (
        point.x.to_bits(),
        point.y.to_bits(),
        point.z().map(f64::to_bits),
        point.m().map(f64::to_bits),
    )
}

fn brute_snap_sites(sites: &[super::Site], radius: f64) -> Vec<super::Site> {
    let mut survivors: Vec<super::Site> = Vec::new();
    for site in sites {
        let mut snapped = false;
        for survivor in &survivors {
            if super::exact::point::distance_within_points(
                &survivor.exact_point,
                &site.exact_point,
                radius,
            )
            .expect("finite binary64 site distances stay within exact dyadic bounds")
            {
                snapped = true;
                break;
            }
        }
        if !snapped {
            survivors.push(site.clone());
        }
    }
    survivors
}

fn assert_snap_matches_brute(sites: &[super::Site], radius: f64) -> Vec<super::Site> {
    let expected = brute_snap_sites(sites, radius);
    let actual = super::snap_sites(sites, radius).unwrap();
    let signature = |sites: &[super::Site]| {
        sites
            .iter()
            .map(|site| (site.id, site.point.x.to_bits(), site.point.y.to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(signature(&actual), signature(&expected));
    actual
}

#[test]
fn canonical_voronoi_sites_normalize_zero_and_globally_deduplicate_snaps() {
    let input = vec![
        xy(-0.0, 0.0),
        xy(0.0, 1.0),
        xy(0.09, 0.0),
        xy(0.18, 0.0),
        xy(1.0, 0.0),
    ];
    let restored = canonical_voronoi_sites(input.clone(), 0.1).unwrap();
    let mut reversed = input;
    reversed.reverse();
    let mutated_order = canonical_voronoi_sites(reversed, 0.1).unwrap();
    let signature = |sites: &[super::Site]| {
        sites
            .iter()
            .map(|site| (site.point.x.to_bits(), site.point.y.to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(signature(&restored), signature(&mutated_order));
    assert_eq!(restored.len(), 4);
    assert_eq!(restored[0].point.x.to_bits(), 0.0_f64.to_bits());
    assert_eq!(
        restored.iter().map(|site| site.id).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
}

#[test]
fn voronoi_snap_matches_exact_reference_at_strict_radius_neighbors() {
    let below = 1.0_f64.next_down();
    let above = 1.0_f64.next_up();
    let sites = [
        super::Site::raw(0, xy(0.0, 0.0), 0),
        super::Site::raw(1, xy(0.0, below), 1),
        super::Site::raw(2, xy(0.0, 1.0), 2),
        super::Site::raw(3, xy(0.0, above), 3),
    ];

    assert_eq!(
        assert_snap_matches_brute(&sites, 1.0)
            .iter()
            .map(|site| site.id)
            .collect::<Vec<_>>(),
        [0, 2]
    );
}

#[test]
fn voronoi_snap_matches_exact_reference_at_subnormal_and_extreme_bounds() {
    let minimum = f64::from_bits(1);
    let subnormal_sites = [
        super::Site::raw(0, xy(0.0, 0.0), 0),
        super::Site::raw(1, xy(minimum, 0.0), 1),
        super::Site::raw(2, xy(2.0 * minimum, 0.0), 2),
    ];
    assert_eq!(
        assert_snap_matches_brute(&subnormal_sites, minimum)
            .iter()
            .map(|site| site.id)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );

    let maximum = f64::MAX;
    let overflowing_y_sites = [
        super::Site::raw(0, xy(0.0, -maximum), 0),
        super::Site::raw(1, xy(0.0, maximum), 1),
    ];
    assert_eq!(
        assert_snap_matches_brute(&overflowing_y_sites, maximum)
            .iter()
            .map(|site| site.id)
            .collect::<Vec<_>>(),
        [0, 1]
    );

    let opposite_x_sites = [
        super::Site::raw(0, xy(-maximum, 0.0), 0),
        super::Site::raw(1, xy(0.0, 0.0), 1),
        super::Site::raw(2, xy(maximum, 0.0), 2),
    ];
    assert_eq!(
        assert_snap_matches_brute(&opposite_x_sites, maximum)
            .iter()
            .map(|site| site.id)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
}

#[test]
fn voronoi_snap_matches_exact_reference_when_y_order_differs() {
    let sites = [
        super::Site::raw(0, xy(0.0, 0.8), 0),
        super::Site::raw(1, xy(0.8, 0.0), 1),
        super::Site::raw(2, xy(0.81, 0.4), 2),
    ];

    assert_eq!(
        assert_snap_matches_brute(&sites, 1.0)
            .iter()
            .map(|site| site.id)
            .collect::<Vec<_>>(),
        [0, 1]
    );
}

#[test]
fn canonical_voronoi_keeps_4097_equal_x_sparse_y_sites_in_dense_id_order() {
    let points = (0..4_097)
        .map(|site| xy(0.0, 2.0 * f64::from(site)))
        .collect::<Vec<_>>();
    let unsnapped = canonical_voronoi_sites(points.clone(), 0.0).unwrap();
    let snapped = canonical_voronoi_sites(points, 1.0).unwrap();
    let signature = |sites: &[super::Site]| {
        sites
            .iter()
            .map(|site| (site.id, site.point.x.to_bits(), site.point.y.to_bits()))
            .collect::<Vec<_>>()
    };

    assert_eq!(signature(&snapped), signature(&unsnapped));
    assert_eq!(
        snapped.iter().map(|site| site.id).collect::<Vec<_>>(),
        (0..4_097).collect::<Vec<_>>()
    );
}

#[test]
fn exact_snap_does_not_use_a_rounded_axis_rejection() {
    let sites = [
        super::Site::raw(0, xy(0.0, 1e20), 0),
        super::Site::raw(1, xy(0.0, 1.0), 1),
    ];
    assert_eq!(
        (sites[0].point.y - sites[1].point.y).abs().to_bits(),
        1e20_f64.to_bits()
    );
    let survivors = super::snap_sites(&sites, 1e20).unwrap();
    assert_eq!(survivors.iter().map(|site| site.id).collect::<Vec<_>>(), [
        0
    ]);
}
