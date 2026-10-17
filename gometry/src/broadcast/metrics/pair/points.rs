use super::{
    FrameDependentCaches, GeographicFrame, GeometryArrayStorage, PackedColumns, PackedStorage,
    Point, PointRows, PreparedPlan, PyGeometryArray, crs, geographic,
};

pub(crate) fn geodesic_point_columns_to_shape_values(
    crs: &str,
    xs: &[f64],
    ys: &[f64],
    shape: &PreparedPlan,
    frame_cache: &FrameDependentCaches,
    frame: GeographicFrame,
    state: &crs::RuntimeState,
) -> crate::error::Result<Vec<f64>> {
    crs::with_resolved_ellipsoid_metric_at(crs, &[shape.shape()], state, |crs, metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        shape.geodesic_distance_points(
            frame_cache,
            xs.iter().copied().zip(ys.iter().copied()),
            crs,
            semi_major,
            flattening,
            metric,
            frame,
        )
    })
}

pub(crate) fn geodesic_point_columns_dwithin_shape_values(
    crs: &str,
    xs: &[f64],
    ys: &[f64],
    shape: &PreparedPlan,
    frame_cache: &FrameDependentCaches,
    distance: f64,
    frame: GeographicFrame,
    state: &crs::RuntimeState,
) -> crate::error::Result<Vec<bool>> {
    crs::with_resolved_ellipsoid_metric_at(crs, &[shape.shape()], state, |crs, metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        shape.geodesic_dwithin_points(
            frame_cache,
            xs.iter().copied().zip(ys.iter().copied()),
            crs,
            semi_major,
            flattening,
            metric,
            distance,
            frame,
        )
    })
}

/// Array sibling of [`lonlat_shape`]: validated lon/lat points at logical rows.
pub(crate) enum LonLatPointRows<'a> {
    Source(PointRows<'a>),
    Columns {
        xs: Vec<f64>,
        ys: Vec<f64>,
    },
    CompactColumns {
        xs: Vec<f64>,
        ys: Vec<f64>,
        present_rows: Vec<usize>,
        logical_len: usize,
    },
}

impl LonLatPointRows<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Source(points) => points.len(),
            Self::Columns { xs, .. } => xs.len(),
            Self::CompactColumns { logical_len, .. } => *logical_len,
        }
    }

    pub(crate) fn for_each_present(
        &self,
        mut is_missing: impl FnMut(usize) -> bool,
        mut visit: impl FnMut(usize, Point),
    ) {
        match self {
            Self::Source(points) => {
                for row in 0..points.len() {
                    if !is_missing(row) {
                        visit(row, points.get(row));
                    }
                }
            },
            Self::Columns { xs, ys } => {
                for (row, (&x, &y)) in xs.iter().zip(ys).enumerate() {
                    if !is_missing(row) {
                        visit(row, Point::new_raw_xy(x, y));
                    }
                }
            },
            Self::CompactColumns {
                xs,
                ys,
                present_rows,
                ..
            } => {
                for ((&row, &x), &y) in present_rows.iter().zip(xs).zip(ys) {
                    visit(row, Point::new_raw_xy(x, y));
                }
            },
        }
    }
}

/// Gather error provenance. Callers format: grid without a row note, geocode
/// with [`crate::note_array_row`] on [`Self::Row`].
#[derive(Debug)]
pub(crate) enum LonLatPointsError {
    Type,
    Setup(crate::error::Error),
    Row(usize, crate::error::Error),
}

fn lonlat_domain(row: usize, x: f64, y: f64) -> Result<(), LonLatPointsError> {
    geographic::validate_lonlat_xy(x, y).map_err(|error| LonLatPointsError::Row(row, error))
}

fn transform_failure_to_row(
    failure: crate::crs::TransformFailure,
    present_rows: &[usize],
) -> LonLatPointsError {
    let crate::crs::TransformFailure { error, coordinate } = failure;
    match coordinate {
        Some(coordinate) => LonLatPointsError::Row(
            *present_rows
                .get(coordinate)
                .expect("transform failure coordinate belongs to the present-row batch"),
            error,
        ),
        None => LonLatPointsError::Setup(error),
    }
}

fn transform_present(
    crs: &str,
    xs: &mut [f64],
    ys: &mut [f64],
    present: &[usize],
) -> Result<(), LonLatPointsError> {
    let mut px = Vec::with_capacity(present.len());
    let mut py = Vec::with_capacity(present.len());
    for &row in present {
        px.push(
            *xs.get(row)
                .expect("present logical row is inside longitude columns"),
        );
        py.push(
            *ys.get(row)
                .expect("present logical row is inside latitude columns"),
        );
    }
    let plan =
        crate::crs::TransformPlan::new(crs, "EPSG:4326").map_err(LonLatPointsError::Setup)?;
    plan.transform_coordinates_indexed(&mut px, &mut py, crate::Zt::None)
        .map_err(|failure| transform_failure_to_row(failure, present))?;
    for ((&row, x), y) in present.iter().zip(px).zip(py) {
        lonlat_domain(row, x, y)?;
        *xs.get_mut(row)
            .expect("present logical row is inside longitude columns") = x;
        *ys.get_mut(row)
            .expect("present logical row is inside latitude columns") = y;
    }
    Ok(())
}

fn transform_compact_present(
    crs: &str,
    xs: &mut [f64],
    ys: &mut [f64],
    present_rows: &[usize],
) -> Result<(), LonLatPointsError> {
    let plan =
        crate::crs::TransformPlan::new(crs, "EPSG:4326").map_err(LonLatPointsError::Setup)?;
    plan.transform_coordinates_indexed(xs, ys, crate::Zt::None)
        .map_err(|failure| transform_failure_to_row(failure, present_rows))?;
    for (index, &row) in present_rows.iter().enumerate() {
        lonlat_domain(
            row,
            *xs.get(index)
                .expect("transformed longitude column matches present rows"),
            *ys.get(index)
                .expect("transformed latitude column matches present rows"),
        )?;
    }
    Ok(())
}

fn compact_lonlat_points(
    array: &PyGeometryArray,
) -> Result<LonLatPointRows<'_>, LonLatPointsError> {
    let projected_crs = array.crs_str().filter(|crs| !crs::is_wgs84_lonlat(crs));
    let Some(PackedColumns::Points(points)) = array.row_owner().packed_columns() else {
        unreachable!("compact nullable point rows expose packed point columns");
    };
    let point_rows = points.rows();
    let present_count = point_rows.present_count();
    let mut xs = Vec::with_capacity(present_count);
    let mut ys = Vec::with_capacity(present_count);
    let mut present_rows = Vec::with_capacity(present_count);
    for (logical, physical) in point_rows.cursor() {
        let Some(physical) = physical else {
            continue;
        };
        let (x, y) = points.physical_xy_at(physical);
        xs.push(x);
        ys.push(y);
        present_rows.push(logical);
    }
    let Some(crs) = projected_crs else {
        for (index, &row) in present_rows.iter().enumerate() {
            lonlat_domain(
                row,
                *xs.get(index)
                    .expect("compact longitude column matches present rows"),
                *ys.get(index)
                    .expect("compact latitude column matches present rows"),
            )?;
        }
        return Ok(LonLatPointRows::CompactColumns {
            xs,
            ys,
            present_rows,
            logical_len: array.len(),
        });
    };
    if !present_rows.is_empty() {
        transform_compact_present(crs, &mut xs, &mut ys, &present_rows)?;
    }
    Ok(LonLatPointRows::CompactColumns {
        xs,
        ys,
        present_rows,
        logical_len: array.len(),
    })
}

/// Resolve every logical row of `array` to a validated lon/lat point.
pub(crate) fn lonlat_points(
    array: &PyGeometryArray,
) -> Result<LonLatPointRows<'_>, LonLatPointsError> {
    if array.is_compact_nullable() {
        if !matches!(
            array.storage(),
            GeometryArrayStorage::Packed(PackedStorage::Points { .. })
        ) {
            return Err(LonLatPointsError::Type);
        }
        return compact_lonlat_points(array);
    }
    let points = array
        .storage()
        .point_rows()
        .ok_or(LonLatPointsError::Type)?;
    let projected_crs = array.crs_str().filter(|crs| !crs::is_wgs84_lonlat(crs));
    let crs = match projected_crs {
        Some(crs) if !points.is_empty() => crs,
        _ => {
            for (row, point) in points.iter().enumerate() {
                if array.is_row_missing(row) {
                    continue;
                }
                lonlat_domain(row, point.x, point.y)?;
            }
            return Ok(LonLatPointRows::Source(points));
        },
    };
    let mut xs = Vec::with_capacity(points.len());
    let mut ys = Vec::with_capacity(points.len());
    for point in points.iter() {
        xs.push(point.x);
        ys.push(point.y);
    }
    let mut present = Vec::with_capacity(points.len());
    for row in 0..points.len() {
        if !array.is_row_missing(row) {
            present.push(row);
        }
    }
    if !present.is_empty() {
        transform_present(crs, &mut xs, &mut ys, &present)?;
    }
    Ok(LonLatPointRows::Columns { xs, ys })
}

/// CRS-aware minimum distance: native linear units for projected geometry,
/// raw coordinate units for CRS-free geometry, and geodesic meters on the CRS
/// ellipsoid (between the closest pair of points) for geographic geometry.
///
#[cfg(test)]
mod lonlat_points_tests {
    use super::{GeometryArrayStorage, LonLatPointRows, LonLatPointsError, lonlat_points};
    use crate::array::PyGeometryArray;
    use crate::crs::frame::{Frame, crs_arc_static};
    use crate::geometry::storage::ValidityMask;
    use crate::geometry::{CoordSeq, Point, Shape};
    use crate::io::parse_wkt;

    fn packed_xy(xs: Vec<f64>, ys: Vec<f64>, frame: Frame) -> PyGeometryArray {
        PyGeometryArray::packed_points(CoordSeq::try_from_vecs(xs, ys, None, None).unwrap(), frame)
            .unwrap()
    }

    fn assert_transform_error_row(array: &PyGeometryArray, expected_row: usize) {
        match lonlat_points(array) {
            Err(LonLatPointsError::Row(row, error)) => {
                assert_eq!(row, expected_row);
                assert!(
                    error.to_string().contains("transverse Mercator domain"),
                    "unexpected projected coordinate error: {error}"
                );
            },
            Err(LonLatPointsError::Type) => panic!("expected a projected row error, got Type"),
            Err(LonLatPointsError::Setup(error)) => {
                panic!("expected a projected row error, got Setup({error})")
            },
            Ok(_) => panic!("expected a projected coordinate error on row {expected_row}"),
        }
    }

    #[test]
    fn dense_lonlat_borrows_source() {
        let array = packed_xy(vec![1.0, 2.0], vec![3.0, 4.0], Frame::None);
        let points = lonlat_points(&array).expect("in-domain packed points");
        assert!(matches!(points, LonLatPointRows::Source(_)));
        assert_eq!(points.len(), 2);
        let mut visited = Vec::new();
        points.for_each_present(|_| false, |row, point| visited.push((row, point)));
        assert_eq!(visited, [
            (0, Point::new_raw_xy(1.0, 3.0)),
            (1, Point::new_raw_xy(2.0, 4.0)),
        ]);
    }

    #[test]
    fn missing_row_is_not_validated_and_keeps_logical_index() {
        let present = packed_xy(vec![1.0], vec![2.0], Frame::None);
        let mask = ValidityMask::from_vec(3, vec![true, false, true]).expect("mask");
        let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        let points = lonlat_points(&array).expect("present row is in domain");
        assert!(matches!(points, LonLatPointRows::CompactColumns { .. }));
        assert_eq!(points.len(), 3);
        let mut visited = Vec::new();
        points.for_each_present(
            |row| array.is_row_missing(row),
            |row, point| visited.push((row, point)),
        );
        assert_eq!(visited, [(1, Point::new_raw_xy(1.0, 2.0))]);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn compact_projected_points_transform_present_columns_in_place() {
        let present = packed_xy(
            vec![960_464.566_564_364_4],
            vec![6_002_020.553_970_727],
            Frame::Crs(crs_arc_static("EPSG:3857")),
        );
        let mask = ValidityMask::from_vec(3, vec![true, false, true]).expect("mask");
        let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        let points = lonlat_points(&array).expect("projected point transforms to valid lon/lat");

        assert!(matches!(points, LonLatPointRows::CompactColumns { .. }));
        let mut visited = Vec::new();
        points.for_each_present(
            |row| array.is_row_missing(row),
            |row, point| visited.push((row, point)),
        );
        assert_eq!(visited.len(), 1);
        let (row, point) = *visited.first().expect("one point remains present");
        assert_eq!(row, 1);
        assert!((point.x - 8.628).abs() < 1e-10);
        assert!((point.y - 47.366).abs() < 1e-10);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn compact_projected_transform_failure_maps_present_rank_to_logical_row() {
        let present = packed_xy(
            vec![500_000.0, 1e300],
            vec![0.0, 1e300],
            Frame::Crs(crs_arc_static("EPSG:32660")),
        );
        let array = PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(4, vec![true, false, true, false]).expect("mask"),
        )
        .unwrap();

        assert_transform_error_row(&array, 3);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn mixed_point_transform_failure_maps_present_rank_to_logical_row() {
        let array = PyGeometryArray::mixed_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(10.0, 20.0)),
                Shape::Point(Point::new_raw_xy(500_000.0, 0.0)),
                Shape::Point(Point::new_raw_xy(30.0, 40.0)),
                Shape::Point(Point::new_raw_xy(1e300, 1e300)),
            ],
            Frame::Crs(crs_arc_static("EPSG:32660")),
        )
        .unwrap()
        .with_missing_mask(Some(
            ValidityMask::from_vec(4, vec![true, false, true, false]).expect("mask"),
        ))
        .unwrap();
        assert!(matches!(array.storage(), GeometryArrayStorage::Mixed(_)));

        assert_transform_error_row(&array, 3);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn projected_transform_plan_failure_remains_setup() {
        let unavailable = "urn:example:unavailable";
        let array = packed_xy(
            vec![1.0],
            vec![2.0],
            Frame::Crs(crs_arc_static(unavailable)),
        );

        assert!(matches!(
            lonlat_points(&array),
            Err(LonLatPointsError::Setup(_))
        ));
    }

    #[test]
    fn domain_error_carries_logical_row() {
        let array = packed_xy(vec![0.0, 200.0], vec![0.0, 0.0], Frame::None);
        match lonlat_points(&array) {
            Err(LonLatPointsError::Row(row, error)) => {
                assert_eq!(row, 1);
                assert!(error.to_string().contains("invalid longitude/latitude"));
            },
            Err(LonLatPointsError::Type) => panic!("expected Row(1), got Type"),
            Err(LonLatPointsError::Setup(error)) => {
                panic!("expected Row(1), got Setup({error})")
            },
            Ok(_) => panic!("expected Row(1), got Ok"),
        }
    }

    #[test]
    fn compact_missing_domain_error_uses_logical_index() {
        let present = packed_xy(vec![0.0, 200.0], vec![0.0, 0.0], Frame::None);
        let mask = ValidityMask::from_vec(4, vec![true, false, true, false]).expect("mask");
        let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        match lonlat_points(&array) {
            Err(LonLatPointsError::Row(row, _)) => assert_eq!(row, 3),
            Err(LonLatPointsError::Type) => panic!("expected Row(3), got Type"),
            Err(LonLatPointsError::Setup(error)) => {
                panic!("expected Row(3), got Setup({error})")
            },
            Ok(_) => panic!("expected Row(3), got Ok"),
        }
    }

    #[test]
    fn empty_projected_array_borrows_source_without_transform() {
        let array = packed_xy(
            Vec::new(),
            Vec::new(),
            Frame::Crs(crs_arc_static("EPSG:3857")),
        );
        let points = lonlat_points(&array).expect("empty projected array skips transform");
        assert!(matches!(points, LonLatPointRows::Source(_)));
        assert_eq!(points.len(), 0);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn compact_projected_all_missing_skips_transform() {
        // Same unavailable label Frame tests already store without a PROJ
        // query. TransformPlan::new to EPSG:4326 fails; EPSG:3857 would
        // succeed even on empty buffers, so Ok+len would not prove a skip.
        let unavailable = "urn:example:unavailable";
        let present = packed_xy(
            Vec::new(),
            Vec::new(),
            Frame::Crs(crs_arc_static(unavailable)),
        );
        let mask = ValidityMask::from_vec(2, vec![true, true]).expect("mask");
        let array = PyGeometryArray::scatter_present_rows(&present, mask).unwrap();
        assert!(
            crate::crs::TransformPlan::new(unavailable, "EPSG:4326").is_err(),
            "counterfactual: building the plan must fail"
        );
        let points =
            lonlat_points(&array).expect("all-missing compact must not construct TransformPlan");
        assert!(matches!(points, LonLatPointRows::CompactColumns { .. }));
        assert_eq!(points.len(), 2);
    }

    #[test]
    fn non_point_array_is_type_error() {
        let line = parse_wkt("LINESTRING (0 0, 1 1)").expect("line parses");
        let array = PyGeometryArray::from_shapes(vec![line], Frame::None).unwrap();
        assert!(matches!(
            lonlat_points(&array),
            Err(LonLatPointsError::Type)
        ));
    }
}
