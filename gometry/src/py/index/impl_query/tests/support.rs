fn public_point(py: Python<'_>, x: f64, y: f64) -> Bound<'_, PyAny> {
        crate::Typed(PyGeometry::with_frame(
            ShapeData::new(Shape::Point(Point::new_raw_xy(x, y))),
            Frame::None,
        ))
        .into_pyobject(py)
        .expect("point binds as its public typed geometry")
        .into_any()
    }

fn public_ids(value: &Bound<'_, PyAny>) -> Vec<i64> {
        value
            .call_method0("tolist")
            .expect("index result supports tolist")
            .extract::<Vec<i64>>()
            .expect("index result contains signed handles")
    }

fn assert_no_compact_point_rank(logical_len: usize, operation: &str) -> usize {
        let bytes = count_valid_bytes();
        assert_eq!(
            bytes, 0,
            "compact nullable point {operation} scanned {bytes} validity bytes at M={logical_len}"
        );
        bytes
    }
