    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn cell_array_depth_is_not_admitted_without_a_present_row() {
        Python::initialize();
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                c"class Depth:\n    calls = 0\n    def __index__(self):\n        self.calls += 1\n        raise ValueError('depth should not be parsed')\n",
                c"<cell-array-empty-depth-test>",
                c"cell_array_empty_depth_test",
            )
            .expect("depth module");
            let depth = module
                .getattr("Depth")
                .and_then(|class| class.call0())
                .expect("counting depth");
            for cells in [
                PyCellArray::from_ids(GridKind::H3Cell, Vec::new()),
                PyCellArray::from_ids_with_missing(GridKind::H3Cell, vec![0, 0], vec![true, true]),
            ] {
                cells
                    .parent(Some(&depth))
                    .expect("empty parent does not inspect depth");
                cells
                    .children_count(py, Some(&depth))
                    .expect("empty descendants do not inspect depth");
            }
            assert_eq!(
                depth
                    .getattr("calls")
                    .and_then(|calls| calls.extract::<usize>())
                    .expect("call count"),
                0
            );
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn cell_array_depth_is_parsed_once_at_first_present_row() {
        Python::initialize();
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                c"class Depth:\n    def __init__(self):\n        self.calls = 0\n        self.value = 6\n    def __index__(self):\n        self.calls += 1\n        return self.value\n",
                c"<cell-array-depth-test>",
                c"cell_array_depth_test",
            )
            .expect("depth module");
            let depth = module
                .getattr("Depth")
                .and_then(|class| class.call0())
                .expect("counting depth");
            let cells = PyCellArray::from_ids(GridKind::H3Cell, vec![
                h3_id(37.7749, -122.4194, 7),
                h3_id(37.7749, -122.4194, 8),
            ]);

            let parent = cells
                .parent(Some(&depth))
                .expect("parent resolves one shared explicit depth");
            let expected_parent = h3o::CellIndex::try_from(cells.id_at(0))
                .expect("valid H3 id")
                .parent_at(6)
                .expect("parent at depth 6");
            assert_eq!(parent.id_at(0), expected_parent.hash_key());
            assert_eq!(
                depth
                    .getattr("calls")
                    .and_then(|calls| calls.extract::<usize>())
                    .expect("call count"),
                1
            );

            depth.setattr("value", 9).expect("change requested depth");
            let counts = cells
                .children_count(py, Some(&depth))
                .expect("descendant counts resolve one shared explicit depth");
            let counts = counts
                .bind(py)
                .call_method0("tolist")
                .and_then(|values| values.extract::<Vec<f64>>())
                .expect("counts as list");
            assert_eq!(counts, [49.0, 7.0]);
            assert_eq!(
                depth
                    .getattr("calls")
                    .and_then(|calls| calls.extract::<usize>())
                    .expect("call count"),
                2
            );
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn empty_missing_and_max_depth_neighbors_children_succeed() {
        Python::initialize();
        Python::attach(|py| {
            let empty = PyCellArray::from_ids(GridKind::H3Cell, Vec::new());
            match empty.neighbors(py) {
                Ok(groups) => drop(groups),
                Err(error) => panic!("{error}"),
            }
            match empty.children(py, None) {
                Ok(groups) => drop(groups),
                Err(error) => panic!("{error}"),
            }

            let present = h3_id(37.7749, -122.4194, 7);
            let missing =
                PyCellArray::from_ids_with_missing(GridKind::H3Cell, vec![present, 0], vec![
                    false, true,
                ]);
            match missing.neighbors(py) {
                Ok(groups) => drop(groups),
                Err(error) => panic!("{error}"),
            }

            let max = PyCellArray::from_ids(GridKind::H3Cell, vec![h3_id(37.7749, -122.4194, 15)]);
            match max.children(py, None) {
                Ok(groups) => drop(groups),
                Err(error) => panic!("{error}"),
            }
        });
    }
