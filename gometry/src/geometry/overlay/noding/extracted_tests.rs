use crate::geometry::overlay::self_node_segments_sourced;
use crate::geometry::{Segment, XY};

#[test]
fn sourced_noder_orders_extreme_horizontal_cuts_and_keeps_owner() {
    for scale in [2.0_f64.powi(-600), 2.0_f64.powi(600)] {
        let source = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(4.0 * scale, 0.0),
        };
        let cutters = [3.0, 1.0, 2.0].map(|multiple| Segment {
            start: XY::new(multiple * scale, -scale),
            end: XY::new(multiple * scale, scale),
        });
        let (atomic, provenance) =
            self_node_segments_sourced(std::iter::once(source).chain(cutters)).unwrap();
        let source_atoms: Vec<_> = atomic
            .iter()
            .enumerate()
            .filter(|(_, segment)| segment.start.y == 0.0 && segment.end.y == 0.0)
            .collect();
        assert_eq!(source_atoms.len(), 4);
        assert_eq!(
            source_atoms
                .iter()
                .map(|(_, segment)| (segment.start.x, segment.end.x))
                .collect::<Vec<_>>(),
            vec![
                (0.0, scale),
                (scale, 2.0 * scale),
                (2.0 * scale, 3.0 * scale),
                (3.0 * scale, 4.0 * scale),
            ]
        );
        for (index, _) in source_atoms {
            assert_eq!(provenance.owners(index), &[(0, false)]);
        }
    }
}

#[test]
fn sourced_noder_preserves_reversed_partial_overlap_atom_owners() {
    let (atoms, provenance) = self_node_segments_sourced([
        Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(4.0, 0.0),
        },
        Segment {
            start: XY::new(3.0, 0.0),
            end: XY::new(1.0, 0.0),
        },
        Segment {
            start: XY::new(1.0, 0.0),
            end: XY::new(3.0, 0.0),
        },
    ])
    .unwrap();
    assert_eq!(
        atoms
            .iter()
            .map(|segment| [
                segment.start.x.to_bits(),
                segment.start.y.to_bits(),
                segment.end.x.to_bits(),
                segment.end.y.to_bits(),
            ])
            .collect::<Vec<_>>(),
        vec![
            [
                0.0_f64.to_bits(),
                0.0_f64.to_bits(),
                1.0_f64.to_bits(),
                0.0_f64.to_bits(),
            ],
            [
                1.0_f64.to_bits(),
                0.0_f64.to_bits(),
                3.0_f64.to_bits(),
                0.0_f64.to_bits(),
            ],
            [
                3.0_f64.to_bits(),
                0.0_f64.to_bits(),
                4.0_f64.to_bits(),
                0.0_f64.to_bits(),
            ],
            [
                3.0_f64.to_bits(),
                0.0_f64.to_bits(),
                1.0_f64.to_bits(),
                0.0_f64.to_bits(),
            ],
        ]
    );
    assert_eq!(
        provenance.iter().map(<[_]>::to_vec).collect::<Vec<_>>(),
        vec![
            vec![(0_u32, false)],
            vec![(0_u32, false)],
            vec![(0_u32, false)],
            vec![(1_u32, false), (2_u32, true)],
        ]
    );
}
