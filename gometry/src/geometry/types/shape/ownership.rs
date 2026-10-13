use super::*;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ShapeOwnerKey {
    address: usize,
    len: usize,
    kind: ShapeOwnerKind,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ShapeOwnerKind {
    Collection,
    MultiLineValues,
    MultiPolygonValues,
    PolygonHoles,
}

impl ShapeOwnerKey {
    pub(crate) fn new(members: &[Shape]) -> Self {
        Self::for_slice(ShapeOwnerKind::Collection, members)
    }

    /// Slice windows within one allocation have distinct summaries.
    fn for_slice<T>(kind: ShapeOwnerKind, values: &[T]) -> Self {
        Self {
            address: values.as_ptr().addr(),
            len: values.len(),
            kind,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct ShapeHeapStats {
    coordinate_bytes: usize,
    heap_bytes: usize,
}

#[derive(Default)]
pub(crate) struct ShapeHeapMemo {
    collections: HashMap<ShapeOwnerKey, ShapeHeapStats>,
}

impl ShapeHeapMemo {
    pub(crate) fn heap_bytes(&mut self, shape: &Shape) -> usize {
        self.shape_stats(shape).heap_bytes
    }

    pub(crate) fn coordinate_bytes(&mut self, shape: &Shape) -> usize {
        self.shape_stats(shape).coordinate_bytes
    }

    fn shape_stats(&mut self, shape: &Shape) -> ShapeHeapStats {
        match shape {
            Shape::GeometryCollection(collection) => self.collection_stats(collection),
            Shape::MultiLineString(lines) => ShapeHeapStats {
                coordinate_bytes: lines.iter().fold(0_usize, |bytes, line| {
                    bytes.saturating_add(seq_coordinate_bytes(line.as_coords()))
                }),
                heap_bytes: lines.heap_bytes_saturating(),
            },
            Shape::Polygon(polygon) => ShapeHeapStats {
                coordinate_bytes: polygon.rings().fold(0_usize, |bytes, ring| {
                    bytes.saturating_add(seq_coordinate_bytes(ring.coords()))
                }),
                heap_bytes: polygon.heap_bytes_saturating(),
            },
            Shape::MultiPolygon(polygons) => ShapeHeapStats {
                coordinate_bytes: polygons.iter().fold(0_usize, |bytes, polygon| {
                    polygon.rings().fold(bytes, |bytes, ring| {
                        bytes.saturating_add(seq_coordinate_bytes(ring.coords()))
                    })
                }),
                heap_bytes: polygons.heap_bytes_saturating(),
            },
            Shape::MultiPoint(points) => ShapeHeapStats {
                coordinate_bytes: seq_coordinate_bytes(points),
                heap_bytes: points.heap_bytes_saturating(),
            },
            Shape::LineString(line) => ShapeHeapStats {
                coordinate_bytes: seq_coordinate_bytes(line.as_coords()),
                heap_bytes: line.as_coords().heap_bytes_saturating(),
            },
            Shape::Point(point) => {
                let bytes = CoordinateAxes::from_point(*point).byte_width(1);
                ShapeHeapStats {
                    coordinate_bytes: bytes,
                    heap_bytes: bytes,
                }
            },
            Shape::Empty(..) => ShapeHeapStats::default(),
        }
    }

    fn collection_stats(&mut self, collection: &GeometryCollection) -> ShapeHeapStats {
        let owner = collection
            .members_are_shared()
            .then(|| ShapeOwnerKey::new(collection.as_slice()));
        if let Some(&stats) = owner.as_ref().and_then(|owner| self.collections.get(owner)) {
            return stats;
        }
        let stats = collection.as_slice().iter().fold(
            ShapeHeapStats {
                coordinate_bytes: 0,
                heap_bytes: collection.shallow_heap_bytes(),
            },
            |stats, member| {
                let child = self.shape_stats(member);
                ShapeHeapStats {
                    coordinate_bytes: stats
                        .coordinate_bytes
                        .saturating_add(child.coordinate_bytes),
                    heap_bytes: stats.heap_bytes.saturating_add(child.heap_bytes),
                }
            },
        );
        if let Some(owner) = owner {
            self.collections.insert(owner, stats);
        }
        stats
    }
}

fn seq_coordinate_bytes(seq: &CoordSeq) -> usize {
    seq.len().saturating_mul(seq.axes().byte_width(1))
}

// `Shape` is the widest hot type and the one whose width multiplies: mixed
// array storage is `Vec<Shape>`, and both `storage_impl.rs` and `packed_ops.rs`
// account heap by `rows * size_of::<Shape>()`. `Polygon` and `MultiPoint` carry
// 88-byte payloads, producing a 96-byte enum. Boxing either would trade the hot
// structural access for a per-row allocation and indirection.
impl HeapSize for Shape {
    /// Retained native heap. Multipart, polygon, line, and collection payloads
    /// delegate to their owners, which include `SharedSlice` Arc/Vec capacity.
    /// Empty and Point have no shared owner and report only coordinate bytes.
    fn heap_bytes(&self) -> usize {
        ShapeHeapMemo::default().heap_bytes(self)
    }
}

impl Shape {
    pub(crate) fn logical_heap_bytes(&self) -> usize {
        let mut memo = ShapeHeapMemo::default();
        match self {
            Self::LineString(line) => line.logical_heap_bytes_saturating(),
            Self::Polygon(polygon) => polygon.logical_heap_bytes_saturating(),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::MultiLineString(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(..) => memo.heap_bytes(self),
        }
    }
}
