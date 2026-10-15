use std::ops::Range;

use super::{ShapeMembers, ShapeStructure, matches_multipart_members, matches_shape_header};
use crate::geometry::storage::ShapeRow;
use crate::geometry::{CoordinateAxes, GeometryKind, MultiMember, Shape};

#[derive(Default)]
pub(in crate::geometry) struct StructureHeaders {
    nodes: Vec<StructureHeader>,
    empty_members: Vec<usize>,
}

struct StructureHeader {
    kind: GeometryKind,
    axes: CoordinateAxes,
    empty: bool,
    members: Members,
    end: usize,
}

enum Members {
    Primitive,
    Multipart { count: usize, empty: Range<usize> },
    Collection { count: usize },
}

impl StructureHeaders {
    pub(in crate::geometry) fn capture(&mut self, shape: &Shape) -> usize {
        let row = ShapeRow::Shape(shape);
        let root = self.nodes.len();
        self.nodes.push(StructureHeader {
            kind: row.kind(),
            axes: row.axes(),
            empty: row.is_empty(),
            members: Members::Primitive,
            end: root,
        });
        let members = match shape {
            Shape::GeometryCollection(parts) => {
                for part in parts {
                    self.capture(part);
                }
                Members::Collection { count: parts.len() }
            },
            Shape::MultiPoint(parts) => self.capture_members(parts.members()),
            Shape::MultiLineString(parts) => self.capture_members(parts.members()),
            Shape::MultiPolygon(parts) => self.capture_members(parts.members()),
            Shape::Point(_) | Shape::LineString(_) | Shape::Polygon(_) | Shape::Empty(..) => {
                Members::Primitive
            },
        };
        let end = self.nodes.len();
        let node = self.nodes.get_mut(root).expect("the root was appended");
        node.members = members;
        node.end = end;
        root
    }

    fn capture_members(
        &mut self,
        members: impl ExactSizeIterator<Item = MultiMember<usize>>,
    ) -> Members {
        let count = members.len();
        let start = self.empty_members.len();
        self.empty_members.extend(
            members.enumerate().filter_map(|(index, member)| {
                matches!(member, MultiMember::Empty).then_some(index)
            }),
        );
        Members::Multipart {
            count,
            empty: start..self.empty_members.len(),
        }
    }

    pub(in crate::geometry) fn adopt(&mut self, structure: ShapeStructure) -> usize {
        let root = self.nodes.len();
        self.nodes.push(StructureHeader {
            kind: structure.kind,
            axes: structure.axes,
            empty: structure.empty,
            members: Members::Primitive,
            end: root,
        });
        let members = match structure.members {
            ShapeMembers::Primitive => Members::Primitive,
            ShapeMembers::Multipart { count, empty } => {
                let start = self.empty_members.len();
                self.empty_members.extend(empty);
                Members::Multipart {
                    count,
                    empty: start..self.empty_members.len(),
                }
            },
            ShapeMembers::Collection(parts) => {
                let count = parts.len();
                for part in parts {
                    self.adopt(part);
                }
                Members::Collection { count }
            },
        };
        let end = self.nodes.len();
        let node = self.nodes.get_mut(root).expect("the root was appended");
        node.members = members;
        node.end = end;
        root
    }

    pub(in crate::geometry) fn matches(&self, root: usize, row: ShapeRow<'_>) -> bool {
        let Some(header) = self.nodes.get(root) else {
            return false;
        };
        if !matches_shape_header(header.kind, header.axes, header.empty, row) {
            return false;
        }
        let shape = match row {
            ShapeRow::Handle(handle) => handle.shape(),
            ShapeRow::Shape(shape) => shape,
            ShapeRow::Point(_) | ShapeRow::Packed(_) => {
                return matches!(header.members, Members::Primitive);
            },
        };
        match shape {
            Shape::GeometryCollection(parts) => {
                let Members::Collection { count } = header.members else {
                    return false;
                };
                if count != parts.len() {
                    return false;
                }
                let mut next = root
                    .checked_add(1)
                    .expect("the root indexes a stored header");
                for part in parts {
                    if !self.matches(next, ShapeRow::Shape(part)) {
                        return false;
                    }
                    let Some(child) = self.nodes.get(next) else {
                        return false;
                    };
                    next = child.end;
                }
                next == header.end
            },
            Shape::MultiPoint(parts) => self.matches_members(&header.members, parts.members()),
            Shape::MultiLineString(parts) => self.matches_members(&header.members, parts.members()),
            Shape::MultiPolygon(parts) => self.matches_members(&header.members, parts.members()),
            Shape::Point(_) | Shape::LineString(_) | Shape::Polygon(_) | Shape::Empty(..) => {
                matches!(header.members, Members::Primitive)
            },
        }
    }

    fn matches_members(
        &self,
        expected: &Members,
        members: impl ExactSizeIterator<Item = MultiMember<usize>>,
    ) -> bool {
        let Members::Multipart { count, empty } = expected else {
            return false;
        };
        self.empty_members
            .get(empty.clone())
            .is_some_and(|empty| matches_multipart_members(*count, empty.iter().copied(), members))
    }
}
