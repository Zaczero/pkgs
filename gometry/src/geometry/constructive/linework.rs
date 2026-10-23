use std::borrow::Borrow;

use crate::error::Result;
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::publication::PublicationRows;
use crate::geometry::topology::overlay::{
    QProducer, exact_geographic_linework_drawing_from_shapes, exact_geographic_linework_from_shapes,
};
use crate::geometry::{
    CoordinateAxes, ExpansionBudget, GeometryErrorKind, Polygon, PolygonizeFull, Shape,
    SourceArcContext,
};

fn polygonal_parts(shape: Shape) -> crate::error::Result<Vec<Shape>> {
    match shape {
        Shape::Polygon(polygon) => Ok(vec![Shape::Polygon(polygon)]),
        Shape::MultiPolygon(polygons) => Ok(polygons
            .as_slice()
            .iter()
            .cloned()
            .map(Shape::Polygon)
            .collect()),
        Shape::Empty(..) => Ok(Vec::new()),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_) => Err(GeometryErrorKind::Invalid(
            "Q linework polygon producer emitted a non-polygonal shape".into(),
        )
        .into()),
    }
}

impl Shape {
    pub fn polygonize(&self, strict: bool) -> Result<Vec<Self>> {
        Self::polygonize_all(&[self], strict)
    }

    pub(crate) fn polygonize_with_publication_frame(
        &self,
        strict: bool,
        context: &SourceArcContext<'_, '_>,
        publication_frame: crate::Frame,
    ) -> Result<Vec<Self>> {
        Self::polygonize_all_with_publication_frame(&[self], strict, context, publication_frame)
    }

    /// Polygonize many shapes as one noding universe: every line part from
    /// every input participates in a single assembly (the array form — rings
    /// that close across elements still close). Output rings reuse input
    /// vertices and interpolate noding vertices from source edges, so Z/M is
    /// carried by provenance; `strict` selects strict ordinate verification
    /// while the default uses the lenient degradation policy.
    pub fn polygonize_all<S: Borrow<Self>>(shapes: &[S], strict: bool) -> Result<Vec<Self>> {
        Self::polygonize_all_with_context(
            shapes,
            strict,
            &crate::geometry::SourceArcContext::cartesian(),
        )
    }

    pub(crate) fn polygonize_all_with_context<S: Borrow<Self>>(
        shapes: &[S],
        strict: bool,
        context: &SourceArcContext<'_, '_>,
    ) -> Result<Vec<Self>> {
        Self::polygonize_all_with_publication_frame(shapes, strict, context, crate::Frame::None)
    }

    pub(crate) fn polygonize_all_with_publication_frame<S: Borrow<Self>>(
        shapes: &[S],
        strict: bool,
        context: &SourceArcContext<'_, '_>,
        publication_frame: crate::Frame,
    ) -> Result<Vec<Self>> {
        if context.domain().is_geographic() {
            let selected = exact_geographic_linework_from_shapes(
                shapes,
                QProducer::Polygonize,
                false,
                strict,
                context,
                publication_frame,
            )?;
            return polygonal_parts(selected);
        }
        let declared_axes = shapes.iter().fold(CoordinateAxes::XY, |axes, shape| {
            axes.union(Borrow::<Self>::borrow(shape).axes())
        });
        let inventory = ExactSourceInventory::from_shapes(shapes)?;
        let mut budget = ExpansionBudget::new("polygonize", "exact partition");
        let mut partition = inventory.partition(&mut budget)?;
        inventory.linework_polygons(
            &mut partition,
            false,
            "polygonize",
            declared_axes,
            strict,
            context,
        )
    }

    pub(crate) fn append_polygonize_rows<M: crate::geometry::ChartMetric>(
        shapes: &[impl Borrow<Self>],
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
        publication_frame: &crate::Frame,
        logical_row: Option<usize>,
        output: &mut PublicationRows,
    ) -> Result<()> {
        let crate::geometry::TopologyDomain::Geographic(_) = context.domain() else {
            return Err(GeometryErrorKind::Invalid(
                "producer-drawn polygon rows require a geographic source context".into(),
            )
            .into());
        };
        let (shape, _) = exact_geographic_linework_drawing_from_shapes(
            shapes,
            QProducer::Polygonize,
            false,
            strict,
            context,
            (*publication_frame).clone(),
        )?;
        let mut append = |polygon| {
            let logical = logical_row.unwrap_or_else(|| output.next_value_row());
            output.try_push_drawing(Self::Polygon(polygon), logical, "polygonize")
        };
        match shape {
            Self::Polygon(polygon) => append(polygon),
            Self::MultiPolygon(polygons) => {
                for polygon in polygons.as_slice() {
                    append(polygon.clone())?;
                }
                Ok(())
            },
            Self::Empty(..) => Ok(()),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::GeometryCollection(_) => Err(GeometryErrorKind::Invalid(
                "polygon publication produced a non-polygonal shape".into(),
            )
            .into()),
        }
    }

    /// Assemble noded linework into areal geometry with GEOS `BuildArea`
    /// semantics: nested rings alternate solid/hole (even-odd fill), so a ring
    /// enclosing a hole becomes ONE holed polygon — unlike [`polygonize_all`],
    /// which emits every minimal face. This is the right model for dissolving a
    /// cell-grid coverage into its outline (a coverage hole is a hole, not a
    /// separate region).
    ///
    /// [`polygonize_all`]: Self::polygonize_all
    pub fn build_area_all(shapes: &[&Self], strict: bool) -> Result<Vec<Self>> {
        Self::build_area_all_with_context(
            shapes,
            strict,
            &crate::geometry::SourceArcContext::cartesian(),
        )
    }

    pub(crate) fn build_area_all_with_context(
        shapes: &[&Self],
        strict: bool,
        context: &SourceArcContext<'_, '_>,
    ) -> Result<Vec<Self>> {
        Self::build_area_all_with_publication_frame(shapes, strict, context, crate::Frame::None)
    }

    pub(crate) fn build_area_all_with_publication_frame(
        shapes: &[&Self],
        strict: bool,
        context: &SourceArcContext<'_, '_>,
        publication_frame: crate::Frame,
    ) -> Result<Vec<Self>> {
        if context.domain().is_geographic() {
            let selected = exact_geographic_linework_from_shapes(
                shapes,
                QProducer::BuildArea,
                false,
                strict,
                context,
                publication_frame,
            )?;
            return polygonal_parts(selected);
        }
        let declared_axes = shapes
            .iter()
            .fold(CoordinateAxes::XY, |axes, shape| axes.union(shape.axes()));
        let inventory = ExactSourceInventory::from_shapes(shapes)?;
        let mut budget = ExpansionBudget::new("build_area", "exact partition");
        let mut partition = inventory.partition(&mut budget)?;
        inventory.linework_polygons(
            &mut partition,
            true,
            "build_area",
            declared_axes,
            strict,
            context,
        )
    }

    /// Assemble this geometry's linework into one areal geometry (GEOS
    /// `BuildArea`): nested rings alternate solid/hole by even-odd fill, so a
    /// ring enclosing a hole becomes one holed polygon. Returns `POLYGON EMPTY`
    /// when no ring closes, a `Polygon` for one face, a `MultiPolygon` when all
    /// returned polygons share axes, or a `GeometryCollection` when their axes
    /// differ.
    pub fn build_area(&self, strict: bool) -> Result<Self> {
        self.build_area_with_context(strict, &crate::geometry::SourceArcContext::cartesian())
    }

    pub(crate) fn build_area_with_context(
        &self,
        strict: bool,
        context: &SourceArcContext<'_, '_>,
    ) -> Result<Self> {
        if context.domain().is_geographic() {
            return exact_geographic_linework_from_shapes(
                &[self],
                QProducer::BuildArea,
                false,
                strict,
                context,
                crate::Frame::None,
            );
        }
        let polygons: Vec<Polygon> = Self::build_area_all_with_context(&[self], strict, context)?
            .into_iter()
            .map(|shape| {
                let Self::Polygon(polygon) = shape else {
                    unreachable!("build_area_all yields only polygons")
                };
                polygon
            })
            .collect();
        match polygons.len() {
            0 => Ok(Self::empty_polygon()),
            1 => Ok(Self::Polygon(
                polygons.into_iter().next().expect("one polygon"),
            )),
            _ => {
                #[expect(
                    clippy::indexing_slicing,
                    reason = "this arm handles at least two polygons"
                )]
                let axes = polygons[0].axes();
                if polygons.iter().all(|polygon| polygon.axes() == axes) {
                    Ok(Self::MultiPolygon(
                        crate::geometry::MultiPolygon::from_dense(polygons)
                            .expect("multiple polygons have admitted common axes"),
                    ))
                } else {
                    Self::geometry_collection(polygons.into_iter().map(Self::Polygon).collect())
                }
            },
        }
    }

    pub(crate) fn polygonize_full_all_with_context<S: Borrow<Self>>(
        shapes: &[S],
        strict: bool,
        context: &SourceArcContext<'_, '_>,
    ) -> Result<PolygonizeFull> {
        let declared_axes = shapes.iter().fold(CoordinateAxes::XY, |axes, shape| {
            axes.union(Borrow::<Self>::borrow(shape).axes())
        });
        let row_axes = shapes
            .iter()
            .map(|shape| Borrow::<Self>::borrow(shape).axes())
            .collect::<Vec<_>>();
        let inventory = ExactSourceInventory::from_shapes_with_occurrences(shapes)?;
        let mut budget = ExpansionBudget::new("polygonize_full", "exact partition");
        let mut partition = inventory.partition(&mut budget)?;
        inventory.polygonize_full(&mut partition, declared_axes, &row_axes, strict, context)
    }
}
