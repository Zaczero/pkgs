use super::{
    CertifiedDelaunay, ChartPointSite, ExpansionBudget, GeographicFrame, GeometryErrorKind,
    NonNegative, Point, PointKey, PointSelectionChart, Result, Shape, Site, VoronoiBoundary,
    certified_delaunay, certified_delaunay_for_retained_points, snap_sites, voronoi_dcel,
};
#[cfg(test)]
use crate::geometry::CoordSeq;

/// Canonical sites: signed zeros normalized, coincident points merged, and
/// snapping applied once at the public boundary, so the tessellators always
/// receive a true site set in x order with dense ids.
pub(in crate::geometry::tessellation) fn canonical_voronoi_sites(
    points: Vec<Point>,
    tolerance: f64,
) -> Result<Vec<Site>> {
    let mut seen: crate::collections::HashMap<_, usize> =
        crate::collections::try_hash_map_with_capacity(points.len())?;
    let mut unique: Vec<(usize, Point, Vec<usize>)> = Vec::with_capacity(points.len());
    for (source_ordinal, mut point) in points.into_iter().enumerate() {
        point.x += 0.0;
        point.y += 0.0;
        let key = PointKey::new(point);
        if let Some(&site) = seen.get(&key) {
            unique
                .get_mut(site)
                .expect("raw site key points into the unique source table")
                .2
                .push(source_ordinal);
        } else {
            let site = unique.len();
            unique.push((source_ordinal, point, Vec::new()));
            let _ = seen.insert(key, site);
        }
    }
    drop(seen);
    unique.sort_unstable_by(|(left_ordinal, left, _), (right_ordinal, right, _)| {
        left.x
            .total_cmp(&right.x)
            .then_with(|| left.y.total_cmp(&right.y))
            .then_with(|| left_ordinal.cmp(right_ordinal))
    });
    let sites: Vec<Site> = unique
        .into_iter()
        .enumerate()
        .map(|(id, (source_ordinal, point, quotient_aliases))| {
            let mut site = Site::raw(id, point, source_ordinal);
            site.quotient_aliases = quotient_aliases;
            site
        })
        .collect();
    if tolerance == 0.0 {
        return Ok(sites);
    }
    let mut survivors = snap_sites(&sites, tolerance)?;
    for (id, site) in survivors.iter_mut().enumerate() {
        site.id = id;
    }
    Ok(survivors)
}

impl Shape {
    #[cfg(test)]
    pub(crate) fn voronoi_cells(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
    ) -> Result<Vec<Self>> {
        let mut budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");
        self.voronoi_cells_budgeted(tolerance, boundary, &mut budget)
    }

    #[cfg(test)]
    pub(crate) fn voronoi_cells_budgeted(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Vec<Self>> {
        self.voronoi_polygon_outputs_budgeted(tolerance, boundary, budget)
            .map(|outputs| {
                outputs
                    .into_iter()
                    .map(|output| Self::Polygon(output.polygon))
                    .collect()
            })
    }

    #[cfg(test)]
    pub(crate) fn voronoi_polygon_outputs_budgeted(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<crate::geometry::VoronoiPolygonOutputs> {
        self.voronoi_polygon_outputs_in_frame_budgeted(tolerance, boundary, None, budget)
    }

    pub(crate) fn voronoi_polygon_outputs_in_frame_budgeted(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        frame: Option<GeographicFrame>,
        budget: &mut ExpansionBudget,
    ) -> Result<crate::geometry::VoronoiPolygonOutputs> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let (canonical, mesh, chart) = self.prepare_voronoi(tolerance, frame, boundary)?;
        let Some(mesh) = mesh else {
            // A lone site's cell is the whole clip region; the envelope and
            // padded frames of one point have no area.
            return match (canonical.len(), boundary) {
                (1, VoronoiBoundary::Polygon(polygon)) => {
                    voronoi_dcel::lone_site_polygon_outputs(&canonical, polygon, chart.as_ref())
                },
                (1, _) => voronoi_dcel::lone_site_empty_polygon_outputs(&canonical),
                _ => voronoi_dcel::empty_polygon_outputs(&canonical),
            };
        };
        let embedded = if mesh.collinear_order().is_some() {
            voronoi_dcel::build_collinear_polygons_with_chart(
                &canonical,
                boundary,
                budget,
                &mesh,
                chart.as_ref(),
            )?
        } else {
            voronoi_dcel::build_product_with_chart(
                &canonical,
                boundary,
                budget,
                true,
                &mesh,
                voronoi_dcel::VoronoiProduct::Polygons,
                chart.as_ref(),
            )?
        };
        let (cells, receipt) = embedded.into_polygon_outputs();
        let outputs = cells
            .into_iter()
            .map(|output| crate::geometry::VoronoiPolygonOutput {
                polygon: output.polygon,
                owner: output.owner,
                edge_roles: output.edge_roles,
            })
            .collect();
        Ok(crate::geometry::VoronoiPolygonOutputs { outputs, receipt })
    }

    /// The edges as one packed line column: what an array result stores, so
    /// the Python lane never builds a line object per edge.
    #[cfg(test)]
    pub(crate) fn voronoi_edge_column_budgeted(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<(CoordSeq, crate::geometry::CsrOffsetColumn)> {
        self.voronoi_edge_lines_raw(tolerance, boundary, budget)?
            .into_line_column()
    }

    #[cfg(test)]
    fn voronoi_edge_lines_raw(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<voronoi_dcel::EmbeddedEdges> {
        self.voronoi_edge_lines_in_frame_raw(tolerance, boundary, None, budget)
    }

    pub(crate) fn voronoi_edge_column_in_frame_budgeted(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        frame: Option<GeographicFrame>,
        budget: &mut ExpansionBudget,
    ) -> Result<crate::geometry::VoronoiEdgeColumn> {
        let (coordinates, offsets, receipt) = self
            .voronoi_edge_lines_in_frame_raw(tolerance, boundary, frame, budget)?
            .into_line_column_with_receipt()?;
        Ok(crate::geometry::VoronoiEdgeColumn {
            coordinates,
            offsets,
            receipt,
        })
    }

    fn voronoi_edge_lines_in_frame_raw(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        frame: Option<GeographicFrame>,
        budget: &mut ExpansionBudget,
    ) -> Result<voronoi_dcel::EmbeddedEdges> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let (canonical, mesh, chart) = self.prepare_voronoi(tolerance, frame, boundary)?;
        let Some(mesh) = mesh else {
            return if canonical.len() == 1 {
                voronoi_dcel::lone_site_empty_edges(&canonical, boundary, chart.as_ref())
            } else {
                voronoi_dcel::empty_packed_edges()
            };
        };
        if mesh.collinear_order().is_none() {
            return voronoi_dcel::build_edges_with_chart(
                &canonical,
                boundary,
                budget,
                true,
                &mesh,
                chart.as_ref(),
            );
        }
        voronoi_dcel::build_collinear_edges_with_chart(
            &canonical,
            boundary,
            budget,
            &mesh,
            chart.as_ref(),
        )
    }
}

impl Shape {
    fn voronoi_sites(
        &self,
        tolerance: f64,
        frame: Option<GeographicFrame>,
        boundary: VoronoiBoundary<'_>,
    ) -> Result<(Vec<Site>, Option<PointSelectionChart>)> {
        let mut points = Vec::new();
        self.try_for_each_point(|point| -> Result<()> {
            points.push(point);
            Ok(())
        })?;
        let chart = if let Some(frame) = frame {
            let mut support = points.clone();
            if let VoronoiBoundary::Polygon(polygon) = boundary {
                support.extend(
                    std::iter::once(&polygon.shell)
                        .chain(polygon.holes.iter())
                        .flat_map(|ring| ring.iter()),
                );
            }
            PointSelectionChart::try_from_points(frame, &support)?
        } else {
            None
        };
        let Some(chart) = chart else {
            return Ok((canonical_voronoi_sites(points, tolerance)?, None));
        };

        let frame = chart.frame();
        let mut seen: crate::collections::HashMap<_, usize> =
            crate::collections::try_hash_map_with_capacity(points.len())?;
        let mut retained: Vec<(ChartPointSite, Vec<usize>)> = Vec::new();
        for (source_ordinal, &point) in points.iter().enumerate() {
            let chart_site = chart.site(point, source_ordinal)?;
            let longitude = chart_site.longitude();
            let key = (
                longitude.periods(),
                crate::geometry::canonical_f64_bits(longitude.native().get()),
                crate::geometry::canonical_f64_bits(chart_site.source_y()),
            );
            if let Some(&site) = seen.get(&key) {
                retained
                    .get_mut(site)
                    .expect("chart site key points into the retained site table")
                    .1
                    .push(source_ordinal);
            } else {
                let site = retained.len();
                retained.push((chart_site, Vec::new()));
                let _ = seen.insert(key, site);
            }
        }
        drop(seen);
        retained.sort_unstable_by(|(left, _), (right, _)| left.compare_chart_xy(*right));
        let mut sites = Vec::with_capacity(retained.len());
        for (chart_site, quotient_aliases) in retained {
            let source_ordinal = chart_site.source_ordinal();
            let point = *points.get(source_ordinal).ok_or_else(|| {
                GeometryErrorKind::Invalid("retained Voronoi source is unknown".into())
            })?;
            let mut site = Site::retained(sites.len(), point, source_ordinal, frame, chart_site)?;
            site.quotient_aliases = quotient_aliases;
            sites.push(site);
        }
        let mut sites = if tolerance == 0.0 {
            sites
        } else {
            snap_sites(&sites, tolerance)?
        };
        for (id, site) in sites.iter_mut().enumerate() {
            site.id = id;
        }
        Ok((sites, Some(chart)))
    }

    fn prepare_voronoi(
        &self,
        tolerance: f64,
        frame: Option<GeographicFrame>,
        boundary: VoronoiBoundary<'_>,
    ) -> Result<(
        Vec<Site>,
        Option<CertifiedDelaunay>,
        Option<PointSelectionChart>,
    )> {
        let (sites, chart) = self.voronoi_sites(tolerance, frame, boundary)?;
        if let Some(last) = sites.len().checked_sub(1) {
            u32::try_from(last).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        }
        let mesh = if sites.len() >= 2 {
            Some(if let Some(frame) = chart.map(|chart| chart.frame()) {
                let retained = sites
                    .iter()
                    .map(|site| -> Result<crate::geographic::ChartPointSite> {
                        let retained = site.retained_chart_site.ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "retained Voronoi site lost its chart identity".into(),
                            )
                        })?;
                        if retained.source_ordinal() != site.source_ordinal {
                            return Err(GeometryErrorKind::Invalid(
                                "retained Voronoi site lost its source occurrence".into(),
                            )
                            .into());
                        }
                        Ok(retained)
                    })
                    .collect::<Result<Vec<_>>>()?;
                certified_delaunay_for_retained_points(frame, &retained)?
            } else {
                certified_delaunay(&sites)?
            })
        } else {
            None
        };
        Ok((sites, mesh, chart))
    }
}
