use super::*;

#[derive(Clone, Debug)]
pub(super) struct Site {
    pub(in crate::geometry::tessellation) id: usize,
    pub(in crate::geometry::tessellation) point: Point,
    pub(in crate::geometry::tessellation) source_ordinal: usize,
    pub(in crate::geometry::tessellation) exact_point: exact::ExactPoint,
    pub(in crate::geometry::tessellation) retained_chart_site: Option<ChartPointSite>,
    pub(in crate::geometry::tessellation) quotient_aliases: Vec<usize>,
    pub(in crate::geometry::tessellation) snapped_sources: Vec<usize>,
}

impl Site {
    pub(in crate::geometry::tessellation) fn raw(
        id: usize,
        point: Point,
        source_ordinal: usize,
    ) -> Self {
        Self {
            id,
            point,
            source_ordinal,
            exact_point: exact::ExactPoint::from_xy(point.xy()),
            retained_chart_site: None,
            quotient_aliases: Vec::new(),
            snapped_sources: Vec::new(),
        }
    }

    pub(in crate::geometry::tessellation) fn retained(
        id: usize,
        point: Point,
        source_ordinal: usize,
        frame: GeographicFrame,
        chart_site: ChartPointSite,
    ) -> Result<Self> {
        let x = retained_site_longitude(frame.longitude(), chart_site.longitude())?;
        let y = exact::ExactDyadic::from_f64(chart_site.source_y());
        Ok(Self {
            id,
            point,
            source_ordinal,
            exact_point: exact::ExactPoint::from_dyadics(x, y),
            retained_chart_site: Some(chart_site),
            quotient_aliases: Vec::new(),
            snapped_sources: Vec::new(),
        })
    }

    pub(in crate::geometry::tessellation) fn rounded_xy(&self) -> Result<XY> {
        self.exact_point.round_nearest_even()
    }
}

/// The Delaunay mesh the Voronoi dual reads: counter-clockwise faces in the
/// crate's halfedge layout plus one exact cocircular flag per slot, so every
/// fan of cocircular faces can be collapsed onto its one circumcenter.
#[derive(Debug)]
pub(super) struct CertifiedDelaunay {
    topology: Triangulation,
    cocircular_halfedges: Vec<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CertifiedPrimalEdge {
    Interior {
        sites: [usize; 2],
        faces: [usize; 2],
    },
    Hull {
        sites: [usize; 2],
        face: usize,
        opposite: usize,
    },
}

impl CertifiedDelaunay {
    const fn empty(hull: Vec<usize>) -> Self {
        Self {
            topology: Triangulation {
                triangles: Vec::new(),
                halfedges: Vec::new(),
                hull,
            },
            cocircular_halfedges: Vec::new(),
        }
    }

    pub(in crate::geometry::tessellation) fn triangles(&self) -> &[[usize; 3]] {
        self.topology.triangles.as_chunks::<3>().0
    }

    pub(in crate::geometry::tessellation) fn halfedges(&self) -> &[usize] {
        &self.topology.halfedges
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the slot comes from this certified mesh's aligned halfedge column"
    )]
    pub(in crate::geometry::tessellation) fn edge_is_cocircular(&self, slot: usize) -> bool {
        self.cocircular_halfedges[slot]
    }

    #[cfg(test)]
    pub(super) fn hull(&self) -> &[usize] {
        &self.topology.hull
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "each slot and reverse slot comes from the same completed flat mesh columns; direct halfedge-slot access preserves the certified topology layout"
    )]
    pub(in crate::geometry::tessellation) fn primal_edges(&self) -> Vec<CertifiedPrimalEdge> {
        let mut edges = Vec::with_capacity(self.topology.halfedges.len());
        for (slot, &reverse) in self.topology.halfedges.iter().enumerate() {
            if reverse != EMPTY && slot > reverse {
                continue;
            }
            let mut sites = [
                self.topology.triangles[slot],
                self.topology.triangles[next_halfedge(slot)],
            ];
            sites.sort_unstable();
            if reverse == EMPTY {
                edges.push(CertifiedPrimalEdge::Hull {
                    sites,
                    face: slot / 3,
                    opposite: self.topology.triangles[prev_halfedge(slot)],
                });
            } else {
                let mut faces = [slot / 3, reverse / 3];
                faces.sort_unstable();
                edges.push(CertifiedPrimalEdge::Interior { sites, faces });
            }
        }
        edges.sort_unstable_by_key(|edge| match *edge {
            CertifiedPrimalEdge::Interior { sites, faces } => (sites, 0, faces, 0),
            CertifiedPrimalEdge::Hull {
                sites,
                face,
                opposite,
            } => (sites, 1, [face, usize::MAX], opposite),
        });
        edges
    }

    pub(in crate::geometry::tessellation) fn collinear_order(&self) -> Option<&[usize]> {
        self.topology
            .triangles
            .is_empty()
            .then_some(self.topology.hull.as_slice())
    }

    /// The public layout: faces run clockwise, which is what every
    /// triangulation consumer outside this module has always read.
    pub(in crate::geometry::tessellation) fn into_topology(self) -> Result<Triangulation> {
        let Self {
            topology:
                Triangulation {
                    mut triangles,
                    halfedges: previous_halfedges,
                    hull,
                },
            cocircular_halfedges,
        } = self;
        drop(previous_halfedges);
        drop(cocircular_halfedges);
        for face in triangles.as_chunks_mut::<3>().0 {
            face.swap(1, 2);
        }
        let halfedges = delaunay_halfedges(&triangles)?;
        Ok(Triangulation {
            triangles,
            halfedges,
            hull,
        })
    }
}

pub(super) fn certified_delaunay(sites: &[Site]) -> Result<CertifiedDelaunay> {
    // Every site by its input index; duplicates and collinear input are the
    // engine's to settle exactly.
    let mut points = Vec::with_capacity(sites.len());
    for site in sites {
        points.push(site.point);
    }
    let xy = |site| exact_delaunay::site_xy(&points, site);
    certified_delaunay_with_coordinates(&xy, points.len())
}

pub(in crate::geometry::tessellation) fn certified_delaunay_with_coordinates<
    C: exact_delaunay::MeshCoordinates,
>(
    coordinates: &C,
    site_count: usize,
) -> Result<CertifiedDelaunay> {
    let mut mesh = exact_delaunay::exact_delaunay_with(coordinates, site_count)?;
    if mesh.triangles.is_empty() {
        return Ok(CertifiedDelaunay::empty(mesh.hull));
    }
    let cocircular_halfedges = std::mem::take(&mut mesh.cocircular);
    Ok(CertifiedDelaunay {
        topology: mesh.into_topology(),
        cocircular_halfedges,
    })
}
