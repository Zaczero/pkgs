use super::{
    Bounds, ControlFlow, CoordSeq, EdgeId, EdgeYIndex, HeapSize, PointQuery, QueryAxis, Result,
    Ring, RingEdgeCandidates, RingView, SharedSlice, XY, edge_winding, edge_winding_for_query,
    point_in_bounds, ring_kind_columns, xy_bounds_columns,
};

/// One closed ring's Y-indexed edges for prepared point membership.
pub(crate) struct RingPointIndex {
    pub(super) bounds: Bounds,
    pub(super) xs: SharedSlice<f64>,
    pub(super) ys: SharedSlice<f64>,
    pub(super) kind: crate::geometry::RingKind,
    /// Edge start indices (`0..n-1`) stabbed by Y — dense CSR stores edge
    /// ids directly (no items-vector indirection).
    pub(super) edges: EdgeYIndex,
}

impl RingPointIndex {
    pub(super) fn build(ring: &Ring) -> Result<Self> {
        let coords = ring.coords();
        Self::build_with_kind(coords, ring_kind_columns(coords.xs(), coords.ys()))
    }

    pub(super) fn build_winding(ring: &Ring) -> Result<Option<Self>> {
        let coords = ring.coords();
        let kind = ring_kind_columns(coords.xs(), coords.ys());
        if kind == crate::geometry::RingKind::Collapsed {
            return Ok(None);
        }
        Self::build_with_kind(coords, kind).map(Some)
    }

    fn build_with_kind(coords: &CoordSeq, kind: crate::geometry::RingKind) -> Result<Self> {
        let xs = coords.xs();
        let ys = coords.ys();
        let n = xs.len();
        debug_assert_eq!(n, ys.len());
        let bounds = if n == 0 {
            Bounds::new_raw(
                f64::INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
            )
        } else {
            let [minx, miny, maxx, maxy] = xy_bounds_columns(xs, ys);
            Bounds::new_raw(minx, miny, maxx, maxy)
        };
        Ok(Self {
            bounds,
            xs: coords.carried_xs(),
            ys: coords.carried_ys(),
            kind,
            edges: EdgeYIndex::build(ys)?,
        })
    }

    fn view(&self) -> RingView<'_> {
        RingView {
            xs: &self.xs,
            ys: &self.ys,
            kind: self.kind,
        }
    }

    pub(super) fn edge_xy(&self, edge: EdgeId) -> (f64, f64, f64, f64) {
        let e = edge.as_usize();
        let next = e.wrapping_add(1);
        debug_assert_eq!(self.xs.len(), self.ys.len());
        debug_assert!(next < self.xs.len());
        // SAFETY: Rings repeat their first coordinate at the end. EdgeYIndex::build
        // emits only adjacent-pair ordinals from these equally sized columns,
        // so both the start and its successor lie inside the visible window.
        unsafe {
            (
                *self.xs.get_unchecked(e),
                *self.ys.get_unchecked(e),
                *self.xs.get_unchecked(next),
                *self.ys.get_unchecked(next),
            )
        }
    }

    /// Winding number at a probe that avoids the ring, read from the edges
    /// stabbed at its height; `None` when the probe lies on an edge.
    pub(super) fn winding_off_boundary(&self, x: f64, y: f64) -> Option<i64> {
        let query = XY::new(x, y);
        let mut winding = 0_i64;
        let mut visit = |edge| {
            let (ax, ay, bx, by) = self.edge_xy(edge);
            edge_winding(XY::new(ax, ay), XY::new(bx, by), query).map_or(
                ControlFlow::Break(()),
                |delta| {
                    winding = winding.wrapping_add(delta);
                    ControlFlow::Continue(())
                },
            )
        };
        let touched = if let Some(edges) = self.edges.dense_band_edges(y) {
            let mut touched = None;
            for edge in edges {
                if visit(edge).is_break() {
                    touched = Some(());
                    break;
                }
            }
            touched
        } else {
            self.edges.for_each_edge(y, visit)
        };
        touched.is_none().then_some(winding)
    }

    pub(super) fn winding_off_boundary_query(&self, query: impl PointQuery) -> Result<Option<i64>> {
        if !point_in_bounds(query, self.bounds)? {
            return Ok(Some(0));
        }
        let y = query.index_coordinate(QueryAxis::Y);
        let mut winding = 0_i64;
        let mut boundary = false;
        let mut error = None;
        let mut visit = |edge| {
            let (ax, ay, bx, by) = self.edge_xy(edge);
            let start = XY::new(ax, ay);
            let end = XY::new(bx, by);
            let contact = edge_winding_for_query(start, end, query);
            match contact {
                Ok(Some(delta)) => {
                    winding = winding.wrapping_add(delta);
                    ControlFlow::Continue(())
                },
                Ok(None) => {
                    boundary = true;
                    ControlFlow::Break(())
                },
                Err(failure) => {
                    error = Some(failure);
                    ControlFlow::Break(())
                },
            }
        };
        if let Some(edges) = self.edges.dense_band_edges(y) {
            for edge in edges {
                if visit(edge).is_break() {
                    break;
                }
            }
        } else {
            self.edges.for_each_edge(y, visit);
        }
        if let Some(error) = error {
            return Err(error);
        }
        Ok((!boundary).then_some(winding))
    }
}

impl RingEdgeCandidates for RingPointIndex {
    fn ring_view(&self) -> RingView<'_> {
        self.view()
    }

    fn visit_stabbed_edges(
        &self,
        y: f64,
        visit: &mut dyn FnMut(XY, XY) -> Result<()>,
    ) -> Result<()> {
        let failure = self.edges.for_each_edge(y, |edge| {
            let (ax, ay, bx, by) = self.edge_xy(edge);
            match visit(XY::new(ax, ay), XY::new(bx, by)) {
                Ok(()) => ControlFlow::Continue(()),
                Err(error) => ControlFlow::Break(error),
            }
        });
        failure.map_or(Ok(()), Err)
    }
}

impl HeapSize for RingPointIndex {
    fn heap_bytes(&self) -> usize {
        self.xs
            .heap_bytes()
            .saturating_add(self.ys.heap_bytes())
            .saturating_add(self.edges.heap_bytes())
    }
}
