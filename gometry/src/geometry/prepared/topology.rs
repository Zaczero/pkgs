#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{
    Arc, ExactGeographicTopology, GeographicFrame, GeographicTopologyKey,
    GeographicTopologyProducts, GeographicTopologyRoute, HeapSize, Mutex, OnceLock, PointQueryPlan,
    PreparedPlan, Result, ScalarGeographicView, Shape, ShapeData, ValidationIssue, fmt,
    validate_structure,
};

/// Shape-derived topology products shared by persistent and transient plans.
///
/// The geographic graph owns seam charts and frame-keyed pole quotient topology. Cartesian
/// point-query products are admitted independently by query demand.
#[derive(Default)]
pub(super) struct TopologyPlan {
    pub(super) structure: OnceLock<Option<Box<crate::geometry::ValidationIssue>>>,
    pub(super) validity: OnceLock<Option<Box<crate::geometry::ValidationIssue>>>,
    pub(super) simplicity: OnceLock<bool>,
    pub(super) geographic: Mutex<Option<(GeographicTopologyKey, Arc<GeographicTopologyProducts>)>>,
    pub(super) point_queries: OnceLock<Option<Box<PointQueryPlan>>>,
    pub(super) point_scans: std::sync::atomic::AtomicU32,
    #[cfg(test)]
    pub(super) geographic_exact_topology_accesses: AtomicUsize,
    #[cfg(test)]
    pub(super) geographic_exact_topology_builds: AtomicUsize,
}

impl PreparedPlan {
    pub(crate) fn existing_validity(&self) -> Option<bool> {
        self.topology.get()?.validity.get().map(Option::is_none)
    }

    pub(crate) fn existing_simplicity(&self) -> Option<bool> {
        self.topology.get()?.simplicity.get().copied()
    }

    pub(crate) fn existing_structure_issue(&self) -> Option<&ValidationIssue> {
        self.topology.get()?.structure.get()?.as_deref()
    }

    pub(crate) fn validate_cached(&self) -> Result<Option<&ValidationIssue>> {
        Ok(self
            .topology()
            .validity
            .get_or_try_init(|| {
                Ok::<_, crate::error::Error>(self.shape().validate()?.map(Box::new))
            })?
            .as_deref())
    }

    pub(crate) fn validate_structure_cached(&self) -> Option<&ValidationIssue> {
        self.topology()
            .structure
            .get_or_init(|| validate_structure(self.shape(), "$").map(Box::new))
            .as_deref()
    }

    pub(crate) fn is_simple_cached(&self) -> Result<bool> {
        Ok(*self
            .topology()
            .simplicity
            .get_or_try_init(|| self.shape().is_simple())?)
    }

    pub(crate) fn geographic_validity_route(
        &self,
        view: ScalarGeographicView<'_>,
    ) -> GeographicTopologyRoute {
        *self
            .topology()
            .geographic_products(view.frame())
            .route
            .get_or_init(|| crate::geometry::classify_geographic_validity_chart(view))
    }

    pub(crate) fn geographic_exact_topology(
        &self,
        view: ScalarGeographicView<'_>,
    ) -> Result<Arc<ExactGeographicTopology>> {
        self.topology().geographic_exact_topology(view)
    }

    pub(crate) fn quotient_validity(
        &self,
        view: ScalarGeographicView<'_>,
    ) -> Result<Option<ValidationIssue>> {
        let products = self.topology().geographic_products(view.frame());
        Ok(products
            .validity
            .get_or_try_init(|| {
                self.geographic_exact_topology(view)?
                    .validity_issue(self.shape())
                    .map(|issue| issue.map(Box::new))
            })?
            .as_deref()
            .cloned())
    }

    pub(crate) fn geographic_occurrence_topology(
        &self,
        view: ScalarGeographicView<'_>,
    ) -> Result<Arc<ExactGeographicTopology>> {
        if !matches!(self.shape(), Shape::GeometryCollection(_)) {
            return self.geographic_exact_topology(view);
        }
        let products = self.topology().geographic_products(view.frame());
        products
            .occurrence_exact
            .get_or_try_init(|| {
                // Set membership can fold shared owners; boundary parity and
                // diagnostics observe their distinct source occurrences.
                if self.shape().has_repeated_shared_owner() {
                    ExactGeographicTopology::from_views(view.frame(), std::iter::once(view))
                        .map(Arc::new)
                } else {
                    self.geographic_exact_topology(view)
                }
            })
            .map(Arc::clone)
    }

    #[cfg(test)]
    pub(crate) fn geographic_exact_topology_accesses(&self) -> usize {
        self.topology.get().map_or(0, |plan| {
            plan.geographic_exact_topology_accesses
                .load(Ordering::Relaxed)
        })
    }

    #[cfg(test)]
    pub(crate) fn geographic_exact_topology_builds(&self) -> usize {
        self.topology.get().map_or(0, |plan| {
            plan.geographic_exact_topology_builds
                .load(Ordering::Relaxed)
        })
    }
}

impl TopologyPlan {
    pub(super) fn geographic_products(
        &self,
        frame: GeographicFrame,
    ) -> Arc<GeographicTopologyProducts> {
        let key = GeographicTopologyKey::new(frame);
        let mut slot = self
            .geographic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((known, products)) = slot.as_ref()
            && *known == key
        {
            return Arc::clone(products);
        }
        let products = Arc::new(GeographicTopologyProducts::default());
        *slot = Some((key, Arc::clone(&products)));
        products
    }

    fn geographic_exact_topology(
        &self,
        view: ScalarGeographicView<'_>,
    ) -> Result<Arc<ExactGeographicTopology>> {
        #[cfg(test)]
        self.geographic_exact_topology_accesses
            .fetch_add(1, Ordering::Relaxed);
        let products = self.geographic_products(view.frame());
        products
            .exact
            .get_or_try_init(|| {
                #[cfg(test)]
                self.geographic_exact_topology_builds
                    .fetch_add(1, Ordering::Relaxed);
                ExactGeographicTopology::from_views_as_set(view.frame(), std::iter::once(view))
                    .map(Arc::new)
            })
            .map(Arc::clone)
    }
}

impl std::ops::Deref for PreparedPlan {
    type Target = ShapeData;

    fn deref(&self) -> &Self::Target {
        self.shape_data()
    }
}

impl fmt::Debug for PreparedPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.shape.fmt(f)
    }
}

impl HeapSize for TopologyPlan {
    fn heap_bytes(&self) -> usize {
        self.structure
            .heap_bytes()
            .saturating_add(self.validity.heap_bytes())
            .saturating_add(self.point_queries.heap_bytes())
            .saturating_add(
                self.geographic
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .map_or(0, |(_, products)| products.heap_bytes()),
            )
    }
}

impl HeapSize for PreparedPlan {
    fn heap_bytes(&self) -> usize {
        self.shape
            .heap_bytes()
            .saturating_add(self.topology.heap_bytes())
            .saturating_add(self.metric.heap_bytes())
    }
}
