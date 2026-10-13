use std::sync::Arc;

use geographiclib_rs::Geodesic;

use crate::crs::geodesic::{AuthalicSeries, GeodesicMetric};
use crate::geographic::lonlat::lonlat_rect_area_m2;
use crate::geographic::{AngularFactor, GeodeticPole, GeographicAxisOrientation, GeographicFrame};
use crate::geometry::{
    Bounds, ChartMetric, Coordinates as _, MultiPolygon, Point, Polygon, Ring, Shape,
    ring_bounded_pole,
};

mod area;
mod perimeter;
mod support;

use support::*;
