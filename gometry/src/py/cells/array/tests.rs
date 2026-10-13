use pyo3::prelude::*;

use super::*;

fn h3_id(lat: f64, lon: f64, resolution: u8) -> u64 {
    u64::from(
        h3o::LatLng::new(lat, lon)
            .expect("finite latlng")
            .to_cell(h3o::Resolution::try_from(resolution).expect("resolution")),
    )
}

include!("tests/storage.rs");
include!("tests/hierarchy.rs");
