use super::super::super::h3_boundary::degrees_to_radians_bound;
use super::super::super::*;
use super::super::pole_key;

#[cfg(test)]
pub(super) fn test_affine_source_arc(
    lon0: f64,
    lat0: f64,
    lon1: f64,
    lat1: f64,
) -> Option<AffineSourceArc> {
    let lambda0 = degrees_to_radians_bound(lon0)?;
    let phi0 = degrees_to_radians_bound(lat0)?;
    let lambda1 = degrees_to_radians_bound(lon1)?;
    let phi1 = degrees_to_radians_bound(lat1)?;
    let same_longitude = lon0.to_bits() == lon1.to_bits();
    let same_latitude = lat0.to_bits() == lat1.to_bits();
    let equatorial = lat0 == 0.0 && lat1 == 0.0;
    let structure = if same_longitude && same_latitude {
        AffineStructure::Point
    } else if same_longitude {
        AffineStructure::ConstantLongitude {
            axis: AxisMeridian::from_exact_degrees(lon0),
        }
    } else if equatorial {
        AffineStructure::Equator
    } else {
        AffineStructure::General
    };
    let map = AffineParentMap {
        lambda0,
        phi0: if equatorial { Bound::exact(0.0)? } else { phi0 },
        dlambda: if same_longitude {
            Bound::exact(0.0)?
        } else {
            lambda1.sub(lambda0)?
        },
        dphi: if same_latitude || equatorial {
            Bound::exact(0.0)?
        } else {
            phi1.sub(phi0)?
        },
        structure,
    };
    AffineSourceArc::from_parent_map(
        &map,
        Bound::new(0.0, 1.0)?,
        [
            SourceEndpointRole::ParentStart,
            SourceEndpointRole::ParentEnd,
        ],
        [pole_key(lat0), pole_key(lat1)],
        [
            AffineEndpointIdentity {
                longitude: AxisMeridian::from_exact_degrees(lon0),
                latitude: (lat0 == 0.0).then_some(AxisLatitude::Equator),
            },
            AffineEndpointIdentity {
                longitude: AxisMeridian::from_exact_degrees(lon1),
                latitude: (lat1 == 0.0).then_some(AxisLatitude::Equator),
            },
        ],
    )
}
