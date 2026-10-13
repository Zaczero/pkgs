use crate::work::Unmetered;
    use super::*;

    #[test]
    fn distinct_selected_roots_cannot_share_one_native_face() -> Result<()> {
        let mut face_regions = vec![None; 1];
        let mut known = vec![false; 1];
        let mut work = Unmetered;
        assign_native_face_region(&mut face_regions, &mut known, 0, Some(1256), &mut work)?;

        if assign_native_face_region(&mut face_regions, &mut known, 0, Some(1251), &mut work)
            .is_ok()
        {
            return Err(GeometryErrorKind::Invalid(
                "one Native face accepted two distinct selected-region roots".into(),
            )
            .into());
        }
        Ok(())
    }
