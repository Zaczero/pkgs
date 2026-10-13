use super::*;

#[cfg(test)]
mod dwithin_outcome_tests {
    use super::DwithinOutcome;

    #[test]
    fn witnessed_within_dominates_an_earlier_unresolved_edge() {
        assert_eq!(
            DwithinOutcome::Unresolved.combine(DwithinOutcome::Within),
            DwithinOutcome::Within
        );
        assert_eq!(
            DwithinOutcome::Outside.combine(DwithinOutcome::Unresolved),
            DwithinOutcome::Unresolved
        );
        assert_eq!(
            DwithinOutcome::Outside.combine(DwithinOutcome::Outside),
            DwithinOutcome::Outside
        );
    }

    #[test]
    fn unresolved_is_an_error_only_at_the_boolean_boundary() {
        let error = DwithinOutcome::Unresolved
            .into_bool()
            .expect_err("a bool cannot encode an unresolved threshold");
        assert!(error.to_string().contains(
            "geodesic dwithin could not certify the threshold within its evaluation work limit"
        ));
    }
}
