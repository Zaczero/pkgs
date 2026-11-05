//! Type-guaranteed ASCII digits shared by HTTP serialization and access logs.

use std::ascii::Char;

use crate::http::types::HttpStatusCode;

const DIGIT_PAIRS: [[Char; 2]; 100] = {
    let mut out = [[Char::Digit0; 2]; 100];
    let mut value = 0;
    while value < 100 {
        *out.get_mut(value).expect("decimal pair entry") = [
            Char::digit((value / 10) as u8).expect("decimal tens digit"),
            Char::digit((value % 10) as u8).expect("decimal units digit"),
        ];
        value = value.checked_add(1).expect("decimal pair cursor");
    }
    out
};

/// Format a value below 100, preserving ASCII validity for byte and text writers.
pub(crate) fn two_digits(value: u8) -> [Char; 2] {
    *DIGIT_PAIRS
        .get(usize::from(value))
        .expect("two-digit decimal value is below 100")
}

pub(crate) fn three_digits(status: HttpStatusCode) -> [Char; 3] {
    let value = status.get();
    let [tens, units] = two_digits((value % 100) as u8);
    [
        Char::digit((value / 100) as u8).expect("admitted status hundreds digit"),
        tens,
        units,
    ]
}

#[cfg(test)]
mod tests {
    use super::{three_digits, two_digits};
    use crate::http::types::HttpStatusCode;

    #[test]
    fn two_digits_formats_valid_values() {
        assert_eq!(two_digits(0).as_str(), "00");
        assert_eq!(two_digits(7).as_str(), "07");
        assert_eq!(two_digits(99).as_str(), "99");
    }

    #[test]
    fn three_digits_formats_valid_values() {
        assert_eq!(
            three_digits(HttpStatusCode::new(100).unwrap()).as_str(),
            "100"
        );
        assert_eq!(
            three_digits(HttpStatusCode::new(200).unwrap()).as_str(),
            "200"
        );
        assert_eq!(
            three_digits(HttpStatusCode::new(999).unwrap()).as_str(),
            "999"
        );
    }
}
