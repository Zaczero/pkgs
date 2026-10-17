use super::{parse_zid_timestamp, zid_from_time_and_sequence};

#[kani::proof]
fn timestamp_and_sequence_preserve_the_signed_integer_format() {
    let timestamp: u64 = kani::any();
    let sequence: u16 = kani::any();
    kani::assume(timestamp < (1_u64 << 47));

    let identifier = zid_from_time_and_sequence(timestamp, sequence);
    let expected = u128::from(timestamp) * 65_536 + u128::from(sequence);
    assert_eq!(u128::from(identifier), expected);
    assert!(identifier <= i64::MAX as u64);
    assert_eq!(parse_zid_timestamp(identifier), timestamp);
}
