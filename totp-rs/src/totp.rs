use std::num::NonZeroU32;

use hmac::digest::{FixedOutputReset, KeyInit};
use hmac::{Hmac, Mac};
use sha1::Sha1;
use sha2::{Sha256, Sha512};
use subtle::ConstantTimeEq as _;

use crate::algorithm::Algorithm;

fn dynamic_truncate(digest: &[u8]) -> u32 {
    // RFC 4226 section 5.3: the offset is the low nibble of the final byte, so
    // it selects four bytes that lie inside any digest of at least 20 -- which
    // every HMAC this crate constructs is.
    let Some(&last) = digest.last() else {
        return 0;
    };
    let offset = usize::from(last & 0x0F);
    let Some(&[high, second, third, low]) = digest.get(offset..).and_then(<[u8]>::first_chunk::<4>)
    else {
        return 0;
    };
    (u32::from(high & 0x7F) << 24)
        | (u32::from(second) << 16)
        | (u32::from(third) << 8)
        | u32::from(low)
}

fn totp_code_with<M: Mac + KeyInit>(
    secret: &[u8],
    counter_bytes: [u8; 8],
    modulus: NonZeroU32,
) -> u32 {
    let mut mac = <M as KeyInit>::new_from_slice(secret).expect("HMAC accepts any key size");
    mac.update(&counter_bytes);
    let digest = mac.finalize().into_bytes();
    let truncated = dynamic_truncate(&digest);
    truncated % modulus
}

pub(crate) fn totp_code(
    secret: &[u8],
    counter: i64,
    modulus: NonZeroU32,
    algorithm: Algorithm,
) -> u32 {
    let counter_bytes = counter.to_be_bytes();
    match algorithm {
        Algorithm::Sha1 => totp_code_with::<Hmac<Sha1>>(secret, counter_bytes, modulus),
        Algorithm::Sha256 => totp_code_with::<Hmac<Sha256>>(secret, counter_bytes, modulus),
        Algorithm::Sha512 => totp_code_with::<Hmac<Sha512>>(secret, counter_bytes, modulus),
    }
}

fn totp_code_with_reset<M: Mac + FixedOutputReset>(
    mac: &mut M,
    counter: i64,
    modulus: NonZeroU32,
) -> u32 {
    Mac::update(mac, &counter.to_be_bytes());
    let digest = mac.finalize_reset().into_bytes();
    let truncated = dynamic_truncate(&digest);
    truncated % modulus
}

fn verify_with_mac<M: Mac + FixedOutputReset>(
    mut mac: M,
    counter: i64,
    window: u8,
    modulus: NonZeroU32,
    code: u32,
) -> bool {
    let code_bytes = code.to_be_bytes();

    let mut matches = |candidate: i64| -> bool {
        totp_code_with_reset(&mut mac, candidate, modulus)
            .to_be_bytes()
            .ct_eq(&code_bytes)
            .unwrap_u8()
            == 1
    };

    if matches(counter) {
        return true;
    }

    for offset in 1..=i64::from(window) {
        if counter.checked_sub(offset).is_some_and(&mut matches) {
            return true;
        }
        if counter.checked_add(offset).is_some_and(&mut matches) {
            return true;
        }
    }

    false
}

pub(crate) fn verify(
    secret: &[u8],
    counter: i64,
    window: u8,
    modulus: NonZeroU32,
    code: u32,
    algorithm: Algorithm,
) -> bool {
    match algorithm {
        Algorithm::Sha1 => verify_with_mac(
            <Hmac<Sha1> as KeyInit>::new_from_slice(secret).expect("HMAC accepts any key size"),
            counter,
            window,
            modulus,
            code,
        ),
        Algorithm::Sha256 => verify_with_mac(
            <Hmac<Sha256> as KeyInit>::new_from_slice(secret).expect("HMAC accepts any key size"),
            counter,
            window,
            modulus,
            code,
        ),
        Algorithm::Sha512 => verify_with_mac(
            <Hmac<Sha512> as KeyInit>::new_from_slice(secret).expect("HMAC accepts any key size"),
            counter,
            window,
            modulus,
            code,
        ),
    }
}
