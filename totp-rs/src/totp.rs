use hmac::digest::{FixedOutputReset, KeyInit};
use hmac::{Hmac, Mac};
use sha1::Sha1;
use sha2::{Sha256, Sha512};
use subtle::ConstantTimeEq;

use crate::algorithm::Algorithm;

fn dynamic_truncate(digest: &[u8]) -> u32 {
    let offset = (digest[digest.len() - 1] & 0x0F) as usize;
    let p = &digest[offset..offset + 4];
    (u32::from(p[0] & 0x7f) << 24)
        | (u32::from(p[1]) << 16)
        | (u32::from(p[2]) << 8)
        | u32::from(p[3])
}

fn totp_code_with<M: Mac + KeyInit>(secret: &[u8], counter_bytes: &[u8; 8], modulus: u32) -> u32 {
    let mut mac = <M as KeyInit>::new_from_slice(secret).expect("HMAC accepts any key size");
    mac.update(counter_bytes);
    let digest = mac.finalize().into_bytes();
    let truncated = dynamic_truncate(&digest);
    truncated % modulus
}

pub(crate) fn totp_code(secret: &[u8], counter: i64, modulus: u32, algorithm: Algorithm) -> u32 {
    let counter_bytes = counter.to_be_bytes();
    match algorithm {
        Algorithm::Sha1 => totp_code_with::<Hmac<Sha1>>(secret, &counter_bytes, modulus),
        Algorithm::Sha256 => totp_code_with::<Hmac<Sha256>>(secret, &counter_bytes, modulus),
        Algorithm::Sha512 => totp_code_with::<Hmac<Sha512>>(secret, &counter_bytes, modulus),
    }
}

fn totp_code_with_reset<M: Mac + FixedOutputReset>(mac: &mut M, counter: i64, modulus: u32) -> u32 {
    Mac::update(mac, &counter.to_be_bytes());
    let digest = mac.finalize_reset().into_bytes();
    let truncated = dynamic_truncate(&digest);
    truncated % modulus
}

fn verify_with_mac<M: Mac + FixedOutputReset>(
    mut mac: M,
    counter: i64,
    window: u8,
    modulus: u32,
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

    for offset in 1..=window as i64 {
        if matches(counter - offset) {
            return true;
        }
        if matches(counter + offset) {
            return true;
        }
    }

    false
}

pub(crate) fn verify(
    secret: &[u8],
    counter: i64,
    window: u8,
    modulus: u32,
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
