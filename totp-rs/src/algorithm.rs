use crate::errors::Error;

#[derive(Clone, Copy)]
pub(crate) enum Algorithm {
    Sha1,
    Sha256,
    Sha512,
}

pub(crate) fn parse_algorithm(algorithm: &str) -> Result<Algorithm, Error> {
    match algorithm {
        "sha1" => Ok(Algorithm::Sha1),
        "sha256" => Ok(Algorithm::Sha256),
        "sha512" => Ok(Algorithm::Sha512),
        _ => Err(Error::InvalidAlgorithm),
    }
}
