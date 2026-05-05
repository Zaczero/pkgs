use std::error::Error;
use std::io;
use std::path::Path;
use std::sync::Arc;

use rustls::crypto::aws_lc_rs::default_provider;
use rustls::server::WebPkiClientVerifier;
use rustls::version::{TLS12, TLS13};
use rustls::{RootCertStore, ServerConfig as RustlsServerConfig};
use rustls_pki_types::pem::{Error as PemError, PemObject};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

use crate::config::{ClientCertMode, TlsConfig};

pub const ALPN_H2: &[u8] = b"h2";
pub const ALPN_HTTP1: &[u8] = b"http/1.1";

pub fn build_tls_config(
    certfile: &Path,
    keyfile: &Path,
    ca_certs: Option<&Path>,
    cert_mode: ClientCertMode,
    http1: bool,
) -> io::Result<TlsConfig> {
    let cert_chain = load_certificates(certfile)?;
    let key = load_private_key(keyfile)?;
    let provider = Arc::new(default_provider());
    let builder = RustlsServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&TLS13, &TLS12])
        .map_err(tls_io_error)?;

    let verifier = match cert_mode {
        ClientCertMode::None => WebPkiClientVerifier::no_client_auth(),
        ClientCertMode::Optional | ClientCertMode::Required => {
            let Some(ca_certs) = ca_certs else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cert_reqs optional/required requires ca_certs",
                ));
            };
            let roots = Arc::new(load_root_store(ca_certs)?);
            let builder = WebPkiClientVerifier::builder_with_provider(roots, provider);
            if cert_mode == ClientCertMode::Optional {
                builder.allow_unauthenticated().build()
            } else {
                builder.build()
            }
            .map_err(tls_io_error)?
        },
    };

    let mut config = builder
        .with_client_cert_verifier(verifier)
        .with_single_cert(cert_chain, key)
        .map_err(tls_io_error)?;
    config.alpn_protocols = if http1 {
        vec![ALPN_H2.to_vec(), ALPN_HTTP1.to_vec()]
    } else {
        vec![ALPN_H2.to_vec()]
    };
    config.max_early_data_size = 0;

    Ok(TlsConfig {
        acceptor: TlsAcceptor::from(Arc::new(config)),
    })
}

fn load_certificates(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(pem_io_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(pem_io_error)?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "TLS certificate file contains no certificates: {}",
                path.display()
            ),
        ));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).map_err(|err| {
        let detail = if matches!(err, PemError::NoItemsFound) {
            "no unencrypted private key found"
        } else {
            "could not parse private key"
        };
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{detail}: {}", path.display()),
        )
    })
}

fn load_root_store(path: &Path) -> io::Result<RootCertStore> {
    let certs = load_certificates(path)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).map_err(tls_io_error)?;
    }
    Ok(roots)
}

fn pem_io_error(err: PemError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, err)
}

fn tls_io_error(err: impl Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, err)
}
