use std::error::Error;
use std::io;
use std::sync::Arc;

use pyo3::FromPyObject;
use rustls::crypto::aws_lc_rs::default_provider;
use rustls::server::WebPkiClientVerifier;
use rustls::version::{TLS12, TLS13};
use rustls::{RootCertStore, ServerConfig as RustlsServerConfig, ServerConnection};
use rustls_pki_types::pem::{Error as PemError, PemObject};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;
use x509_cert::Certificate;
use x509_cert::der::Decode;
use x509_cert::der::pem::{LineEnding, encode_string};

use crate::config::{ClientCertMode, TlsConfig};

pub(crate) const ALPN_H2: &[u8] = b"h2";
pub(crate) const ALPN_HTTP1: &[u8] = b"http/1.1";

/// PEM material handed over by the Python `TlsMaterial` that read it.
#[derive(FromPyObject)]
pub(crate) struct TlsMaterial {
    certificate: Vec<u8>,
    private_key: Vec<u8>,
    client_ca: Option<Vec<u8>>,
}

/// Verified client identity captured from a nonempty peer certificate chain.
///
/// The subject is RFC 4514 (`RdnSequence::Display`); the chain is the original
/// DER re-wrapped as PEM, leaf first — never re-encoded from a parse tree.
pub(crate) struct ClientIdentity {
    pub subject: Box<str>,
    pub certificate_chain: Box<[Box<str>]>,
}

impl ClientIdentity {
    /// Build an identity from the peer chain, or `None` when the peer sent none.
    ///
    /// A nonempty chain whose leaf distinguished name cannot be rendered fails
    /// the connection: a chain without a name would leave ASGI with
    /// `client_cert_name=None` despite verified certificates.
    pub(crate) fn from_certificates(
        certificates: &[CertificateDer<'_>],
    ) -> io::Result<Option<Self>> {
        if certificates.is_empty() {
            return Ok(None);
        }
        let leaf = Certificate::from_der(certificates[0].as_ref()).map_err(tls_io_error)?;
        // Fields are crate-private in x509-cert 0.3; accessors expose the same
        // `Name` whose `Display` is RFC 4514 via `RdnSequence`.
        let subject = leaf
            .tbs_certificate()
            .subject()
            .to_string()
            .into_boxed_str();
        let certificate_chain = certificates
            .iter()
            .map(|certificate| certificate_der_to_pem(certificate.as_ref()))
            .collect::<io::Result<Box<[_]>>>()?;
        Ok(Some(Self {
            subject,
            certificate_chain,
        }))
    }
}

/// What one TLS handshake settled, for `scope["extensions"]["tls"]`.
///
/// Captured once as the connection is established, because the rustls session
/// is dropped when the stream is split into halves. Client identity is PEM and
/// subject together so each request reuses the same strings.
pub(crate) struct TlsSessionInfo {
    /// The certificate this server presented. Shared with every other
    /// connection: one server, one identity.
    pub server_certificate: Arc<str>,
    pub version: Option<u16>,
    pub cipher_suite: Option<u16>,
    pub client: Option<ClientIdentity>,
}

impl TlsSessionInfo {
    pub(crate) fn from_session(
        session: &ServerConnection,
        server_certificate: Arc<str>,
    ) -> io::Result<Self> {
        let client =
            ClientIdentity::from_certificates(session.peer_certificates().unwrap_or_default())?;
        Ok(Self {
            server_certificate,
            version: session.protocol_version().map(u16::from),
            cipher_suite: session
                .negotiated_cipher_suite()
                .map(|suite| u16::from(suite.suite())),
            client,
        })
    }
}

/// One security plane for a connection: plaintext or a completed TLS session.
///
/// Scheme and the ASGI TLS extension both read this; there is no parallel
/// `secure: bool`.
pub(crate) enum ConnectionSecurity {
    Plaintext,
    Tls(TlsSessionInfo),
}

impl ConnectionSecurity {
    pub(crate) const fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }

    /// HTTP/1 has no peer-supplied scheme; TLS is `https`, otherwise `http`.
    pub(crate) const fn h1_scheme(&self) -> &'static str {
        if self.is_tls() { "https" } else { "http" }
    }
}

/// PEM-wrap original DER without re-encoding a parsed certificate.
fn certificate_der_to_pem(der: &[u8]) -> io::Result<Box<str>> {
    encode_string("CERTIFICATE", LineEnding::LF, der)
        .map(String::into_boxed_str)
        .map_err(tls_io_error)
}

/// Build an acceptor from PEM material that has already been read.
///
/// The bytes arrive here rather than the paths they came from because the
/// files are read while the process still holds the privileges to read them
/// — a key readable only by root is loaded before `setuid`, not after.
pub(crate) fn build_tls_config(
    material: &TlsMaterial,
    cert_mode: ClientCertMode,
    http1: bool,
) -> io::Result<TlsConfig> {
    let cert_chain = load_certificates(&material.certificate, "certfile")?;
    // The leaf is what this server presents as its identity; anything after it
    // in the file is the chain to a root the client already trusts.
    let server_certificate = certificate_der_to_pem(cert_chain[0].as_ref())?;
    let key = load_private_key(&material.private_key)?;
    let provider = Arc::new(default_provider());
    let builder = RustlsServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&TLS13, &TLS12])
        .map_err(tls_io_error)?;

    let verifier = match cert_mode {
        ClientCertMode::None => WebPkiClientVerifier::no_client_auth(),
        ClientCertMode::Optional | ClientCertMode::Required => {
            let Some(ca_certs) = material.client_ca.as_deref() else {
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
        server_certificate: Arc::from(server_certificate),
    })
}

fn load_certificates(pem: &[u8], setting: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(pem_io_error)?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{setting} contains no certificates"),
        ));
    }
    Ok(certs)
}

fn load_private_key(pem: &[u8]) -> io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_slice(pem).map_err(|err| {
        let detail = if matches!(err, PemError::NoItemsFound) {
            "keyfile contains no unencrypted private key"
        } else {
            "keyfile could not be parsed"
        };
        io::Error::new(io::ErrorKind::InvalidInput, detail)
    })
}

fn load_root_store(pem: &[u8]) -> io::Result<RootCertStore> {
    let certs = load_certificates(pem, "ca_certs")?;
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
