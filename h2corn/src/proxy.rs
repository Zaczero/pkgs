use std::mem::size_of;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str;

use atoi_simd::parse_pos;
use memchr::memchr;
use tokio::io::AsyncRead;
use zerocopy::byteorder::network_endian::U16;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Ref, Unaligned};

use crate::error::{ConfigError, ErrorExt, H2CornError, ProxyError};
use crate::frame::{CONNECTION_PREFACE, FrameReader};

const PROXY_V1_MAX_LEN: usize = 107;
const HTTP2_PREFACE_LEAD: &[u8] = b"PRI * HTTP/2.0";
const PROXY_V2_SIG: [u8; 12] = [
    0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a,
];

#[derive(FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned)]
#[repr(C)]
struct ProxyV2Header {
    sig: [u8; 12],
    version_command: u8,
    family_transport: u8,
    payload_len: U16,
}

#[derive(FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned)]
#[repr(C)]
struct ProxyV2Ipv4Addrs {
    client_ip: [u8; 4],
    server_ip: [u8; 4],
    client_port: U16,
    server_port: U16,
}

#[derive(FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned)]
#[repr(C)]
struct ProxyV2Ipv6Addrs {
    client_ip: [u8; 16],
    server_ip: [u8; 16],
    client_port: U16,
    server_port: U16,
}

const _: () = assert!(size_of::<ProxyV2Header>() == 16);
const _: () = assert!(size_of::<ProxyV2Ipv4Addrs>() == 12);
const _: () = assert!(size_of::<ProxyV2Ipv6Addrs>() == 36);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProxyProtocolMode {
    Off,
    V1,
    V2,
}

#[derive(Clone, Debug)]
pub enum TrustedPeer {
    Any,
    Unix,
    Ip(IpAddr),
    Cidr(Cidr),
}

impl TrustedPeer {
    fn matches_unix(&self) -> bool {
        matches!(self, Self::Any | Self::Unix)
    }

    fn matches_ip(&self, ip: IpAddr) -> bool {
        match self {
            Self::Any => true,
            Self::Unix => false,
            Self::Ip(candidate) => *candidate == ip,
            Self::Cidr(cidr) => cidr.contains(ip),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProxyV1Transport {
    Tcp4,
    Tcp6,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cidr {
    V4 { network: u32, mask: u32 },
    V6 { network: u128, mask: u128 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionPeer {
    Tcp(SocketAddr),
    Unix,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DetectedProtocol {
    Http1,
    Http2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientAddr {
    pub host: Box<str>,
    pub port: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerAddr {
    pub host: Box<str>,
    pub port: Option<u16>,
}

#[derive(Clone, Debug)]
pub struct ConnectionStart {
    pub proxy: Option<ProxyInfo>,
    pub protocol: DetectedProtocol,
}

#[derive(Clone, Debug)]
pub struct ProxyInfo {
    pub client: Option<ClientAddr>,
    pub server: Option<ServerAddr>,
}

pub struct ConnectionInfo {
    pub actual_peer: ConnectionPeer,
    pub actual_server: Option<ServerAddr>,
    pub proxy_headers_trusted: bool,
    pub client: Option<ClientAddr>,
    pub server: Option<ServerAddr>,
}

impl ConnectionInfo {
    pub fn from_peer(
        actual_peer: ConnectionPeer,
        actual_server: Option<ServerAddr>,
        proxy_headers_trusted: bool,
    ) -> Self {
        Self {
            client: match &actual_peer {
                ConnectionPeer::Tcp(peer) => Some(ClientAddr {
                    host: peer.ip().to_string().into(),
                    port: peer.port(),
                }),
                ConnectionPeer::Unix => None,
            },
            actual_peer,
            actual_server,
            proxy_headers_trusted,
            server: None,
        }
    }

    pub fn apply_proxy_info(&mut self, proxy: ProxyInfo) {
        if let Some(client) = proxy.client {
            self.client = Some(client);
        }
        if let Some(server) = proxy.server {
            self.server = Some(server);
        }
    }
}

pub fn parse_trusted_peer(value: &str) -> Result<TrustedPeer, H2CornError> {
    if value == "*" {
        return Ok(TrustedPeer::Any);
    }
    if value.eq_ignore_ascii_case("unix") {
        return Ok(TrustedPeer::Unix);
    }
    if let Some((host, prefix)) = value.split_once('/') {
        let network = host
            .parse::<IpAddr>()
            .map_err(|_| ConfigError::invalid_trusted_proxy_entry(value))?;
        let prefix = prefix
            .parse::<u8>()
            .map_err(|_| ConfigError::invalid_trusted_proxy_entry(value))?;
        let max_prefix = match network {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix > max_prefix {
            return ConfigError::invalid_trusted_proxy_cidr_prefix(value).err();
        }
        return Ok(TrustedPeer::Cidr(Cidr::new(network, prefix)));
    }
    let ip = value
        .parse::<IpAddr>()
        .map_err(|_| ConfigError::invalid_trusted_proxy_entry(value))?;
    Ok(TrustedPeer::Ip(ip))
}

pub fn peer_is_trusted(trusted: &[TrustedPeer], actual_peer: &ConnectionPeer) -> bool {
    match actual_peer {
        ConnectionPeer::Unix => trusted.iter().any(TrustedPeer::matches_unix),
        ConnectionPeer::Tcp(peer) => trusted.iter().any(|entry| entry.matches_ip(peer.ip())),
    }
}

pub fn trusted_host_matches(trusted: &[TrustedPeer], host: &str, is_unix: bool) -> bool {
    if is_unix {
        return trusted.iter().any(TrustedPeer::matches_unix);
    }
    let Ok(ip) = host.parse::<IpAddr>() else {
        return false;
    };
    trusted.iter().any(|entry| entry.matches_ip(ip))
}

async fn prepare_proxy_read<R>(
    reader: &mut FrameReader<R>,
    actual_peer: &ConnectionPeer,
    trusted: &[TrustedPeer],
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin,
{
    if !peer_is_trusted(trusted, actual_peer) {
        return ProxyError::ProtocolRequiresTrustedPeer.err();
    }

    if !reader.read_at_least(12).await? {
        return ProxyError::ClosedBeforeProxyOrHttp2Preface.err();
    }
    Ok(())
}

pub(crate) async fn read_proxy_v2<R>(
    reader: &mut FrameReader<R>,
    actual_peer: &ConnectionPeer,
    trusted: &[TrustedPeer],
) -> Result<Option<ProxyInfo>, H2CornError>
where
    R: AsyncRead + Unpin + Send,
{
    prepare_proxy_read(reader, actual_peer, trusted).await?;
    if !reader.buffered().starts_with(&PROXY_V2_SIG) {
        return ProxyError::ExpectedProxyV2HeaderBeforeHttp2Preface.err();
    }

    if !reader.read_at_least(size_of::<ProxyV2Header>()).await? {
        return ProxyError::ClosedWhileReadingProxyV2Header.err();
    }
    let header =
        Ref::<_, ProxyV2Header>::from_bytes(&reader.buffered()[..size_of::<ProxyV2Header>()])
            .map_err(|_| ProxyError::InvalidProxyV2Header)?;
    let payload_len = usize::from(header.payload_len.get());
    let total_len = size_of::<ProxyV2Header>() + payload_len;
    if !reader.read_at_least(total_len).await? {
        return ProxyError::ClosedWhileReadingProxyV2Header.err();
    }
    let proxy = parse_proxy_v2(&reader.buffered()[..total_len])?;
    reader.consume(total_len);
    Ok(proxy)
}

pub(crate) async fn read_proxy_v1<R>(
    reader: &mut FrameReader<R>,
    actual_peer: &ConnectionPeer,
    trusted: &[TrustedPeer],
) -> Result<Option<ProxyInfo>, H2CornError>
where
    R: AsyncRead + Unpin + Send,
{
    prepare_proxy_read(reader, actual_peer, trusted).await?;
    if !reader.buffered().starts_with(b"PROXY ") {
        return ProxyError::ExpectedProxyV1HeaderBeforeHttp2Preface.err();
    }

    loop {
        if let Some(end) = find_crlf(reader.buffered()) {
            let line_len = end + 2;
            if line_len > PROXY_V1_MAX_LEN {
                return ProxyError::ProxyV1HeaderTooLong.err();
            }
            let proxy = parse_proxy_v1(&reader.buffered()[..line_len])?;
            reader.consume(line_len);
            return Ok(proxy);
        }
        let read_cap = PROXY_V1_MAX_LEN.saturating_sub(reader.buffered().len());
        if read_cap == 0 {
            return ProxyError::ProxyV1HeaderTooLong.err();
        }
        if !reader.read_more_capped(read_cap).await? {
            return ProxyError::ClosedWhileReadingProxyV1Header.err();
        }
    }
}

pub(crate) async fn read_preamble_protocol<R, const HTTP1: bool>(
    reader: &mut FrameReader<R>,
) -> Result<DetectedProtocol, H2CornError>
where
    R: AsyncRead + Unpin,
{
    if HTTP1 {
        detect_protocol(reader).await
    } else {
        read_h2_preface(reader).await?;
        Ok(DetectedProtocol::Http2)
    }
}

pub async fn read_h2_preface<R>(reader: &mut FrameReader<R>) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin,
{
    if !reader.read_at_least(CONNECTION_PREFACE.len()).await? {
        return ProxyError::ClosedBeforeHttp2Preface.err();
    }
    if &reader.buffered()[..CONNECTION_PREFACE.len()] != CONNECTION_PREFACE {
        return ProxyError::InvalidHttp2Preface.err();
    }
    reader.consume(CONNECTION_PREFACE.len());
    Ok(())
}

async fn detect_protocol<R>(reader: &mut FrameReader<R>) -> Result<DetectedProtocol, H2CornError>
where
    R: AsyncRead + Unpin,
{
    if !reader.read_at_least(1).await? {
        return ProxyError::ClosedBeforeAnyRequestBytes.err();
    }
    if reader.buffered()[0] != CONNECTION_PREFACE[0] {
        return Ok(DetectedProtocol::Http1);
    }
    let need = CONNECTION_PREFACE.len();
    if !reader.read_at_least(need).await? {
        return ProxyError::ClosedBeforeProtocolDetection.err();
    }
    if &reader.buffered()[..need] == CONNECTION_PREFACE {
        read_h2_preface(reader).await?;
        Ok(DetectedProtocol::Http2)
    } else if reader.buffered().starts_with(HTTP2_PREFACE_LEAD) {
        ProxyError::InvalidHttp2Preface.err()
    } else {
        Ok(DetectedProtocol::Http1)
    }
}

fn parse_proxy_v1(line: &[u8]) -> Result<Option<ProxyInfo>, H2CornError> {
    fn invalid_proxy_v1() -> H2CornError {
        ProxyError::InvalidProxyV1Header.into_error()
    }

    fn take_proxy_v1_part<'a>(line: &mut &'a [u8]) -> Option<&'a [u8]> {
        let current = *line;
        if current.is_empty() {
            return None;
        }
        if let Some(index) = current.iter().position(|&byte| byte == b' ') {
            *line = &current[index + 1..];
            Some(&current[..index])
        } else {
            *line = &[];
            Some(current)
        }
    }

    let line = line
        .strip_suffix(b"\r\n")
        .ok_or(ProxyError::ProxyV1HeaderMissingCrlf)?;
    let mut parts = line;
    if take_proxy_v1_part(&mut parts) != Some(b"PROXY".as_slice()) {
        return Err(invalid_proxy_v1());
    }
    let Some(protocol) = take_proxy_v1_part(&mut parts) else {
        return Err(invalid_proxy_v1());
    };
    let transport = match protocol {
        b"UNKNOWN" => return Ok(None),
        b"TCP4" => ProxyV1Transport::Tcp4,
        b"TCP6" => ProxyV1Transport::Tcp6,
        _ => {
            return ProxyError::UnsupportedProxyV1Transport.err();
        }
    };

    let Some(client_host) = take_proxy_v1_part(&mut parts) else {
        return Err(invalid_proxy_v1());
    };
    let Some(server_host) = take_proxy_v1_part(&mut parts) else {
        return Err(invalid_proxy_v1());
    };
    let Some(client_port) = take_proxy_v1_part(&mut parts) else {
        return Err(invalid_proxy_v1());
    };
    let Some(server_port) = take_proxy_v1_part(&mut parts) else {
        return Err(invalid_proxy_v1());
    };
    if take_proxy_v1_part(&mut parts).is_some() {
        return Err(invalid_proxy_v1());
    }

    let parse_ip = |value: &[u8], invalid_error: fn() -> ProxyError| {
        let value = str::from_utf8(value).map_err(|_| invalid_error())?;
        let ip = value.parse::<IpAddr>().map_err(|_| invalid_error())?;
        match (transport, ip) {
            (ProxyV1Transport::Tcp4, IpAddr::V4(ip)) => Ok(IpAddr::V4(ip)),
            (ProxyV1Transport::Tcp6, IpAddr::V6(ip)) => Ok(IpAddr::V6(ip)),
            _ => ProxyError::ProxyV1AddressFamilyMismatch.err(),
        }
    };
    Ok(Some(ProxyInfo {
        client: Some(ClientAddr {
            host: parse_ip(client_host, || ProxyError::InvalidProxyV1SourceAddress)?
                .to_string()
                .into(),
            port: parse_pos::<u16, false>(client_port).map_err(|_| ProxyError::InvalidProxyPort)?,
        }),
        server: Some(ServerAddr {
            host: parse_ip(server_host, || ProxyError::InvalidProxyV1DestinationAddress)?
                .to_string()
                .into(),
            port: Some(
                parse_pos::<u16, false>(server_port).map_err(|_| ProxyError::InvalidProxyPort)?,
            ),
        }),
    }))
}

fn find_crlf(buffer: &[u8]) -> Option<usize> {
    let mut start = 0;
    while let Some(offset) = memchr(b'\n', &buffer[start..]) {
        let index = start + offset;
        if index > 0 && buffer[index - 1] == b'\r' {
            return Some(index - 1);
        }
        start = index + 1;
    }
    None
}

fn parse_proxy_v2(frame: &[u8]) -> Result<Option<ProxyInfo>, H2CornError> {
    let header = Ref::<_, ProxyV2Header>::from_bytes(
        frame
            .get(..size_of::<ProxyV2Header>())
            .ok_or(ProxyError::InvalidProxyV2Header)?,
    )
    .map_err(|_| ProxyError::InvalidProxyV2Header)?;
    if header.sig != PROXY_V2_SIG {
        return ProxyError::InvalidProxyV2Header.err();
    }

    let version = header.version_command >> 4;
    if version != 2 {
        return ProxyError::UnsupportedProxyV2Version.err();
    }
    let command = header.version_command & 0x0f;
    let family = header.family_transport >> 4;
    let transport = header.family_transport & 0x0f;
    let payload_len = usize::from(header.payload_len.get());
    if frame.len() != 16 + payload_len {
        return ProxyError::TruncatedProxyV2Header.err();
    }
    match command {
        0x0 => return Ok(None),
        0x1 => {}
        _ => {
            return ProxyError::UnsupportedProxyV2Command.err();
        }
    }
    if transport != 0x1 {
        return ProxyError::UnsupportedProxyV2Transport.err();
    }

    let payload = &frame[16..];
    match family {
        0x0 => Ok(None),
        0x1 => {
            let (addrs, _) = Ref::<_, ProxyV2Ipv4Addrs>::from_prefix(payload)
                .map_err(|_| ProxyError::InvalidProxyV2Ipv4Payload)?;
            let server = IpAddr::V4(Ipv4Addr::from(addrs.server_ip));
            Ok(Some(ProxyInfo {
                client: Some(ClientAddr {
                    host: IpAddr::V4(Ipv4Addr::from(addrs.client_ip))
                        .to_string()
                        .into(),
                    port: addrs.client_port.get(),
                }),
                server: if server.is_unspecified() && addrs.server_port.get() == 0 {
                    None
                } else {
                    Some(ServerAddr {
                        host: server.to_string().into(),
                        port: Some(addrs.server_port.get()),
                    })
                },
            }))
        }
        0x2 => {
            let (addrs, _) = Ref::<_, ProxyV2Ipv6Addrs>::from_prefix(payload)
                .map_err(|_| ProxyError::InvalidProxyV2Ipv6Payload)?;
            let server = IpAddr::V6(Ipv6Addr::from(addrs.server_ip));
            Ok(Some(ProxyInfo {
                client: Some(ClientAddr {
                    host: IpAddr::V6(Ipv6Addr::from(addrs.client_ip))
                        .to_string()
                        .into(),
                    port: addrs.client_port.get(),
                }),
                server: if server.is_unspecified() && addrs.server_port.get() == 0 {
                    None
                } else {
                    Some(ServerAddr {
                        host: server.to_string().into(),
                        port: Some(addrs.server_port.get()),
                    })
                },
            }))
        }
        _ => ProxyError::UnsupportedProxyV2AddressFamily.err(),
    }
}

impl Cidr {
    fn new(network: IpAddr, prefix: u8) -> Self {
        match network {
            IpAddr::V4(network) => {
                let mask = prefix_to_mask_v4(prefix);
                Self::V4 {
                    network: u32::from(network) & mask,
                    mask,
                }
            }
            IpAddr::V6(network) => {
                let mask = prefix_to_mask_v6(prefix);
                Self::V6 {
                    network: u128::from(network) & mask,
                    mask,
                }
            }
        }
    }

    fn contains(self, ip: IpAddr) -> bool {
        match (self, ip) {
            (Self::V4 { network, mask }, IpAddr::V4(ip)) => u32::from(ip) & mask == network,
            (Self::V6 { network, mask }, IpAddr::V6(ip)) => u128::from(ip) & mask == network,
            _ => false,
        }
    }
}

const fn prefix_to_mask_v4(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (u32::BITS - prefix as u32)
    }
}

const fn prefix_to_mask_v6(prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        u128::MAX << (u128::BITS - prefix as u32)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncWriteExt, duplex};

    use crate::error::H2CornError;
    use crate::frame::FrameReader;

    use super::*;

    #[test]
    fn trusted_peer_cidr_matches_ipv4_and_ipv6() {
        let ipv4 = parse_trusted_peer("10.0.0.0/24").unwrap();
        let ipv6 = parse_trusted_peer("2001:db8::/32").unwrap();

        match ipv4 {
            TrustedPeer::Cidr(cidr) => {
                assert!(cidr.contains("10.0.0.42".parse().unwrap()));
                assert!(!cidr.contains("10.0.1.42".parse().unwrap()));
            }
            _ => panic!("expected IPv4 CIDR"),
        }

        match ipv6 {
            TrustedPeer::Cidr(cidr) => {
                assert!(cidr.contains("2001:db8::1".parse().unwrap()));
                assert!(!cidr.contains("2001:db9::1".parse().unwrap()));
            }
            _ => panic!("expected IPv6 CIDR"),
        }
    }

    #[test]
    fn parse_proxy_v2_ipv4() {
        let mut frame = Vec::from(PROXY_V2_SIG);
        frame.extend_from_slice(&[
            0x21, 0x11, 0x00, 0x0c, 192, 0, 2, 1, 198, 51, 100, 7, 0x1f, 0x90, 0x00, 0x50,
        ]);

        let proxy = parse_proxy_v2(&frame).unwrap().unwrap();

        assert_eq!(
            proxy.client,
            Some(ClientAddr {
                host: "192.0.2.1".into(),
                port: 8080,
            })
        );
        assert_eq!(
            proxy.server,
            Some(ServerAddr {
                host: "198.51.100.7".into(),
                port: Some(80),
            })
        );
    }

    #[test]
    fn parse_proxy_v2_ipv6() {
        let mut frame = Vec::from(PROXY_V2_SIG);
        frame.extend_from_slice(&[0x21, 0x21, 0x00, 0x24]);
        frame.extend_from_slice(&[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        frame.extend_from_slice(&[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);
        frame.extend_from_slice(&[0x1f, 0x90, 0x00, 0x50]);

        let proxy = parse_proxy_v2(&frame).unwrap().unwrap();

        assert_eq!(
            proxy.client,
            Some(ClientAddr {
                host: "2001:db8::1".into(),
                port: 8080,
            })
        );
        assert_eq!(
            proxy.server,
            Some(ServerAddr {
                host: "2001:db8::2".into(),
                port: Some(80),
            })
        );
    }

    #[tokio::test]
    async fn read_proxy_v2_rejects_truncated_header_after_signature() {
        let (client, mut server) = duplex(64);
        server.write_all(&PROXY_V2_SIG).await.unwrap();
        server.write_all(&[0x21, 0x11, 0x00]).await.unwrap();
        drop(server);

        let mut reader = FrameReader::new(client);
        let err = read_proxy_v2(
            &mut reader,
            &ConnectionPeer::Tcp("127.0.0.1:8080".parse().unwrap()),
            &[TrustedPeer::Any],
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            H2CornError::Proxy(ProxyError::ClosedWhileReadingProxyV2Header)
        ));
    }
}
