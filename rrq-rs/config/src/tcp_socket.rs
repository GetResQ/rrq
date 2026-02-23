use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};

use anyhow::{Context, Result};

/// Parsed and validated TCP socket specification for a runner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpSocketSpec {
    pub host: IpAddr,
    pub port: u16,
}

impl TcpSocketSpec {
    /// Returns a socket address with the given port (used for pool port allocation).
    #[must_use]
    pub fn addr(&self, port: u16) -> SocketAddr {
        SocketAddr::new(self.host, port)
    }
}

/// Parses and validates a tcp_socket string (e.g., "127.0.0.1:9000").
///
/// Validates:
/// - Format is `host:port` or `[ipv6]:port`
/// - Host is localhost/loopback only (127.0.0.1, ::1, localhost)
/// - Port is > 0 and <= 65535
pub fn parse_tcp_socket(raw: &str) -> Result<TcpSocketSpec> {
    parse_tcp_socket_with_allowed_hosts(raw, &[])
}

/// Parses and validates a tcp_socket string with an explicit non-loopback allowlist.
///
/// By default RRQ requires loopback hosts only. When `allowed_hosts` contains the
/// tcp host exactly (case-insensitive), non-loopback hosts are allowed.
pub fn parse_tcp_socket_with_allowed_hosts(
    raw: &str,
    allowed_hosts: &[String],
) -> Result<TcpSocketSpec> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(anyhow::anyhow!("tcp_socket cannot be empty"));
    }

    let (host, port_str) = if let Some(rest) = raw.strip_prefix('[') {
        // IPv6 format: [::1]:port
        let (host, port_str) = rest
            .split_once("]:")
            .ok_or_else(|| anyhow::anyhow!("tcp_socket must be in [host]:port format"))?;
        (host, port_str)
    } else {
        // IPv4 or hostname format: host:port
        let (host, port_str) = raw
            .rsplit_once(':')
            .ok_or_else(|| anyhow::anyhow!("tcp_socket must be in host:port format"))?;
        if host.is_empty() {
            return Err(anyhow::anyhow!("tcp_socket host cannot be empty"));
        }
        (host, port_str)
    };

    let port: u16 = port_str
        .parse()
        .with_context(|| format!("invalid tcp_socket port '{port_str}' - must be 1-65535"))?;
    if port == 0 {
        return Err(anyhow::anyhow!("tcp_socket port must be > 0"));
    }

    let is_allowed_host = allowed_hosts
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(host));

    let ip = if host == "localhost" {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    } else {
        match host.parse::<IpAddr>() {
            Ok(parsed) => {
                if !parsed.is_loopback() && !is_allowed_host {
                    return Err(anyhow::anyhow!(
                        "tcp_socket host must be loopback (127.0.0.1, ::1, or localhost) \
for security, or explicitly listed in allowed_hosts - got '{host}'"
                    ));
                }
                parsed
            }
            Err(_) => {
                if !is_allowed_host {
                    return Err(anyhow::anyhow!(
                        "tcp_socket hostname '{host}' is not allowed; add it to allowed_hosts"
                    ));
                }
                let resolved = (host, port)
                    .to_socket_addrs()
                    .with_context(|| format!("invalid tcp_socket host '{host}'"))?
                    .next()
                    .ok_or_else(|| {
                        anyhow::anyhow!("invalid tcp_socket host '{host}' - no addresses resolved")
                    })?;
                resolved.ip()
            }
        }
    };

    Ok(TcpSocketSpec { host: ip, port })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tcp_socket_ipv4_localhost() {
        let spec = parse_tcp_socket("127.0.0.1:9000").unwrap();
        assert_eq!(spec.host, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(spec.port, 9000);
    }

    #[test]
    fn parse_tcp_socket_localhost_hostname() {
        let spec = parse_tcp_socket("localhost:1234").unwrap();
        assert_eq!(spec.host, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(spec.port, 1234);
    }

    #[test]
    fn parse_tcp_socket_ipv6_loopback() {
        let spec = parse_tcp_socket("[::1]:8080").unwrap();
        assert!(spec.host.is_loopback());
        assert_eq!(spec.port, 8080);
    }

    #[test]
    fn parse_tcp_socket_rejects_non_loopback() {
        let err = parse_tcp_socket("10.0.0.1:1234").unwrap_err();
        assert!(err.to_string().contains("loopback"));
    }

    #[test]
    fn parse_tcp_socket_allows_non_loopback_ip_when_allowlisted() {
        let spec = parse_tcp_socket_with_allowed_hosts("10.0.0.1:1234", &["10.0.0.1".to_string()])
            .unwrap();
        assert_eq!(spec.host, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(spec.port, 1234);
    }

    #[test]
    fn parse_tcp_socket_rejects_hostname_without_allowlist() {
        let err = parse_tcp_socket("docker-runner:9000").unwrap_err();
        assert!(err.to_string().contains("allowed_hosts"));
    }

    #[test]
    fn parse_tcp_socket_rejects_zero_port() {
        let err = parse_tcp_socket("127.0.0.1:0").unwrap_err();
        assert!(err.to_string().contains("port must be > 0"));
    }

    #[test]
    fn parse_tcp_socket_rejects_empty() {
        let err = parse_tcp_socket("").unwrap_err();
        assert!(err.to_string().contains("cannot be empty"));
    }

    #[test]
    fn parse_tcp_socket_rejects_missing_port() {
        let err = parse_tcp_socket("127.0.0.1").unwrap_err();
        assert!(err.to_string().contains("host:port"));
    }

    #[test]
    fn parse_tcp_socket_rejects_invalid_port() {
        let err = parse_tcp_socket("127.0.0.1:abc").unwrap_err();
        assert!(err.to_string().contains("port"));
    }
}
