//! Where a lookup is allowed to go: one public HTTPS address.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use super::StepError;

/// A validated address for a team's API.
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub url: reqwest::Url,
    pub host: String,
    pub port: u16,
}

const TYPO_FIX: &str =
    "Check the address for a typo. It should look like https://api.yourcompany.com/graphql";

/// Accepts only `https://host[:port]/path` with no embedded username or password.
pub fn parse_endpoint(raw: &str) -> Result<Endpoint, StepError> {
    let raw = raw.trim();
    if raw.len() > 2000 {
        return Err(StepError::new(
            "reach",
            "That address is too long.",
            TYPO_FIX,
        ));
    }
    let url = reqwest::Url::parse(raw)
        .map_err(|_| StepError::new("reach", "That doesn't look like a web address.", TYPO_FIX))?;
    if url.scheme() != "https" {
        return Err(StepError::new(
            "reach",
            "The address must start with https://",
            "Use the secure (https) address of your API.",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(StepError::new(
            "reach",
            "The address must not contain a username or password.",
            "Remove the part before the @ and paste the key in the next step instead.",
        ));
    }
    let host = match url.host() {
        Some(url::Host::Domain(d)) => d.to_ascii_lowercase(),
        Some(url::Host::Ipv4(ip)) => {
            ensure_public(IpAddr::V4(ip))?;
            ip.to_string()
        }
        Some(url::Host::Ipv6(ip)) => {
            ensure_public(IpAddr::V6(ip))?;
            ip.to_string()
        }
        None => {
            return Err(StepError::new(
                "reach",
                "That address has no host name.",
                TYPO_FIX,
            ))
        }
    };
    if host == "localhost" || host.ends_with(".localhost") || host.ends_with(".internal") {
        return Err(private_address());
    }
    let port = url.port_or_known_default().unwrap_or(443);
    Ok(Endpoint { url, host, port })
}

fn private_address() -> StepError {
    StepError::new(
        "reach",
        "That address is inside a private network, so we can't reach it from here.",
        "Use the address your apps reach over the internet.",
    )
}

fn ensure_public(ip: IpAddr) -> Result<(), StepError> {
    if is_public_ip(ip) {
        Ok(())
    } else {
        Err(private_address())
    }
}

/// True only for addresses routable on the public internet. Everything that
/// could point back into a private network, a cloud metadata service or this
/// host itself is false.
pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_v4(v4),
        IpAddr::V6(v6) => is_public_v6(v6),
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_unspecified()
        || ip.is_multicast()
        || o[0] == 0
        // carrier-grade NAT 100.64.0.0/10
        || (o[0] == 100 && (o[1] & 0xc0) == 64)
        // IETF protocol assignments 192.0.0.0/24
        || (o[0] == 192 && o[1] == 0 && o[2] == 0)
        // benchmarking 198.18.0.0/15
        || (o[0] == 198 && (o[1] & 0xfe) == 18)
        // reserved 240.0.0.0/4
        || o[0] >= 240)
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_public_v4(v4);
    }
    let s = ip.segments();
    !(ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        // unique local fc00::/7
        || (s[0] & 0xfe00) == 0xfc00
        // link local fe80::/10
        || (s[0] & 0xffc0) == 0xfe80
        // IPv4-compatible ::/96 and NAT64 64:ff9b::/96 can embed a private IPv4
        || s[..6] == [0, 0, 0, 0, 0, 0]
        || (s[0] == 0x64 && s[1] == 0xff9b)
        // documentation 2001:db8::/32
        || (s[0] == 0x2001 && s[1] == 0xdb8))
}

/// Resolves the host and returns one address to connect to. Refuses when ANY
/// resolved address is not public, so a name that answers with both a public
/// and a private address cannot be used to reach inside a network. The caller
/// pins the connection to the returned address.
pub async fn resolve_public(ep: &Endpoint) -> Result<SocketAddr, StepError> {
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((ep.host.as_str(), ep.port))
        .await
        .map_err(|_| {
            StepError::new(
                "reach",
                format!("We couldn't find {} on the internet.", ep.host),
                TYPO_FIX,
            )
        })?
        .collect();
    pick_public(&addrs)
}

fn pick_public(addrs: &[SocketAddr]) -> Result<SocketAddr, StepError> {
    if addrs.is_empty() {
        return Err(StepError::new(
            "reach",
            "That address doesn't point anywhere.",
            TYPO_FIX,
        ));
    }
    if addrs.iter().any(|a| !is_public_ip(a.ip())) {
        return Err(private_address());
    }
    Ok(addrs[0])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(raw: &str) -> String {
        parse_endpoint(raw).unwrap_err().message
    }

    #[test]
    fn accepts_a_plain_https_address() {
        let ep = parse_endpoint(" https://API.Example.com/graphql ").unwrap();
        assert_eq!(ep.host, "api.example.com");
        assert_eq!(ep.port, 443);
        assert_eq!(
            parse_endpoint("https://api.example.com:8443/q")
                .unwrap()
                .port,
            8443
        );
    }

    #[test]
    fn refuses_anything_but_https() {
        assert!(err("http://api.example.com/graphql").contains("https://"));
        assert!(err("ftp://api.example.com").contains("https://"));
        assert!(err("file:///etc/passwd").contains("https://"));
        assert!(err("not a url").contains("web address"));
    }

    #[test]
    fn refuses_embedded_credentials() {
        assert!(err("https://user:pw@api.example.com/").contains("username or password"));
        assert!(err("https://user@api.example.com/").contains("username or password"));
    }

    #[test]
    fn refuses_private_and_local_hosts() {
        for raw in [
            "https://localhost/graphql",
            "https://app.localhost/graphql",
            "https://db.internal/graphql",
            "https://127.0.0.1/graphql",
            "https://10.1.2.3/graphql",
            "https://172.16.0.9/graphql",
            "https://192.168.1.1/graphql",
            "https://169.254.169.254/latest/meta-data/",
            "https://100.64.0.1/",
            "https://0.0.0.0/",
            "https://[::1]/graphql",
            "https://[fd00::1]/graphql",
            "https://[fe80::1]/graphql",
            "https://[::ffff:10.0.0.1]/graphql",
            "https://[::ffff:169.254.169.254]/",
            "https://[64:ff9b::a00:1]/",
        ] {
            assert!(
                err(raw).contains("private network"),
                "{raw} should be refused"
            );
        }
    }

    #[test]
    fn public_addresses_pass() {
        assert!(is_public_ip("8.8.8.8".parse().unwrap()));
        assert!(is_public_ip("2606:4700:4700::1111".parse().unwrap()));
        assert!(parse_endpoint("https://8.8.8.8/graphql").is_ok());
    }

    #[test]
    fn one_private_answer_poisons_the_whole_name() {
        let public: SocketAddr = "8.8.8.8:443".parse().unwrap();
        let private: SocketAddr = "10.0.0.5:443".parse().unwrap();
        assert_eq!(pick_public(&[public]).unwrap(), public);
        assert!(pick_public(&[public, private]).is_err());
        assert!(pick_public(&[private, public]).is_err());
        assert!(pick_public(&[]).is_err());
    }
}
