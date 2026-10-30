//! One DNS TXT lookup over UDP, through the resolver the system names
//! (`/etc/resolv.conf`), for the email setup check's DMARC record. No
//! third-party resolver is asked: the lookup goes where the install's own
//! lookups go.

use std::net::SocketAddr;
use std::time::Duration;

/// The query for `name`'s TXT records (recursion desired), with `id`.
pub fn query(id: u16, name: &str) -> Option<Vec<u8>> {
    let mut q = Vec::with_capacity(32 + name.len());
    q.extend_from_slice(&id.to_be_bytes());
    q.extend_from_slice(&[0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]);
    for label in name.trim_end_matches('.').split('.') {
        if label.is_empty() || label.len() > 63 {
            return None;
        }
        q.push(label.len() as u8);
        q.extend_from_slice(label.as_bytes());
    }
    q.push(0);
    q.extend_from_slice(&[0, 16, 0, 1]);
    Some(q)
}

/// Skips a (possibly compressed) name at `i`; the index after it.
fn skip_name(b: &[u8], mut i: usize) -> Option<usize> {
    loop {
        let len = *b.get(i)? as usize;
        if len == 0 {
            return Some(i + 1);
        }
        if len & 0xC0 == 0xC0 {
            return Some(i + 2);
        }
        i += 1 + len;
    }
}

/// The TXT strings in an answer to query `id` (each record's strings
/// joined). None for an answer that isn't one, or a server failure; an
/// empty list for a name with no TXT record.
pub fn parse(id: u16, b: &[u8]) -> Option<Vec<String>> {
    if b.len() < 12 || u16::from_be_bytes([b[0], b[1]]) != id || b[2] & 0x80 == 0 {
        return None;
    }
    let rcode = b[3] & 0x0F;
    if rcode == 3 {
        return Some(vec![]);
    }
    if rcode != 0 {
        return None;
    }
    let qd = u16::from_be_bytes([b[4], b[5]]) as usize;
    let an = u16::from_be_bytes([b[6], b[7]]) as usize;
    let mut i = 12;
    for _ in 0..qd {
        i = skip_name(b, i)? + 4;
    }
    let mut out = vec![];
    for _ in 0..an {
        i = skip_name(b, i)?;
        let ty = u16::from_be_bytes([*b.get(i)?, *b.get(i + 1)?]);
        let len = u16::from_be_bytes([*b.get(i + 8)?, *b.get(i + 9)?]) as usize;
        let data = b.get(i + 10..i + 10 + len)?;
        i += 10 + len;
        if ty != 16 {
            continue;
        }
        let mut s = String::new();
        let mut j = 0;
        while j < data.len() {
            let l = data[j] as usize;
            s.push_str(&String::from_utf8_lossy(data.get(j + 1..j + 1 + l)?));
            j += 1 + l;
        }
        out.push(s);
    }
    Some(out)
}

/// The first nameserver in a resolv.conf.
pub fn nameserver(conf: &str) -> Option<SocketAddr> {
    conf.lines()
        .filter_map(|l| l.trim().strip_prefix("nameserver"))
        .filter_map(|a| a.trim().parse::<std::net::IpAddr>().ok())
        .map(|ip| SocketAddr::new(ip, 53))
        .next()
}

/// Looks up `name`'s TXT records. Err when no answer came.
pub async fn lookup(name: &str) -> Result<Vec<String>, String> {
    let conf = tokio::fs::read_to_string("/etc/resolv.conf")
        .await
        .unwrap_or_default();
    let server = nameserver(&conf).ok_or("no nameserver")?;
    let id: u16 = rand::random();
    let q = query(id, name).ok_or("not a DNS name")?;
    let bind: SocketAddr = if server.is_ipv4() {
        "0.0.0.0:0".parse().unwrap_or(server)
    } else {
        "[::]:0".parse().unwrap_or(server)
    };
    let sock = tokio::net::UdpSocket::bind(bind)
        .await
        .map_err(|e| e.to_string())?;
    sock.connect(server).await.map_err(|e| e.to_string())?;
    for _ in 0..2 {
        sock.send(&q).await.map_err(|e| e.to_string())?;
        let mut buf = vec![0u8; 4096];
        if let Ok(Ok(n)) = tokio::time::timeout(Duration::from_secs(2), sock.recv(&mut buf)).await {
            if let Some(v) = parse(id, &buf[..n]) {
                return Ok(v);
            }
        }
    }
    Err("no answer".into())
}

/// A domain's DMARC policy (`p=`), from its `_dmarc` TXT records.
pub fn dmarc_policy(records: &[String]) -> Option<String> {
    records
        .iter()
        .find(|r| r.trim_start().to_ascii_lowercase().starts_with("v=dmarc1"))
        .map(|r| {
            r.split(';')
                .filter_map(|p| p.trim().split_once('='))
                .find(|(k, _)| k.trim().eq_ignore_ascii_case("p"))
                .map(|(_, v)| v.trim().to_ascii_lowercase())
                .unwrap_or_else(|| "none".into())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An answer to `query(7, "_dmarc.example.com")` with one TXT record of
    /// two strings, its name compressed to the question.
    fn answer(rcode: u8, strings: &[&str]) -> Vec<u8> {
        let mut b = query(7, "_dmarc.example.com").unwrap();
        b[2] = 0x81;
        b[3] = 0x80 | rcode;
        let n = if strings.is_empty() { 0 } else { 1 };
        b[7] = n;
        if n == 1 {
            let mut data = vec![];
            for s in strings {
                data.push(s.len() as u8);
                data.extend_from_slice(s.as_bytes());
            }
            b.extend_from_slice(&[0xC0, 12, 0, 16, 0, 1, 0, 0, 0, 60]);
            b.extend_from_slice(&(data.len() as u16).to_be_bytes());
            b.extend_from_slice(&data);
        }
        b
    }

    #[test]
    fn a_query_names_each_label() {
        let q = query(1, "_dmarc.example.com.").unwrap();
        assert_eq!(&q[12..19], b"\x06_dmarc");
        assert_eq!(&q[q.len() - 4..], &[0, 16, 0, 1]);
        assert!(query(1, "a..b").is_none());
    }

    #[test]
    fn answers_are_read() {
        let a = answer(0, &["v=DMARC1; p=reject;", " rua=mailto:d@example.com"]);
        let txt = parse(7, &a).unwrap();
        assert_eq!(
            txt,
            vec!["v=DMARC1; p=reject; rua=mailto:d@example.com".to_string()]
        );
        assert_eq!(dmarc_policy(&txt).as_deref(), Some("reject"));
        // No such name: no records. A server failure or another id: no answer.
        assert_eq!(parse(7, &answer(3, &[])).unwrap(), Vec::<String>::new());
        assert!(parse(7, &answer(2, &[])).is_none());
        assert!(parse(8, &a).is_none());
        assert!(parse(7, &a[..20]).is_none());
    }

    #[test]
    fn dmarc_policies() {
        assert_eq!(dmarc_policy(&["v=spf1 -all".into()]), None);
        assert_eq!(
            dmarc_policy(&["v=DMARC1; p=quarantine".into()]).as_deref(),
            Some("quarantine")
        );
        assert_eq!(dmarc_policy(&["v=DMARC1".into()]).as_deref(), Some("none"));
        assert_eq!(
            nameserver("# x\nnameserver 169.254.78.1\nnameserver 8.8.8.8"),
            Some("169.254.78.1:53".parse().unwrap())
        );
    }
}
