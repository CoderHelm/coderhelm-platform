//! One DNS TXT lookup over UDP, through the resolver the system names
//! (`/etc/resolv.conf`), for the email setup check's DMARC record. No
//! third-party resolver is asked: the lookup goes where the install's own
//! lookups go.

use std::net::SocketAddr;
use std::time::Duration;

/// The UDP payload size the query offers (EDNS0): an answer with many TXT
/// values fits instead of coming back cut.
pub const EDNS_UDP_SIZE: u16 = 4096;

/// The query for `name`'s TXT records (recursion desired), with `id`, and an
/// EDNS0 OPT record offering [`EDNS_UDP_SIZE`] bytes.
pub fn query(id: u16, name: &str) -> Option<Vec<u8>> {
    let mut q = question(id, name)?;
    // ARCOUNT = 1, then OPT: root name, type 41, class = UDP size, TTL 0
    // (extended rcode, version 0, no flags), no data.
    q[11] = 1;
    q.push(0);
    q.extend_from_slice(&41u16.to_be_bytes());
    q.extend_from_slice(&EDNS_UDP_SIZE.to_be_bytes());
    q.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
    Some(q)
}

/// The header and question alone (what an answer repeats).
fn question(id: u16, name: &str) -> Option<Vec<u8>> {
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

/// Reads a (possibly compressed) name at `i`, lowercase without the final
/// dot. Pointers are followed a bounded number of times.
fn read_name(b: &[u8], mut i: usize) -> Option<String> {
    let mut labels: Vec<String> = Vec::new();
    for _ in 0..64 {
        let len = *b.get(i)? as usize;
        if len == 0 {
            return Some(labels.join("."));
        }
        if len & 0xC0 == 0xC0 {
            i = ((len & 0x3F) << 8) | *b.get(i + 1)? as usize;
            continue;
        }
        let label = b.get(i + 1..i + 1 + len)?;
        labels.push(String::from_utf8_lossy(label).to_ascii_lowercase());
        i += 1 + len;
    }
    None
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
/// joined). None for an answer that isn't one, a server failure, or a cut
/// answer (the TC flag: records may be missing, so it says nothing); an
/// empty list for a name with no TXT record.
pub fn parse(id: u16, b: &[u8]) -> Option<Vec<String>> {
    parse_for(id, b, None)
}

/// As [`parse`], keeping only answers whose owner name is `name` (case
/// insensitive): a TXT record at another name never counts for this one.
pub fn parse_for(id: u16, b: &[u8], name: Option<&str>) -> Option<Vec<String>> {
    if b.len() < 12 || u16::from_be_bytes([b[0], b[1]]) != id || b[2] & 0x80 == 0 {
        return None;
    }
    // Truncated: not a definite answer (a missing record may just be cut).
    if b[2] & 0x02 != 0 {
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
    // Every answer record first: (owner, type, data offset, data).
    let mut records: Vec<(String, u16, usize, &[u8])> = Vec::new();
    for _ in 0..an {
        let owner = read_name(b, i)?;
        i = skip_name(b, i)?;
        let ty = u16::from_be_bytes([*b.get(i)?, *b.get(i + 1)?]);
        let len = u16::from_be_bytes([*b.get(i + 8)?, *b.get(i + 9)?]) as usize;
        let data = b.get(i + 10..i + 10 + len)?;
        records.push((owner, ty, i + 10, data));
        i += 10 + len;
    }
    // The names that answer for the queried one: it, then each CNAME target
    // in the answer section from it, a few hops at most.
    let mut names: Vec<String> = Vec::new();
    if let Some(n) = name {
        names.push(n.trim_end_matches('.').to_ascii_lowercase());
        for _ in 0..8 {
            let last = names.last().cloned().unwrap_or_default();
            let next = records
                .iter()
                .find(|(o, ty, _, _)| *ty == 5 && *o == last)
                .and_then(|(_, _, at, _)| read_name(b, *at));
            match next {
                Some(t) if !names.contains(&t) => names.push(t),
                _ => break,
            }
        }
    }
    let mut out = vec![];
    for (owner, ty, _, data) in &records {
        if *ty != 16 || (name.is_some() && !names.contains(owner)) {
            continue;
        }
        let data: &[u8] = data;
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
            if let Some(v) = parse_for(id, &buf[..n], Some(name)) {
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
        let mut b = question(7, "_dmarc.example.com").unwrap();
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
        let qn = question(1, "_dmarc.example.com").unwrap();
        assert_eq!(&qn[qn.len() - 4..], &[0, 16, 0, 1]);
        assert!(query(1, "a..b").is_none());
        // One additional record: EDNS0 OPT offering a 4096-byte answer.
        assert_eq!(&q[10..12], &[0, 1]);
        assert_eq!(&q[..qn.len()][..10], &qn[..10]);
        assert_eq!(&q[qn.len()..], &[0, 0, 41, 0x10, 0x00, 0, 0, 0, 0, 0, 0]);
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

    /// An answer to `query(9, asked)` whose one TXT record is owned by
    /// `owner`, written out in full (no compression).
    fn answer_owned_by(asked: &str, owner: &str, txt: &str) -> Vec<u8> {
        let mut b = question(9, asked).unwrap();
        b[2] = 0x81;
        b[3] = 0x80;
        b[7] = 1;
        for label in owner.split('.') {
            b.push(label.len() as u8);
            b.extend_from_slice(label.as_bytes());
        }
        b.push(0);
        b.extend_from_slice(&[0, 16, 0, 1, 0, 0, 0, 60]);
        b.extend_from_slice(&((txt.len() + 1) as u16).to_be_bytes());
        b.push(txt.len() as u8);
        b.extend_from_slice(txt.as_bytes());
        b
    }

    #[test]
    fn an_answer_for_another_name_never_counts() {
        let asked = "_coderhelm-okta.acme.example";
        let same = answer_owned_by(asked, "_Coderhelm-Okta.ACME.example", "coderhelm-okta=1");
        assert_eq!(
            parse_for(9, &same, Some(asked)).unwrap(),
            vec!["coderhelm-okta=1"]
        );
        let other = answer_owned_by(asked, "_coderhelm-okta.evil.example", "coderhelm-okta=1");
        assert!(parse_for(9, &other, Some(asked)).unwrap().is_empty());
        // A compressed owner pointing at the question is the question's name.
        let a = answer(0, &["v=DMARC1; p=none"]);
        assert_eq!(
            parse_for(7, &a, Some("_dmarc.example.com.")).unwrap().len(),
            1
        );
        assert!(parse_for(7, &a, Some("_dmarc.other.com"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn a_cname_chain_in_the_answer_is_followed() {
        // _coderhelm-okta.acme.example CNAME proofs.dns.example, whose TXT counts.
        let asked = "_coderhelm-okta.acme.example";
        let mut b = question(9, asked).unwrap();
        b[2] = 0x81;
        b[3] = 0x80;
        b[7] = 3;
        let name = |b: &mut Vec<u8>, n: &str| {
            for l in n.split('.') {
                b.push(l.len() as u8);
                b.extend_from_slice(l.as_bytes());
            }
            b.push(0);
        };
        // CNAME: owner = the question (pointer to 12), target written out.
        b.extend_from_slice(&[0xC0, 12, 0, 5, 0, 1, 0, 0, 0, 60]);
        let mut target = Vec::new();
        name(&mut target, "proofs.dns.example");
        b.extend_from_slice(&(target.len() as u16).to_be_bytes());
        b.extend_from_slice(&target);
        // TXT at the target.
        name(&mut b, "proofs.dns.example");
        b.extend_from_slice(&[0, 16, 0, 1, 0, 0, 0, 60, 0, 3, 2, b'o', b'k']);
        // TXT at an unrelated name: never counts.
        name(&mut b, "other.example");
        b.extend_from_slice(&[0, 16, 0, 1, 0, 0, 0, 60, 0, 4, 3, b'b', b'a', b'd']);
        assert_eq!(parse_for(9, &b, Some(asked)).unwrap(), vec!["ok"]);
        assert!(parse_for(9, &b, Some("_coderhelm-okta.evil.example"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn a_cut_answer_is_no_answer() {
        // One of two TXT values came back and the server set TC: reading it
        // as "the record is gone" would release a domain.
        let mut b = answer(0, &["v=DMARC1; p=reject"]);
        b[2] |= 0x02;
        assert_eq!(parse(7, &b), None);
        assert_eq!(parse_for(7, &b, Some("_dmarc.example.com")), None);
        // And a cut answer with no records at all says nothing either.
        let mut empty = answer(0, &[]);
        empty[2] |= 0x02;
        assert_eq!(parse(7, &empty), None);
        // Not cut: read as before.
        let ok = answer(0, &["v=DMARC1; p=reject"]);
        assert_eq!(parse(7, &ok).unwrap().len(), 1);
    }
}
