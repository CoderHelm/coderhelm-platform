//! A web ACL's log records read straight from its `aws-waf-logs-*` bucket:
//! no Athena, no Glue table, no query results location.
//!
//! An AWS WAF alarm run once ended asking a person to run an Athena query
//! for "which attribute of the crawler's requests changed", while the log
//! objects that hold the answer sat in a bucket the team's role may read
//! (`s3:ListBucket` and `s3:GetObject` on `aws-waf-logs-*`, the role stack's
//! `WafLogs` group). WAF writes them as gzip files of JSON lines, one file
//! every five minutes under
//! `<prefix>AWSLogs/<account>/WAFLogs/<cloudfront|region>/<web ACL>/YYYY/MM/dd/HH/mm/`.
//! This module lists the hours of a window newest first, reads the objects
//! within a bound (objects, compressed and uncompressed bytes), keeps the
//! records the rule matched (terminating, counted, or inside a rule group)
//! and groups them in the process: by JA4, user agent, accept language,
//! path and whether a cookie was sent, with countries, labels and actions
//! counted, and the matches per hour. Nothing else of a request is kept: no
//! client address, no cookie or other header value, no query string.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read as _};

use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;

use crate::live::aws::{Session, TeamSession};

/// Most objects read in one lookup (four hours of five-minute files).
pub const MAX_OBJECTS: usize = 48;
/// Most compressed bytes downloaded in one lookup.
pub const MAX_COMPRESSED_BYTES: i64 = 64 * 1024 * 1024;
/// Most uncompressed bytes read in one lookup.
pub const MAX_UNCOMPRESSED_BYTES: u64 = 512 * 1024 * 1024;
/// Longest window read in one lookup.
pub const MAX_WINDOW_SECS: i64 = 24 * 3600;
/// Rows shown, most requests first.
const TOP_ROWS: usize = 20;
/// Values shown per counted kind.
const TOP: usize = 8;
/// Longest value kept.
const MAX_VALUE: usize = 200;
/// Keys listed per hour.
const MAX_KEYS_PER_HOUR: i32 = 1000;

fn clip(v: &str) -> String {
    crate::truncate_str(v.trim(), MAX_VALUE).replace("```", "'''")
}

/// Where a web ACL's log objects are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLocation {
    pub bucket: String,
    /// The destination's own prefix (empty, or ending in `/`).
    pub prefix: String,
    pub account: String,
    /// `cloudfront` for a CloudFront web ACL, else its region.
    pub scope: String,
    pub web_acl: String,
}

/// Pure: the bucket and prefix of an S3 logging destination
/// (`arn:aws:s3:::aws-waf-logs-example` or `.../aws-waf-logs-example/some/prefix`).
pub fn destination(arn: &str) -> Option<(String, String)> {
    let rest = arn.strip_prefix("arn:aws:s3:::")?;
    let (bucket, prefix) = match rest.split_once('/') {
        Some((b, p)) => (b, p.trim_matches('/')),
        None => (rest, ""),
    };
    if !is_bucket(bucket) {
        return None;
    }
    let prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    };
    Some((bucket.to_string(), prefix))
}

/// Pure: a bucket WAF may log to (its name must start with `aws-waf-logs-`).
pub fn is_bucket(b: &str) -> bool {
    b.starts_with("aws-waf-logs-")
        && b.len() <= 63
        && b.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
}

impl LogLocation {
    /// Pure: from a web ACL's ARN
    /// (`arn:aws:wafv2:<region>:<account>:global/webacl/<name>/<id>` for
    /// CloudFront, `.../regional/webacl/...` otherwise) and its S3 logging
    /// destination.
    pub fn of(web_acl_arn: &str, destination_arn: &str) -> Option<LogLocation> {
        let (bucket, prefix) = destination(destination_arn)?;
        let parts: Vec<&str> = web_acl_arn.splitn(6, ':').collect();
        if parts.len() != 6 || parts[2] != "wafv2" {
            return None;
        }
        let (region, account, res) = (parts[3], parts[4], parts[5]);
        let mut r = res.split('/');
        let scope = match r.next()? {
            "global" => "cloudfront".to_string(),
            "regional" => region.to_string(),
            _ => return None,
        };
        if r.next()? != "webacl" {
            return None;
        }
        let web_acl = r.next()?.to_string();
        let ok = account.len() == 12 && account.chars().all(|c| c.is_ascii_digit());
        (ok && !web_acl.is_empty()).then(|| LogLocation {
            bucket,
            prefix,
            account: account.to_string(),
            scope,
            web_acl,
        })
    }

    /// Pure: the prefix of one hour's objects.
    pub fn hour_prefix(&self, at: DateTime<Utc>) -> String {
        format!(
            "{}AWSLogs/{}/WAFLogs/{}/{}/{}/",
            self.prefix,
            self.account,
            self.scope,
            self.web_acl,
            at.format("%Y/%m/%d/%H")
        )
    }

    /// Pure: each hour's prefix from `end` back to `start`, newest first.
    pub fn hour_prefixes(&self, start: i64, end: i64) -> Vec<String> {
        let hour = |t: i64| t - t.rem_euclid(3600);
        let mut out = Vec::new();
        let mut t = hour(end);
        while t >= hour(start) && out.len() < 25 {
            if let Some(at) = Utc.timestamp_opt(t, 0).single() {
                out.push(self.hour_prefix(at));
            }
            t -= 3600;
        }
        out
    }
}

/// Pure: the minute an object's key says it starts at
/// (`.../YYYY/MM/dd/HH/mm/<file>`).
pub fn key_minute(key: &str) -> Option<i64> {
    let parts: Vec<&str> = key.rsplit('/').skip(1).take(5).collect();
    if parts.len() != 5 {
        return None;
    }
    let n = |s: &str| s.parse::<u32>().ok();
    let (mi, h, d, mo, y) = (
        n(parts[0])?,
        n(parts[1])?,
        n(parts[2])?,
        n(parts[3])?,
        parts[4].parse::<i32>().ok()?,
    );
    Utc.with_ymd_and_hms(y, mo, d, h, mi, 0)
        .single()
        .map(|t| t.timestamp())
}

/// Pure: whether an object can hold records of `[start, end]` (a file holds
/// the five minutes after its folder's minute, so one that starts up to ten
/// minutes before the window is read too).
pub fn key_in_window(key: &str, start: i64, end: i64) -> bool {
    key_minute(key).is_some_and(|m| m >= start - 600 && m <= end)
}

/// Pure: whether a log record names `rule` as matched: its terminating rule,
/// a counted rule, or either inside a rule group.
pub fn matches_rule(v: &Value, rule: &str) -> bool {
    let id = |x: &Value| x["ruleId"].as_str() == Some(rule);
    if v["terminatingRuleId"].as_str() == Some(rule) {
        return true;
    }
    let list = |x: &Value| x.as_array().is_some_and(|a| a.iter().any(id));
    if list(&v["nonTerminatingMatchingRules"]) {
        return true;
    }
    v["ruleGroupList"].as_array().is_some_and(|groups| {
        groups.iter().any(|g| {
            id(&g["terminatingRule"])
                || list(&g["nonTerminatingMatchingRules"])
                || list(&g["excludedRules"])
        })
    })
}

/// One group of matching requests.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowKey {
    pub ja4: String,
    pub user_agent: String,
    pub accept_language: String,
    pub path: String,
    pub cookie: bool,
}

/// What the records read so far say about the rule's matches.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tally {
    pub records: i64,
    pub matched: i64,
    pub rows: BTreeMap<RowKey, i64>,
    pub countries: BTreeMap<String, i64>,
    pub labels: BTreeMap<String, i64>,
    pub actions: BTreeMap<String, i64>,
    /// Matches per hour (`YYYY-MM-DD HH:00`).
    pub hours: BTreeMap<String, i64>,
    /// JA4 prefixes (the first two of its three parts), which change less
    /// often than the whole fingerprint.
    pub ja4_prefixes: BTreeMap<String, i64>,
}

fn header<'a>(headers: &'a [Value], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|h| {
            h["name"]
                .as_str()
                .is_some_and(|n| n.eq_ignore_ascii_case(name))
        })
        .and_then(|h| h["value"].as_str())
}

/// Pure: a JA4 fingerprint's first two parts (`t13d1516h2_8daaf6152771`).
pub fn ja4_prefix(ja4: &str) -> String {
    let mut p = ja4.splitn(3, '_');
    match (p.next(), p.next()) {
        (Some(a), Some(b)) if !a.is_empty() && !b.is_empty() => format!("{a}_{b}"),
        _ => ja4.to_string(),
    }
}

impl Tally {
    /// Counts one log line: a record in `[start_ms, end_ms]` that names
    /// `rule`. Lines that aren't records are skipped.
    pub fn add(&mut self, line: &str, rule: &str, start_ms: i64, end_ms: i64) {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            return;
        };
        let at = v["timestamp"].as_i64().unwrap_or(0);
        if at < start_ms || at > end_ms {
            return;
        }
        self.records += 1;
        if !matches_rule(&v, rule) {
            return;
        }
        self.matched += 1;
        let req = &v["httpRequest"];
        let empty = Vec::new();
        let headers = req["headers"].as_array().unwrap_or(&empty);
        let ja4 = clip(v["ja4Fingerprint"].as_str().unwrap_or(""));
        let key = RowKey {
            ja4: ja4.clone(),
            user_agent: clip(header(headers, "user-agent").unwrap_or("")),
            accept_language: clip(header(headers, "accept-language").unwrap_or("")),
            path: clip(
                req["uri"]
                    .as_str()
                    .unwrap_or("")
                    .split(['?', '#'])
                    .next()
                    .unwrap_or(""),
            ),
            cookie: header(headers, "cookie").is_some(),
        };
        *self.rows.entry(key).or_default() += 1;
        if !ja4.is_empty() {
            *self.ja4_prefixes.entry(ja4_prefix(&ja4)).or_default() += 1;
        }
        let country = clip(req["country"].as_str().unwrap_or(""));
        *self.countries.entry(country).or_default() += 1;
        if let Some(labels) = v["labels"].as_array() {
            for l in labels.iter().filter_map(|l| l["name"].as_str()) {
                *self.labels.entry(clip(l)).or_default() += 1;
            }
        }
        *self
            .actions
            .entry(clip(v["action"].as_str().unwrap_or("")))
            .or_default() += 1;
        if let Some(t) = Utc.timestamp_millis_opt(at).single() {
            *self
                .hours
                .entry(t.format("%Y-%m-%d %H:00").to_string())
                .or_default() += 1;
        }
    }

    /// Pure: the evidence lines, most requests first.
    pub fn lines(&self, rule: &str) -> Vec<String> {
        let mut out = vec![format!(
            "{} of {} log records named the rule {rule}.",
            self.matched, self.records
        )];
        if self.matched == 0 {
            out.push(format!(
                "  - none (the alarm's Rule dimension is the rule's metric name; when the rule's \
                 own name differs, read the logs for that name: {rule} is what was looked for)"
            ));
            return out;
        }
        let total = self.matched.max(1);
        let pct = |n: i64| (n * 100) / total;
        let mut rows: Vec<(&RowKey, &i64)> = self.rows.iter().collect();
        rows.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        out.push(
            "By JA4, user agent, accept language, path and cookie presence, most requests first:"
                .into(),
        );
        let dash = |s: &str| {
            if s.is_empty() {
                "-".to_string()
            } else {
                s.to_string()
            }
        };
        for (k, n) in rows.iter().take(TOP_ROWS) {
            out.push(format!(
                "  - {n} requests ({}%): JA4 {} · {} · Accept-Language {} · {} · {}",
                pct(**n),
                dash(&k.ja4),
                dash(&k.user_agent),
                dash(&k.accept_language),
                dash(&k.path),
                if k.cookie { "a cookie" } else { "no cookie" }
            ));
        }
        if rows.len() > TOP_ROWS {
            out.push(format!("  - and {} more groups", rows.len() - TOP_ROWS));
        }
        let mut counted = |label: &str, m: &BTreeMap<String, i64>| {
            let mut v: Vec<(&String, &i64)> = m.iter().collect();
            v.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
            if v.is_empty() {
                return;
            }
            out.push(format!(
                "{label}: {}",
                v.iter()
                    .take(TOP)
                    .map(|(k, n)| format!("{} {n} ({}%)", dash(k), pct(**n)))
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
        };
        counted("JA4 prefixes", &self.ja4_prefixes);
        counted("Countries", &self.countries);
        counted("Labels", &self.labels);
        counted("Actions", &self.actions);
        if !self.hours.is_empty() {
            out.push(format!(
                "Matches per hour (UTC): {}",
                self.hours
                    .iter()
                    .map(|(h, n)| format!("{h} {n}"))
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
        }
        out
    }
}

/// What a read of the log objects found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogRead {
    pub lines: Vec<String>,
    /// IAM actions AWS refused the role (the install's setup item).
    pub refused: Vec<String>,
    pub tally: Tally,
    pub objects_read: usize,
}

impl LogRead {
    /// Records came back that name the rule.
    pub fn found(&self) -> bool {
        self.tally.matched > 0
    }
}

/// Pure: whether an S3 error code means the role may not make the call.
pub fn is_denied(code: Option<&str>) -> bool {
    crate::log_repo::live::is_refusal(code) || code == Some("AllAccessDisabled")
}

/// Pure: the lines of a gzip object, uncompressed up to `left` bytes, into
/// the tally. Returns the bytes read.
pub fn tally_object(
    gz: &[u8],
    tally: &mut Tally,
    rule: &str,
    start_ms: i64,
    end_ms: i64,
    left: u64,
) -> u64 {
    let decoder = flate2::read::MultiGzDecoder::new(gz).take(left);
    let mut reader = BufReader::new(decoder);
    let mut read = 0u64;
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                read += n as u64;
                tally.add(&line, rule, start_ms, end_ms);
            }
        }
    }
    read
}

fn s3_client(s: &Session, region: &str) -> aws_sdk_s3::Client {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    let creds = Credentials::new(
        &s.access_key_id,
        &s.secret_access_key,
        Some(s.session_token.clone()),
        None,
        "coderhelm-team-role",
    );
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .credentials_provider(creds)
            .build(),
    )
}

/// Reads the rule's matches from the log objects in `[start, end]` (at
/// most [`MAX_WINDOW_SECS`], newest first, within the bounds above).
pub async fn read(
    session: &TeamSession,
    loc: &LogLocation,
    rule: &str,
    start: i64,
    end: i64,
) -> LogRead {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    let start = start.max(end - MAX_WINDOW_SECS);
    let mut out = LogRead::default();
    let mut region = session.region.clone();
    let mut s3 = s3_client(session, &region);
    let mut keys: Vec<(String, i64)> = Vec::new();
    for prefix in loc.hour_prefixes(start, end) {
        let mut token: Option<String> = None;
        loop {
            let mut res = s3
                .list_objects_v2()
                .bucket(&loc.bucket)
                .prefix(&prefix)
                .max_keys(MAX_KEYS_PER_HOUR)
                .set_continuation_token(token.clone())
                .send()
                .await;
            if let Err(e) = &res {
                // A bucket in another region: S3 names it; ask there.
                let moved = crate::live::aws_settings::bucket_region(e).filter(|r| *r != region);
                if let Some(r) = moved {
                    region = r;
                    s3 = s3_client(session, &region);
                    res = s3
                        .list_objects_v2()
                        .bucket(&loc.bucket)
                        .prefix(&prefix)
                        .max_keys(MAX_KEYS_PER_HOUR)
                        .set_continuation_token(token.clone())
                        .send()
                        .await;
                }
            }
            match res {
                Ok(o) => {
                    keys.extend(o.contents().iter().filter_map(|c| {
                        let k = c.key()?.to_string();
                        key_in_window(&k, start, end).then(|| (k, c.size().unwrap_or(0)))
                    }));
                    token = o
                        .next_continuation_token()
                        .filter(|t| !t.is_empty() && o.is_truncated() == Some(true))
                        .map(str::to_string);
                    if token.is_none() {
                        break;
                    }
                }
                Err(e) if is_denied(e.code()) => {
                    out.refused.push("s3:ListBucket".into());
                    out.lines.push(format!(
                        "Couldn't list the web ACL's log objects in {}: CoderHelm's AWS role \
                         doesn't allow s3:ListBucket there yet.",
                        loc.bucket
                    ));
                    return out;
                }
                Err(e) => {
                    out.lines.push(format!(
                        "Couldn't list the web ACL's log objects in {}: {}",
                        loc.bucket,
                        clip(e.message().or(e.code()).unwrap_or("no answer"))
                    ));
                    return out;
                }
            }
        }
        if keys.len() >= MAX_OBJECTS * 4 {
            break;
        }
    }
    if keys.is_empty() {
        out.lines.push(format!(
            "No log objects for the web ACL {} in {} between {} and {} UTC (under {}).",
            loc.web_acl,
            loc.bucket,
            fmt(start),
            fmt(end),
            loc.hour_prefixes(start, end)
                .last()
                .cloned()
                .unwrap_or_default()
        ));
        return out;
    }
    // Newest first, so a bound leaves out the oldest records.
    keys.sort_by(|a, b| b.0.cmp(&a.0));
    let listed = keys.len();
    let (start_ms, end_ms) = (start * 1000, end * 1000);
    let mut compressed = 0i64;
    let mut uncompressed = 0u64;
    let mut oldest_read: Option<i64> = None;
    for (key, size) in keys.iter().take(MAX_OBJECTS) {
        if compressed + size > MAX_COMPRESSED_BYTES && out.objects_read > 0 {
            break;
        }
        let got = s3.get_object().bucket(&loc.bucket).key(key).send().await;
        let body = match got {
            Ok(o) => match o.body.collect().await {
                Ok(b) => b.into_bytes(),
                Err(_) => continue,
            },
            Err(e) if is_denied(e.code()) => {
                out.refused.push("s3:GetObject".into());
                out.lines.push(format!(
                    "Couldn't read the web ACL's log objects in {}: CoderHelm's AWS role doesn't \
                     allow s3:GetObject (or decrypting with the bucket's KMS key) there yet.",
                    loc.bucket
                ));
                break;
            }
            Err(_) => continue,
        };
        compressed += body.len() as i64;
        let left = MAX_UNCOMPRESSED_BYTES.saturating_sub(uncompressed);
        uncompressed += tally_object(&body, &mut out.tally, rule, start_ms, end_ms, left);
        out.objects_read += 1;
        oldest_read = key_minute(key);
        if uncompressed >= MAX_UNCOMPRESSED_BYTES {
            break;
        }
    }
    if out.objects_read == 0 {
        return out;
    }
    let mut head = format!(
        "The web ACL's log objects in {} ({} of {} objects between {} and {} UTC, newest first",
        loc.bucket,
        out.objects_read,
        listed,
        fmt(start),
        fmt(end)
    );
    if out.objects_read < listed {
        head.push_str(&format!(
            "; the bound stopped at the objects from {} UTC",
            oldest_read.map(fmt).unwrap_or_default()
        ));
    }
    head.push_str("):");
    out.lines.push(head);
    out.lines.extend(out.tally.lines(rule));
    out
}

fn fmt(t: i64) -> String {
    Utc.timestamp_opt(t, 0)
        .single()
        .map(|d| d.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::io::Write as _;

    pub const ACL_ARN: &str =
        "arn:aws:wafv2:us-east-1:111122223333:global/webacl/example-web-acl/0123abcd";

    /// One WAF log record, in the shape WAF writes it.
    pub fn record(
        at_ms: i64,
        terminating: &str,
        counted: &[&str],
        ja4: &str,
        ua: &str,
        lang: &str,
        cookie: bool,
    ) -> String {
        let mut headers = vec![
            serde_json::json!({"name": "Host", "value": "www.example.com"}),
            serde_json::json!({"name": "User-Agent", "value": ua}),
            serde_json::json!({"name": "Accept-Language", "value": lang}),
        ];
        if cookie {
            headers.push(serde_json::json!({"name": "Cookie", "value": "session=secret-value"}));
        }
        serde_json::json!({
            "timestamp": at_ms,
            "terminatingRuleId": terminating,
            "action": if terminating == "Default_Action" { "ALLOW" } else { "BLOCK" },
            "nonTerminatingMatchingRules": counted.iter().map(|r| serde_json::json!({"ruleId": r, "action": "COUNT"})).collect::<Vec<_>>(),
            "ruleGroupList": [],
            "httpRequest": {
                "clientIp": "198.51.100.7",
                "country": "US",
                "headers": headers,
                "uri": "/listings",
                "args": "slip=a&slip=b&token=secret",
                "httpMethod": "GET"
            },
            "labels": [{"name": "awswaf:clientip:geo:country:US"}],
            "ja3Fingerprint": "773906b0efdefa24a7f2b8eb6985bf37",
            "ja4Fingerprint": ja4
        })
        .to_string()
    }

    pub fn gz(lines: &[String]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        for l in lines {
            e.write_all(l.as_bytes()).unwrap();
            e.write_all(b"\n").unwrap();
        }
        e.finish().unwrap()
    }

    #[test]
    fn a_web_acls_log_objects_are_found_by_its_arn_and_destination() {
        let loc = LogLocation::of(ACL_ARN, "arn:aws:s3:::aws-waf-logs-example").unwrap();
        assert_eq!(loc.scope, "cloudfront");
        assert_eq!(loc.prefix, "");
        // 2026-11-18 12:59 UTC back to 10:00.
        let end = Utc
            .with_ymd_and_hms(2026, 11, 18, 12, 59, 0)
            .unwrap()
            .timestamp();
        let p = loc.hour_prefixes(end - 3 * 3600 + 60, end);
        assert_eq!(
            p,
            vec![
                "AWSLogs/111122223333/WAFLogs/cloudfront/example-web-acl/2026/11/18/12/",
                "AWSLogs/111122223333/WAFLogs/cloudfront/example-web-acl/2026/11/18/11/",
                "AWSLogs/111122223333/WAFLogs/cloudfront/example-web-acl/2026/11/18/10/",
            ]
        );
        let regional = LogLocation::of(
            "arn:aws:wafv2:eu-west-1:111122223333:regional/webacl/api-acl/9",
            "arn:aws:s3:::aws-waf-logs-example/edge",
        )
        .unwrap();
        assert_eq!(regional.scope, "eu-west-1");
        assert!(regional
            .hour_prefix(Utc.with_ymd_and_hms(2026, 1, 2, 3, 0, 0).unwrap())
            .starts_with("edge/AWSLogs/111122223333/WAFLogs/eu-west-1/api-acl/2026/01/02/03/"));
        // Only WAF's buckets.
        assert_eq!(
            LogLocation::of(ACL_ARN, "arn:aws:s3:::another-bucket"),
            None
        );
        assert_eq!(
            LogLocation::of(
                ACL_ARN,
                "arn:aws:logs:us-east-1:111122223333:log-group:aws-waf-logs-x"
            ),
            None
        );
        let key = "AWSLogs/111122223333/WAFLogs/cloudfront/example-web-acl/2026/11/18/12/55/111122223333_waflogs_cloudfront_example-web-acl_20261118T1255Z_abc.log.gz";
        assert_eq!(
            key_minute(key),
            Some(
                Utc.with_ymd_and_hms(2026, 11, 18, 12, 55, 0)
                    .unwrap()
                    .timestamp()
            )
        );
        assert!(key_in_window(key, end - 3600, end));
        assert!(!key_in_window(key, end - 7200, end - 3600));
    }

    #[test]
    fn the_rules_matches_are_grouped_by_what_tells_a_client_apart() {
        let t0 = 1_795_000_000_000;
        let new_ja4 = "t13d1517h2_8daaf6152771_b6f405a00624";
        let old_ja4 = "t13d1516h2_8daaf6152771_02713d6af862";
        let ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
        let mut lines = Vec::new();
        for i in 0..6 {
            lines.push(record(
                t0 + i,
                "Default_Action",
                &["ExampleCrawlerCount"],
                new_ja4,
                ua,
                "en-US,en;q=0.9",
                false,
            ));
        }
        for i in 0..2 {
            lines.push(record(
                t0 + i,
                "ExampleCrawlerBlock",
                &[],
                old_ja4,
                ua,
                "en-US,en;q=0.9",
                false,
            ));
        }
        lines.push(record(
            t0,
            "Default_Action",
            &["ExampleCrawlerCount"],
            new_ja4,
            ua,
            "en-US,en;q=0.9",
            true,
        ));
        lines.push(record(
            t0,
            "Default_Action",
            &[],
            new_ja4,
            ua,
            "de-DE",
            false,
        ));
        lines.push("not a record".into());
        let body = gz(&lines);
        let mut t = Tally::default();
        let read = tally_object(
            &body,
            &mut t,
            "ExampleCrawlerCount",
            t0 - 1,
            t0 + 1000,
            u64::MAX,
        );
        assert!(read > 0);
        assert_eq!(t.records, 10);
        assert_eq!(t.matched, 7);
        let text = t.lines("ExampleCrawlerCount").join("\n");
        assert!(
            text.contains("7 of 10 log records named the rule ExampleCrawlerCount"),
            "{text}"
        );
        assert!(
            text.contains(&format!("6 requests (85%): JA4 {new_ja4} · {ua} · Accept-Language en-US,en;q=0.9 · /listings · no cookie")),
            "{text}"
        );
        assert!(
            text.contains("1 requests (14%)") && text.contains("a cookie"),
            "{text}"
        );
        assert!(
            text.contains("JA4 prefixes: t13d1517h2_8daaf6152771 7 (100%)"),
            "{text}"
        );
        // Nothing private is kept.
        for never in ["198.51.100.7", "secret", "token=", "www.example.com"] {
            assert!(!text.contains(never), "{never}: {text}");
        }
        // The block rule's own matches are its rule's.
        let mut b = Tally::default();
        tally_object(
            &body,
            &mut b,
            "ExampleCrawlerBlock",
            t0 - 1,
            t0 + 1000,
            u64::MAX,
        );
        assert_eq!(b.matched, 2);
        // Out of the window, or the bound reached: nothing more is counted.
        let mut late = Tally::default();
        tally_object(
            &body,
            &mut late,
            "ExampleCrawlerCount",
            t0 + 10_000,
            t0 + 20_000,
            u64::MAX,
        );
        assert_eq!((late.records, late.matched), (0, 0));
        let mut capped = Tally::default();
        tally_object(
            &body,
            &mut capped,
            "ExampleCrawlerCount",
            t0 - 1,
            t0 + 1000,
            10,
        );
        assert_eq!(capped.records, 0);
    }

    #[test]
    fn a_rule_inside_a_rule_group_counts() {
        let v = serde_json::json!({
            "terminatingRuleId": "Default_Action",
            "ruleGroupList": [{
                "ruleGroupId": "arn:aws:wafv2:us-east-1:111122223333:global/rulegroup/g/1",
                "terminatingRule": null,
                "nonTerminatingMatchingRules": [{"ruleId": "InGroup", "action": "COUNT"}],
                "excludedRules": null
            }]
        });
        assert!(matches_rule(&v, "InGroup"));
        assert!(!matches_rule(&v, "Other"));
        assert_eq!(
            ja4_prefix("t13d1516h2_8daaf6152771_e5627efa2ab1"),
            "t13d1516h2_8daaf6152771"
        );
        assert_eq!(ja4_prefix("odd"), "odd");
        let none = Tally::default().lines("ExampleCrawlerCount").join("\n");
        assert!(
            none.contains("0 of 0") && none.contains("metric name"),
            "{none}"
        );
    }
}
