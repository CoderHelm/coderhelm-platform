//! The public infrastructure feeds, read once a day for every team
//! (`common::infra_vuln::feeds` has the parsers).
//!
//! It rides on the review-reminder tick: the first tick of a UTC day claims
//! `INFRA_FEEDS#<day>` and reads every feed. Each feed is on its own: one
//! that can't be fetched or no longer parses keeps its last good copy, gets
//! the error on its status row (`INFRA_FEED#<feed>`) and a warning in the
//! log, and the others go on.
//!
//! - ALAS (AL2023, AL2): every RSS item is kept as `INFRA_ADV#<id>`. The
//!   advisory page (the fixed AL2023 release and package versions) is read
//!   for kernel and important or critical advisories not read yet, at most
//!   [`PAGES_PER_DAY`] a day, newest first.
//! - Bottlerocket: the gzipped `updateinfo.xml`, every advisory.
//! - Bulletins: the RSS feed, then each listed bulletin's page, read again
//!   every day while it's in the feed (bulletins get updated).
//! - The Lambda runtime table, and AWS's latest-image parameters (read with
//!   the worker's own role in us-east-1: public parameters, no team's
//!   account).

use std::collections::{BTreeSet, HashMap};
use std::io::Read;

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{Duration, Utc};
use common::infra_vuln::{
    self as iv,
    feeds::{self, Advisory, Bulletin, Distro, FeedStatus, LatestImage},
    store,
};
use serde::Serialize;
use tracing::{info, warn};

use crate::passes::{attr_n, attr_s};
use crate::WorkerState;

/// ALAS advisory pages read per day.
const PAGES_PER_DAY: usize = 60;
/// Bulletin pages read per day (the feed holds about 40).
const BULLETINS_PER_DAY: usize = 60;
/// Advisories older than this aren't kept on a first read.
const BACKFILL_DAYS: i64 = 400;
/// Largest response read (the AL2 feed is about 2 MB).
const MAX_BYTES: usize = 12 * 1024 * 1024;

/// What one day's read did.
#[derive(Debug, Default)]
pub struct FeedReport {
    pub ran: bool,
    pub advisories: usize,
    pub pages: usize,
    pub bulletins: usize,
    pub failed: Vec<String>,
}

fn ttl() -> i64 {
    (Utc::now() + Duration::days(iv::KEEP_DAYS)).timestamp()
}

async fn fetch(state: &WorkerState, url: &str) -> Result<Vec<u8>, String> {
    let r = state
        .http
        .get(url)
        .header(
            "user-agent",
            "CoderHelm security feeds",
        )
        .timeout(std::time::Duration::from_secs(40))
        .send()
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    if !r.status().is_success() {
        return Err(format!("{url}: HTTP {}", r.status().as_u16()));
    }
    let b = r.bytes().await.map_err(|e| format!("{url}: {e}"))?;
    if b.len() > MAX_BYTES {
        return Err(format!("{url}: {} bytes, more than expected", b.len()));
    }
    Ok(b.to_vec())
}

async fn fetch_text(state: &WorkerState, url: &str) -> Result<String, String> {
    let b = fetch(state, url).await?;
    String::from_utf8(b).map_err(|_| format!("{url}: not text"))
}

/// Gunzips Bottlerocket's feed, bounded.
pub fn gunzip(b: &[u8]) -> Result<String, String> {
    let mut out = String::new();
    flate2::read::GzDecoder::new(b)
        .take(MAX_BYTES as u64 * 4)
        .read_to_string(&mut out)
        .map_err(|e| format!("not gzip text: {e}"))?;
    Ok(out)
}

async fn put_json<T: Serialize>(
    state: &WorkerState,
    sk: &str,
    v: &T,
    extra: &[(&str, AttributeValue)],
) -> bool {
    let Ok(text) = serde_json::to_string(v) else {
        return false;
    };
    let mut req = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(iv::GLOBAL_PK))
        .item("sk", attr_s(sk))
        .item("json", attr_s(&text))
        .item("updated_at", attr_s(&Utc::now().to_rfc3339()))
        .item("ttl", attr_n(ttl()));
    for (k, v) in extra {
        req = req.item(*k, v.clone());
    }
    match req.send().await {
        Ok(_) => true,
        Err(e) => {
            warn!(sk, error = %e, "infra feeds: a record could not be saved");
            false
        }
    }
}

async fn status(state: &WorkerState, feed: &str) -> FeedStatus {
    store::status(&state.dynamo, &state.config.settings_table_name, feed).await
}

async fn record(
    state: &WorkerState,
    feed: &str,
    outcome: Result<usize, String>,
    report: &mut FeedReport,
) {
    let now = Utc::now().to_rfc3339();
    let st = status(state, feed).await;
    let st = match outcome {
        Ok(n) => st.ok(n, &now),
        Err(e) => {
            // The last good copy stays: only the status row changes.
            warn!(feed, error = %e, "infra feeds: read failed; keeping the last good copy");
            report.failed.push(feed.to_string());
            st.failed(&e, &now)
        }
    };
    put_json(state, &iv::feed_status_sk(feed), &st, &[]).await;
}

async fn advisories(state: &WorkerState) -> Vec<Advisory> {
    store::advisories(&state.dynamo, &state.config.settings_table_name).await
}

async fn bulletins(state: &WorkerState) -> Vec<Bulletin> {
    store::bulletins(&state.dynamo, &state.config.settings_table_name).await
}

/// What to save from a fresh read: new advisories, and known ones whose
/// row changed (the RSS row only; a page already read stays read).
pub fn merge_advisories(
    read: Vec<Advisory>,
    known: &HashMap<String, Advisory>,
    oldest: &str,
) -> Vec<Advisory> {
    read.into_iter()
        .filter(|a| a.released.as_str() >= oldest || known.contains_key(&a.id))
        .filter_map(|mut a| match known.get(&a.id) {
            Some(k) => {
                if k.detailed {
                    a.releasever = k.releasever.clone();
                    a.fixed = k.fixed.clone();
                    a.detailed = true;
                    for c in &k.cves {
                        if !a.cves.contains(c) {
                            a.cves.push(c.clone());
                        }
                    }
                }
                (a != *k).then_some(a)
            }
            None => Some(a),
        })
        .collect()
}

/// The advisory pages to read now: wanted, newest first, those a bulletin
/// names before the rest.
pub fn pages_to_read<'a>(
    all: &'a [Advisory],
    named: &BTreeSet<String>,
    limit: usize,
) -> Vec<&'a Advisory> {
    let mut want: Vec<&Advisory> = all
        .iter()
        .filter(|a| a.wants_detail() && !a.link.is_empty())
        .collect();
    want.sort_by(|a, b| {
        named
            .contains(&b.id)
            .cmp(&named.contains(&a.id))
            .then_with(|| b.released.cmp(&a.released))
    });
    want.truncate(limit);
    want
}

async fn alas(
    state: &WorkerState,
    distro: Distro,
    url: &str,
    named: &BTreeSet<String>,
    report: &mut FeedReport,
) {
    let outcome = async {
        let xml = fetch_text(state, url).await?;
        let read = feeds::parse_alas_rss(&xml, distro).map_err(|e| e.to_string())?;
        let n = read.len();
        let known: HashMap<String, Advisory> = advisories(state)
            .await
            .into_iter()
            .filter(|a| a.distro == distro)
            .map(|a| (a.id.clone(), a))
            .collect();
        let oldest = (Utc::now() - Duration::days(BACKFILL_DAYS)).format("%Y-%m-%d").to_string();
        let changed = merge_advisories(read, &known, &oldest);
        for a in &changed {
            if put_json(state, &iv::advisory_sk(&a.id), a, &[]).await {
                report.advisories += 1;
            }
        }
        // The pages: what the RSS can't say.
        let mut all: Vec<Advisory> = known.into_values().collect();
        for a in changed {
            match all.iter_mut().find(|x| x.id == a.id) {
                Some(x) => *x = a,
                None => all.push(a),
            }
        }
        let todo: Vec<Advisory> = pages_to_read(&all, named, PAGES_PER_DAY).into_iter().cloned().collect();
        for mut a in todo {
            match fetch_text(state, &a.link).await.and_then(|h| feeds::parse_alas_page(&h).map_err(|e| e.to_string())) {
                Ok(p) if p.id == a.id => {
                    a.apply_page(&p);
                    if put_json(state, &iv::advisory_sk(&a.id), &a, &[]).await {
                        report.pages += 1;
                    }
                }
                Ok(p) => warn!(advisory = %a.id, page = %p.id, "infra feeds: the advisory page names another advisory"),
                // One page failing doesn't fail the feed: it is tried again tomorrow.
                Err(e) => warn!(advisory = %a.id, error = %e, "infra feeds: advisory page not read"),
            }
        }
        Ok::<usize, String>(n)
    }
    .await;
    record(state, distro.feed(), outcome, report).await;
}

async fn bottlerocket(state: &WorkerState, report: &mut FeedReport) {
    let outcome = async {
        let b = fetch(state, feeds::BOTTLEROCKET_UPDATEINFO).await?;
        let xml = gunzip(&b)?;
        let read = feeds::parse_bottlerocket(&xml).map_err(|e| e.to_string())?;
        let n = read.len();
        let known: HashMap<String, Advisory> = advisories(state)
            .await
            .into_iter()
            .filter(|a| a.distro == Distro::Bottlerocket)
            .map(|a| (a.id.clone(), a))
            .collect();
        let oldest = (Utc::now() - Duration::days(BACKFILL_DAYS))
            .format("%Y-%m-%d")
            .to_string();
        for a in merge_advisories(read, &known, &oldest) {
            if put_json(state, &iv::advisory_sk(&a.id), &a, &[]).await {
                report.advisories += 1;
            }
        }
        Ok::<usize, String>(n)
    }
    .await;
    record(state, "bottlerocket", outcome, report).await;
}

/// Reads the bulletins and returns the Amazon Linux advisories they name.
async fn bulletins_feed(state: &WorkerState, report: &mut FeedReport) -> BTreeSet<String> {
    let mut named = BTreeSet::new();
    for b in bulletins(state).await {
        named.extend(b.advisories);
    }
    let outcome = async {
        let xml = fetch_text(state, feeds::BULLETINS_RSS).await?;
        let refs = feeds::parse_bulletin_rss(&xml).map_err(|e| e.to_string())?;
        let mut parsed = 0usize;
        let mut failures = 0usize;
        for r in refs.iter().take(BULLETINS_PER_DAY) {
            match fetch_text(state, &r.url).await.and_then(|h| feeds::parse_bulletin(&h, &r.url).map_err(|e| e.to_string())) {
                Ok(mut b) => {
                    if b.title.is_empty() {
                        b.title = r.title.clone();
                    }
                    if b.published.is_empty() {
                        b.published = r.published.clone();
                    }
                    named.extend(b.advisories.iter().cloned());
                    if put_json(state, &iv::bulletin_sk(&b.id), &b, &[]).await {
                        report.bulletins += 1;
                    }
                    parsed += 1;
                }
                Err(e) => {
                    // The stored copy of this bulletin stays as it was.
                    warn!(bulletin = %r.id, error = %e, "infra feeds: bulletin page not read; keeping the last good copy");
                    failures += 1;
                }
            }
        }
        if parsed == 0 && failures > 0 {
            return Err(format!("none of {failures} bulletin pages could be read (the page format may have changed)"));
        }
        Ok::<usize, String>(refs.len())
    }
    .await;
    record(state, "bulletins", outcome, report).await;
    named
}

async fn lambda_runtimes(state: &WorkerState, report: &mut FeedReport) {
    let outcome = async {
        let html = fetch_text(state, feeds::LAMBDA_RUNTIMES).await?;
        let r = feeds::parse_lambda_runtimes(&html).map_err(|e| e.to_string())?;
        put_json(state, iv::RUNTIMES_SK, &r, &[]).await;
        Ok::<usize, String>(r.len())
    }
    .await;
    record(state, "lambda_runtimes", outcome, report).await;
}

async fn latest(state: &WorkerState, report: &mut FeedReport) {
    let outcome = async {
        let conf = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .load()
            .await;
        let ssm = common::public_urls::ssm::Client::new(&conf);
        let mut out: Vec<LatestImage> = Vec::new();
        for chunk in feeds::LATEST_PARAMETERS.chunks(10) {
            let r = ssm
                .get_parameters()
                .set_names(Some(chunk.iter().map(|s| s.to_string()).collect()))
                .send()
                .await
                .map_err(|e| {
                    format!(
                        "ssm GetParameters: {}",
                        aws_sdk_dynamodb::error::DisplayErrorContext(&e)
                    )
                })?;
            for p in r.parameters() {
                if let Some(i) =
                    feeds::parse_latest(p.name().unwrap_or(""), p.value().unwrap_or(""))
                {
                    out.push(i);
                }
            }
        }
        if out.is_empty() {
            return Err("no latest-image parameters answered".into());
        }
        put_json(state, iv::LATEST_IMAGES_SK, &out, &[]).await;
        Ok::<usize, String>(out.len())
    }
    .await;
    record(state, "latest_images", outcome, report).await;
}

/// The daily read, on the review-reminder tick. Only the first tick of a
/// UTC day reads; a read that failed outright is tried again next tick.
pub async fn sweep(state: &WorkerState) -> FeedReport {
    let mut report = FeedReport::default();
    let day = Utc::now().format("%Y%m%d").to_string();
    let won = common::claim::claim(
        &state.dynamo,
        &state.config.settings_table_name,
        iv::GLOBAL_PK,
        &format!("{}{day}", iv::SYNC_CLAIM_PREFIX),
        2 * 86_400,
    )
    .await;
    if !matches!(won, common::claim::Claim::Won) {
        return report;
    }
    report.ran = true;
    // Bulletins first: the advisories they name are read before the rest.
    let named = bulletins_feed(state, &mut report).await;
    alas(
        state,
        Distro::Al2023,
        feeds::ALAS2023_RSS,
        &named,
        &mut report,
    )
    .await;
    alas(state, Distro::Al2, feeds::ALAS2_RSS, &named, &mut report).await;
    bottlerocket(state, &mut report).await;
    lambda_runtimes(state, &mut report).await;
    latest(state, &mut report).await;
    if report.failed.len() == feeds::FEEDS.len() {
        // Nothing answered (no network?): try again on the next tick.
        common::claim::release(
            &state.dynamo,
            &state.config.settings_table_name,
            iv::GLOBAL_PK,
            &format!("{}{day}", iv::SYNC_CLAIM_PREFIX),
        )
        .await;
    }
    info!(
        advisories = report.advisories,
        pages = report.pages,
        bulletins = report.bulletins,
        failed = ?report.failed,
        "Infrastructure feeds read"
    );
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adv(id: &str, released: &str) -> Advisory {
        Advisory {
            id: id.into(),
            package: "kernel".into(),
            severity: "important".into(),
            released: released.into(),
            link: format!("https://alas.aws.amazon.com/AL2023/{id}.html"),
            ..Default::default()
        }
    }

    #[test]
    fn a_new_read_keeps_what_the_page_said() {
        let mut detailed = adv("ALAS2023-2026-3365", "2026-10-30");
        detailed.detailed = true;
        detailed.releasever = "2023.12.20261102".into();
        let known = HashMap::from([(detailed.id.clone(), detailed.clone())]);
        // The same RSS row again: nothing to save.
        let again = merge_advisories(
            vec![adv("ALAS2023-2026-3365", "2026-10-30")],
            &known,
            "2026-01-01",
        );
        assert!(again.is_empty(), "{again:?}");
        // A new advisory, and one too old for a first read.
        let read = vec![
            adv("ALAS2023-2026-3400", "2026-11-09"),
            adv("ALAS2023-2023-0001", "2023-03-01"),
        ];
        let new = merge_advisories(read, &known, "2025-10-01");
        assert_eq!(new.len(), 1);
        assert_eq!(new[0].id, "ALAS2023-2026-3400");
        // A changed row keeps the page's release.
        let mut changed = adv("ALAS2023-2026-3365", "2026-10-30");
        changed.cves = vec!["CVE-2026-1".into()];
        let out = merge_advisories(vec![changed], &known, "2026-01-01");
        assert_eq!(out[0].releasever, "2023.12.20261102");
        assert!(out[0].detailed);
    }

    #[test]
    fn pages_named_by_a_bulletin_go_first() {
        let all = vec![
            adv("A-1", "2026-11-09"),
            adv("A-2", "2026-10-01"),
            adv("A-3", "2026-11-01"),
        ];
        let named = BTreeSet::from(["A-2".to_string()]);
        let p: Vec<&str> = pages_to_read(&all, &named, 2)
            .iter()
            .map(|a| a.id.as_str())
            .collect();
        assert_eq!(p, vec!["A-2", "A-1"]);
        let mut low = adv("A-4", "2026-11-10");
        low.package = "vim".into();
        low.severity = "low".into();
        assert!(pages_to_read(&[low], &BTreeSet::new(), 5).is_empty());
    }

    #[test]
    fn the_bottlerocket_feed_unzips() {
        use std::io::Write;
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(b"<updates></updates>").unwrap();
        let gz = e.finish().unwrap();
        assert_eq!(gunzip(&gz).unwrap(), "<updates></updates>");
        assert!(gunzip(b"plain").is_err());
    }
}
