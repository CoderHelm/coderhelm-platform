//! Hosted side of hybrid: every 15 minutes, check that each
//! `{name}.coderhelm.com` still points only where CoderHelm put it and still
//! reaches its own install, and park it when it doesn't
//! (common::hybrid::name_check decides; this reads and writes).
//!
//! Per claimed name: the zone's records are compared with what CoderHelm
//! stored, then the install proves itself twice with a fresh challenge (its
//! recorded CloudFront address, and the name). Parking swaps the address back
//! to the hosted holding page, records why on the reservation (the install
//! reads it from /hybrid/status, and so does hosted admin) and writes an
//! audit entry. The reservation is never deleted. A parked name whose
//! install proves itself again at its recorded CloudFront address gets its
//! address record back by itself, and the reservation keeps when it
//! recovered (`recovered_at`) as history. Records left under no
//! claimed name in a shape CoderHelm writes are removed; anything else
//! unexpected is logged for CoderHelm's operators.

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_route53::types::{
    AliasTarget, Change, ChangeAction, ChangeBatch, ResourceRecord, ResourceRecordSet, RrType,
};
use common::hybrid::name_check::{self, History, Proof, ZoneRecord};
use common::hybrid::names::{self, NameRecord};
use common::hybrid::{broker, relay, sign};
use serde_json::{json, Value};
use std::collections::HashSet;
use tracing::{info, warn};

use crate::WorkerState;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// CloudFront's own hosted zone id, the one every CloudFront alias uses.
const CLOUDFRONT_ALIAS_ZONE: &str = "Z2FDTNDATAQYW2";

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn env(k: &str) -> Option<String> {
    std::env::var(k)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// One claimed name as its reservation holds it.
struct Claim {
    name: String,
    install_id: String,
    records: Vec<NameRecord>,
    pending: bool,
    history: History,
    /// Why it was parked, while it is (kept as history once it recovers).
    parked_code: String,
    parked_at: String,
}

fn claim_from(i: &std::collections::HashMap<String, AttributeValue>) -> Option<Claim> {
    let g = |k: &str| {
        i.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let n = |k: &str| {
        i.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0)
    };
    let b = |k: &str| {
        i.get(k)
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false)
    };
    let name = g("pk").strip_prefix("HYBRIDNAME#")?.to_string();
    names::validate_name(&name).ok()?;
    Some(Claim {
        install_id: g("install_id"),
        records: serde_json::from_str(&g("records")).unwrap_or_default(),
        pending: b("pending"),
        history: History {
            failures: n("check_failures"),
            proven_before: b("check_proven"),
            unreachable: n("check_unreachable"),
            parked: b("parked"),
        },
        parked_code: g("parked_code"),
        parked_at: g("parked_at"),
        name,
    })
}

/// The reservation's name, for the index rebuild's full scan.
fn reserved_name(i: &std::collections::HashMap<String, AttributeValue>) -> Option<String> {
    let sk = i.get("sk").and_then(|v| v.as_s().ok())?;
    let pk = i.get("pk").and_then(|v| v.as_s().ok())?;
    (sk == "NAME").then(|| pk.strip_prefix("HYBRIDNAME#").map(String::from))?
}

fn name_index() -> common::settings_index::Spec<'static> {
    common::settings_index::Spec {
        name: name_check::NAME_INDEX,
        filter: "begins_with(pk, :p) AND sk = :s",
        values: vec![(":p", s("HYBRIDNAME#")), (":s", s("NAME"))],
        member_of: reserved_name,
    }
}

/// Every name reservation (`HYBRIDNAME#<name>` / `NAME`), read by key from
/// the names index. This was a scan of the whole settings table every 15
/// minutes before.
async fn claims(state: &WorkerState) -> Result<Vec<Claim>, Error> {
    let table = &state.config.settings_table_name;
    let names = common::settings_index::members(&state.dynamo, table, &name_index()).await?;
    let mut out = vec![];
    for name in names {
        let r = state
            .dynamo
            .get_item()
            .table_name(table)
            .key("pk", s(&format!("HYBRIDNAME#{name}")))
            .key("sk", s("NAME"))
            .send()
            .await?;
        match r.item() {
            None => {
                common::settings_index::remove(&state.dynamo, table, name_check::NAME_INDEX, &name)
                    .await
            }
            Some(i) => out.extend(claim_from(i)),
        }
    }
    Ok(out)
}

/// The install's recorded CloudFront address, when it is active, verified,
/// still holds this name and its address isn't revoked.
async fn install_cloudfront(
    state: &WorkerState,
    install_id: &str,
    name: &str,
) -> Result<Option<String>, Error> {
    let table = &state.config.settings_table_name;
    let r = state
        .dynamo
        .get_item()
        .table_name(table)
        .key("pk", s(&relay::install_pk(install_id)))
        .key("sk", s("INSTALL"))
        .send()
        .await?;
    let Some(i) = r.item() else { return Ok(None) };
    let g = |k: &str| {
        i.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let cf = g("cloudfront");
    if g("status") != "active"
        || g("verified_at").is_empty()
        || g("name") != name
        || !names::is_cloudfront_domain(&cf)
    {
        return Ok(None);
    }
    let revoked = state
        .dynamo
        .get_item()
        .table_name(table)
        .key(
            "pk",
            s(&format!(
                "HYBRIDREVOKED#{}",
                cf.trim_end_matches('.').to_ascii_lowercase()
            )),
        )
        .key("sk", s("REVOKED"))
        .send()
        .await?
        .item()
        .is_some();
    Ok((!revoked).then_some(cf))
}

/// Every record in the zone, as listed (kept for exact deletes) and reduced.
async fn zone_records(
    state: &WorkerState,
    zone_id: &str,
) -> Result<(Vec<ResourceRecordSet>, Vec<ZoneRecord>), Error> {
    let mut raw = vec![];
    let (mut next_name, mut next_type) = (None, None);
    loop {
        let r = state
            .route53
            .list_resource_record_sets()
            .hosted_zone_id(zone_id)
            .set_start_record_name(next_name)
            .set_start_record_type(next_type)
            .send()
            .await?;
        raw.extend(r.resource_record_sets().iter().cloned());
        if !r.is_truncated() {
            break;
        }
        next_name = r.next_record_name().map(str::to_string);
        next_type = r.next_record_type().cloned();
    }
    let reduced = raw
        .iter()
        .map(|r| {
            ZoneRecord::new(
                r.name(),
                r.r#type().as_str(),
                r.resource_records().first().map(|v| v.value()),
                r.alias_target().map(|a| a.dns_name()),
            )
        })
        .collect();
    Ok((raw, reduced))
}

/// Asks `host` for an ownership proof with a fresh challenge (so an old
/// answer can't be replayed), never following a redirect.
async fn prove(master: &str, install_id: &str, host: &str) -> Proof {
    let challenge = broker::new_handle("ch");
    let url = format!("https://{host}/webhooks/relay/proof?challenge={challenge}");
    let Ok(r) = relay::install_client()
        .get(&url)
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await
    else {
        return Proof::Unreachable;
    };
    // A server error is the install not answering right now (its gateway
    // restarting or failing after an update), not another app answering.
    if let Some(p) = name_check::proof_for_status(r.status().as_u16()) {
        return p;
    }
    let v: Value = serde_json::from_slice(&relay::read_capped(r).await).unwrap_or_default();
    answer(master, install_id, &challenge, &v)
}

/// Whether an answer is this install's right proof for this challenge.
pub fn answer(master: &str, install_id: &str, challenge: &str, v: &Value) -> Proof {
    let right = v["install_id"].as_str() == Some(install_id)
        && sign::check_proof(
            &sign::install_secret(master, install_id),
            challenge,
            v["proof"].as_str().unwrap_or(""),
        );
    if right {
        Proof::Proven
    } else {
        Proof::Wrong
    }
}

fn fallback_alias(host: &str, fallback: &str, rtype: RrType) -> Result<ResourceRecordSet, Error> {
    Ok(ResourceRecordSet::builder()
        .name(host)
        .r#type(rtype)
        .alias_target(
            AliasTarget::builder()
                .hosted_zone_id(CLOUDFRONT_ALIAS_ZONE)
                .dns_name(fallback)
                .evaluate_target_health(false)
                .build()?,
        )
        .build()?)
}

fn address_record(r: &NameRecord) -> Result<ResourceRecordSet, Error> {
    Ok(ResourceRecordSet::builder()
        .name(&r.name)
        .r#type(RrType::Cname)
        .ttl(300)
        .resource_records(ResourceRecord::builder().value(&r.value).build()?)
        .build()?)
}

/// Deletes listed records (exactly as listed) and adds the fallback alias or
/// the address record, in one atomic change. Refuses anything outside `name`.
async fn change_zone(
    state: &WorkerState,
    zone_id: &str,
    zone: &str,
    name: &str,
    delete: &[&ResourceRecordSet],
    add_fallback: Option<&str>,
    add_address: Option<&NameRecord>,
) -> Result<(), Error> {
    let mut changes = vec![];
    for r in delete {
        if name_check::owner(r.name(), zone).as_deref() != Some(name) {
            return Err(format!("refused a change outside {name}: {}", r.name()).into());
        }
        changes.push(
            Change::builder()
                .action(ChangeAction::Delete)
                .resource_record_set((*r).clone())
                .build()?,
        );
    }
    if let Some(fallback) = add_fallback {
        let host = names::hostname(name);
        for t in [RrType::A, RrType::Aaaa] {
            changes.push(
                Change::builder()
                    .action(ChangeAction::Upsert)
                    .resource_record_set(fallback_alias(&host, fallback, t)?)
                    .build()?,
            );
        }
    }
    if let Some(a) = add_address {
        if name_check::owner(&a.name, zone).as_deref() != Some(name) {
            return Err(format!("refused a change outside {name}: {}", a.name).into());
        }
        // Create, never overwrite: the zone holds nothing else at the name.
        changes.push(
            Change::builder()
                .action(ChangeAction::Create)
                .resource_record_set(address_record(a)?)
                .build()?,
        );
    }
    if changes.is_empty() {
        return Ok(());
    }
    state
        .route53
        .change_resource_record_sets()
        .hosted_zone_id(zone_id)
        .change_batch(ChangeBatch::builder().set_changes(Some(changes)).build()?)
        .send()
        .await?;
    Ok(())
}

async fn audit(state: &WorkerState, install: &str, action: &str, detail: Value) {
    let now = chrono::Utc::now();
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s("HYBRIDAUDIT"))
        .item("sk", s(&ulid::Ulid::new().to_string()))
        .item("install_id", s(install))
        .item("action", s(action))
        .item("detail", s(&detail.to_string()))
        .item(
            "at",
            s(&now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        )
        .item(
            "ttl",
            AttributeValue::N((now.timestamp() + 400 * 86_400).to_string()),
        )
        .send()
        .await
        .map_err(|e| warn!(error = %e, action, "hybrid: audit write failed"));
}

/// Writes the run's result on the reservation (only while it still belongs
/// to the same install: a name released meanwhile is left alone).
async fn save(
    state: &WorkerState,
    c: &Claim,
    d: &name_check::Decision,
    parked: Option<(&[NameRecord], name_check::ParkReason)>,
) -> Result<(), Error> {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    // A parked name has to prove itself again before a wrong answer counts.
    let proven = parked.is_none() && d.history.proven_before;
    let mut q = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&format!("HYBRIDNAME#{}", c.name)))
        .key("sk", s("NAME"))
        .condition_expression("install_id = :i")
        .expression_attribute_values(":i", s(&c.install_id))
        .expression_attribute_values(":f", AttributeValue::N(d.history.failures.to_string()))
        .expression_attribute_values(":u", AttributeValue::N(d.history.unreachable.to_string()))
        .expression_attribute_values(":pv", AttributeValue::Bool(proven))
        .expression_attribute_values(":st", s(d.state))
        .expression_attribute_values(":at", s(&now));
    let mut set = "check_failures = :f, check_unreachable = :u, check_proven = :pv, check_state = :st, checked_at = :at".to_string();
    if let Some((records, reason)) = parked {
        set.push_str(", records = :r, pending = :t, parked = :t, parked_code = :pc, parked_reason = :pr, parked_at = :at");
        q = q
            .expression_attribute_values(":r", s(&serde_json::to_string(records)?))
            .expression_attribute_values(":t", AttributeValue::Bool(true))
            .expression_attribute_values(":pc", s(reason.code()))
            .expression_attribute_values(":pr", s(reason.message()));
    } else if d.state == "ok" {
        // Working again: the parked note goes.
        set.push_str(" REMOVE parked, parked_code, parked_reason, parked_at");
    }
    q = q.update_expression(format!("SET {set}"));
    send_save(q).await
}

/// Writes a restored name: the address record is back, the parked note goes,
/// and when it recovered (and from what) stays as history.
async fn save_restored(
    state: &WorkerState,
    c: &Claim,
    d: &name_check::Decision,
    records: &[NameRecord],
) -> Result<(), Error> {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let q = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&format!("HYBRIDNAME#{}", c.name)))
        .key("sk", s("NAME"))
        .condition_expression("install_id = :i")
        .expression_attribute_values(":i", s(&c.install_id))
        .expression_attribute_values(":f", AttributeValue::N("0".into()))
        .expression_attribute_values(":u", AttributeValue::N("0".into()))
        .expression_attribute_values(":pv", AttributeValue::Bool(true))
        .expression_attribute_values(":st", s(d.state))
        .expression_attribute_values(":at", s(&now))
        .expression_attribute_values(":r", s(&serde_json::to_string(records)?))
        .expression_attribute_values(":no", AttributeValue::Bool(false))
        .expression_attribute_values(":lp", s(if c.parked_at.is_empty() { &now } else { &c.parked_at }))
        .expression_attribute_values(
            ":rc",
            s(if c.parked_code.is_empty() { "parked" } else { &c.parked_code }),
        )
        .update_expression(
            "SET check_failures = :f, check_unreachable = :u, check_proven = :pv, check_state = :st, checked_at = :at, \
             records = :r, pending = :no, recovered_at = :at, recovered_code = :rc, last_parked_at = :lp \
             REMOVE parked, parked_code, parked_reason, parked_at",
        );
    send_save(q).await
}

async fn send_save(
    q: aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder,
) -> Result<(), Error> {
    match q.send().await {
        Ok(_) => Ok(()),
        Err(e)
            if e.as_service_error()
                .is_some_and(|x| x.is_conditional_check_failed_exception()) =>
        {
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

/// The zone as one run sees it.
struct Zone {
    id: String,
    name: String,
    fallback: String,
    /// As listed (exact deletes need the record as Route 53 has it).
    raw: Vec<ResourceRecordSet>,
    listed: Vec<ZoneRecord>,
}

/// Checks one claimed name. Returns its state for the run's summary.
async fn check_one(
    state: &WorkerState,
    master: &str,
    z: &Zone,
    c: &Claim,
) -> Result<&'static str, Error> {
    let (zone_id, zone, fallback) = (z.id.as_str(), z.name.as_str(), z.fallback.as_str());
    let (raw, listed) = (&z.raw, &z.listed);
    let cloudfront = install_cloudfront(state, &c.install_id, &c.name).await?;
    let records = name_check::sort_records(
        &c.name,
        zone,
        &c.records,
        c.pending,
        cloudfront.as_deref().unwrap_or(""),
        fallback,
        listed,
    );
    for i in &records.untouchable {
        warn!(name = %c.name, record = %listed[*i].name, rtype = %listed[*i].rtype, "hybrid_name_unknown_record: under a claimed name, of a type CoderHelm can't change");
    }
    let (origin, via_name) = match (&cloudfront, records.address) {
        // Parked: only the install's own CloudFront address can prove it's back.
        (Some(cf), _) if c.history.parked => (Some(prove(master, &c.install_id, cf).await), None),
        (Some(cf), Some(_)) => (
            Some(prove(master, &c.install_id, cf).await),
            Some(prove(master, &c.install_id, &names::hostname(&c.name)).await),
        ),
        _ => (None, None),
    };
    let d = name_check::decide(c.history, cloudfront.is_some(), &records, origin, via_name);
    if d.restore {
        return restore(
            state,
            z,
            c,
            &d,
            &records,
            cloudfront.as_deref().unwrap_or(""),
        )
        .await;
    }
    let Some(reason) = d.park else {
        save(state, c, &d, None).await?;
        return Ok(d.state);
    };
    let plan = name_check::plan_park(&c.name, zone, &records, listed);
    let delete: Vec<&ResourceRecordSet> = plan.delete.iter().map(|i| &raw[*i]).collect();
    change_zone(
        state,
        zone_id,
        zone,
        &c.name,
        &delete,
        plan.add_fallback.then_some(fallback),
        None,
    )
    .await?;
    let kept = name_check::stored_after_park(&c.records);
    save(state, c, &d, Some((&kept, reason))).await?;
    let removed: Vec<String> = plan
        .delete
        .iter()
        .map(|i| format!("{} {}", listed[*i].rtype, listed[*i].name))
        .collect();
    warn!(name = %c.name, install = %c.install_id, reason = reason.code(), removed = ?removed, "hybrid_name_parked");
    audit(
        state,
        &c.install_id,
        "name_parked",
        json!({ "name": c.name, "reason": reason.code(), "removed": removed }),
    )
    .await;
    Ok("parked")
}

/// Puts a parked name back on its install: the fallback alias goes and the
/// address record is written again, in one change.
async fn restore(
    state: &WorkerState,
    z: &Zone,
    c: &Claim,
    d: &name_check::Decision,
    records: &name_check::Records,
    cloudfront: &str,
) -> Result<&'static str, Error> {
    let Some(plan) = name_check::plan_restore(&c.name, &z.name, records, &z.listed, cloudfront)
    else {
        return Ok("parked");
    };
    let delete: Vec<&ResourceRecordSet> = plan.delete.iter().map(|i| &z.raw[*i]).collect();
    change_zone(
        state,
        &z.id,
        &z.name,
        &c.name,
        &delete,
        None,
        Some(&plan.address),
    )
    .await?;
    let back = name_check::stored_after_restore(&c.records, &plan.address);
    save_restored(state, c, d, &back).await?;
    info!(name = %c.name, install = %c.install_id, was = %c.parked_code, "hybrid_name_restored");
    audit(
        state,
        &c.install_id,
        "name_restored",
        json!({ "name": c.name, "was": c.parked_code, "address": plan.address.value }),
    )
    .await;
    Ok("restored")
}

pub async fn run(state: &WorkerState) -> Result<(), Error> {
    let Some(master) = state
        .secrets
        .hybrid_master_key
        .as_deref()
        .filter(|k| common::hybrid::is_hosted_side(Some(k)))
    else {
        // Only CoderHelm's own deployment holds names.
        return Ok(());
    };
    let (Some(zone_id), Some(fallback)) = (env("HYBRID_ZONE_ID"), env("HYBRID_FALLBACK_DOMAIN"))
    else {
        return Ok(());
    };
    let all = claims(state).await?;
    let (raw, listed) = zone_records(state, &zone_id).await?;
    let z = Zone {
        id: zone_id,
        name: names::zone(),
        fallback,
        raw,
        listed,
    };
    let (zone_id, zone, fallback, raw, listed) = (&z.id, &z.name, &z.fallback, &z.raw, &z.listed);
    let mut summary: std::collections::BTreeMap<&str, u32> = Default::default();
    for c in &all {
        match check_one(state, master, &z, c).await {
            Ok(st) => *summary.entry(st).or_default() += 1,
            Err(e) => {
                *summary.entry("error").or_default() += 1;
                warn!(name = %c.name, error = %e, "hybrid name check failed for one name");
            }
        }
    }

    // Records under no claimed name.
    let claimed: HashSet<&str> = all.iter().map(|c| c.name.as_str()).collect();
    let is_claimed = |n: &str| claimed.contains(n);
    for i in name_check::orphans(zone, &is_claimed, fallback, listed) {
        let r = &listed[i];
        let Some(label) = name_check::owner(&r.name, zone) else {
            continue;
        };
        match change_zone(state, zone_id, zone, &label, &[&raw[i]], None, None).await {
            Ok(()) => {
                warn!(record = %r.name, rtype = %r.rtype, "hybrid_name_orphan_removed");
                audit(
                    state,
                    "",
                    "orphan_record_removed",
                    json!({ "name": label, "record": r.name, "type": r.rtype }),
                )
                .await;
                *summary.entry("orphan_removed").or_default() += 1;
            }
            Err(e) => warn!(record = %r.name, error = %e, "hybrid: orphan record not removed"),
        }
    }
    for i in name_check::unknown(zone, &is_claimed, fallback, listed) {
        warn!(record = %listed[i].name, rtype = %listed[i].rtype, "hybrid_name_unknown_record: under no claimed name, not a shape CoderHelm writes");
    }
    info!(names = all.len(), summary = ?summary, "hybrid name check done");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MASTER: &str = "hosted-master-key-for-tests";

    fn good(challenge: &str) -> Value {
        json!({
            "install_id": "ins_a",
            "proof": sign::proof(&sign::install_secret(MASTER, "ins_a"), challenge),
        })
    }

    #[test]
    fn a_right_proof_for_this_challenge_is_proven() {
        assert_eq!(
            answer(MASTER, "ins_a", "ch_1", &good("ch_1")),
            Proof::Proven
        );
    }

    #[test]
    fn a_replayed_proof_for_another_challenge_is_wrong() {
        // An answer recorded for an earlier challenge proves nothing now.
        assert_eq!(answer(MASTER, "ins_a", "ch_2", &good("ch_1")), Proof::Wrong);
    }

    #[test]
    fn a_forged_or_borrowed_proof_is_wrong() {
        // Another install's right proof.
        let other = json!({
            "install_id": "ins_b",
            "proof": sign::proof(&sign::install_secret(MASTER, "ins_b"), "ch_1"),
        });
        assert_eq!(answer(MASTER, "ins_a", "ch_1", &other), Proof::Wrong);
        // This install's id with another install's proof.
        let mixed = json!({ "install_id": "ins_a", "proof": other["proof"] });
        assert_eq!(answer(MASTER, "ins_a", "ch_1", &mixed), Proof::Wrong);
        // Signed with a key that isn't the derived one.
        let forged = json!({ "install_id": "ins_a", "proof": sign::proof("guess", "ch_1") });
        assert_eq!(answer(MASTER, "ins_a", "ch_1", &forged), Proof::Wrong);
        assert_eq!(answer(MASTER, "ins_a", "ch_1", &json!({})), Proof::Wrong);
    }

    #[test]
    fn reservations_parse_with_their_history() {
        let mut i = std::collections::HashMap::new();
        i.insert("pk".into(), s("HYBRIDNAME#acme"));
        i.insert("sk".into(), s("NAME"));
        i.insert("install_id".into(), s("ins_a"));
        i.insert("records".into(), s("[]"));
        i.insert("pending".into(), AttributeValue::Bool(true));
        i.insert("check_failures".into(), AttributeValue::N("2".into()));
        i.insert("check_proven".into(), AttributeValue::Bool(true));
        let c = claim_from(&i).unwrap();
        assert_eq!(c.name, "acme");
        assert!(c.pending);
        assert_eq!(c.history.failures, 2);
        assert!(c.history.proven_before);
        assert!(!c.history.parked);
        i.insert("parked".into(), AttributeValue::Bool(true));
        i.insert("parked_code".into(), s("origin_gone"));
        let c = claim_from(&i).unwrap();
        assert!(c.history.parked);
        assert_eq!(c.parked_code, "origin_gone");
        // A key that isn't a valid name is skipped, never acted on.
        i.insert("pk".into(), s("HYBRIDNAME#Not A Name"));
        assert!(claim_from(&i).is_none());
    }
}

#[cfg(test)]
mod name_index_tests {
    use super::*;
    use std::collections::HashMap;

    fn item(pk: &str, sk: &str) -> HashMap<String, AttributeValue> {
        HashMap::from([("pk".to_string(), s(pk)), ("sk".to_string(), s(sk))])
    }

    #[test]
    fn only_name_reservations_become_index_members() {
        assert_eq!(
            reserved_name(&item("HYBRIDNAME#acme", "NAME")),
            Some("acme".into())
        );
        assert_eq!(reserved_name(&item("HYBRIDNAME#acme", "AUDIT#1")), None);
        assert_eq!(reserved_name(&item("TEAM#x", "NAME")), None);
    }

    #[test]
    fn the_rebuild_scan_matches_what_the_old_scan_matched() {
        let spec = name_index();
        assert_eq!(spec.name, name_check::NAME_INDEX);
        assert_eq!(spec.filter, "begins_with(pk, :p) AND sk = :s");
    }
}
