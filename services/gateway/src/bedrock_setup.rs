//! "Set up in AWS": copying a team's knowledge bases into a Bedrock
//! knowledge base with one button, on a self-hosted install.
//!
//! 1. An admin picks the bases, confirms the access warning and presses the
//!    button. The gateway makes a one-time token (`chbs_...`, 32 random
//!    bytes) and keeps only its SHA-256, with what the admin chose
//!    ([`Setup`]). The page opens a CloudFormation quick-create link for the
//!    export stack (infra/cfn/coderhelm-bedrock-export.yaml) with every
//!    parameter filled in: the install's prefix and account, where to report,
//!    and the token (a `NoEcho` parameter).
//! 2. The stack's reporter posts each step to `POST
//!    /api/knowledge/bedrock-setup/report` with the token: `started` (first,
//!    before anything is made), `bucket`, then `ready` with what it made, or
//!    `failed` with the first failure when it rolls back.
//! 3. On `ready` the gateway checks every output against what it expects
//!    (the bucket named for this install in its own account, a knowledge base
//!    in that account and region) and turns copying on with the target it
//!    reported: the admin never pastes anything.
//!
//! Why a report from the stack, not the install reading the stack: the
//! install's own role reads only its own update stacks, and a permission
//! limit change reaches an install only through an AWS admin. A report
//! needs no new permission on any role, and it works the same wherever the
//! install runs.
//!
//! A token works for one setup of one team: it is refused once the setup
//! is `ready` or `failed` (single use), after [`TOKEN_TTL_SECS`], when a
//! newer setup replaced it, from a stack in another account, or from a
//! second stack once the first one reported. A token's holder can at most
//! report a setup of the install's own bucket and a knowledge base in the
//! install's own account.
//!
//! Storage, in the settings table:
//! - `pk = BEDROCK_SETUP#<token sha256>`, `sk = BEDROCK_SETUP`: the [`Setup`];
//! - `pk = <team>`, `sk = BEDROCK_SETUP`: the hash of the team's current setup.

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use common::knowledge::bedrock_sync::{self as bs, Target};

pub const TOKEN_PREFIX: &str = "chbs_";
/// How long a setup link works: time to sign in to AWS, review and create.
pub const TOKEN_TTL_SECS: i64 = 2 * 3600;
pub const SK: &str = "BEDROCK_SETUP";
pub const PK_PREFIX: &str = "BEDROCK_SETUP#";
/// The stack's name in the AWS console.
pub const STACK_SUFFIX: &str = "-bedrock-export";
pub const TEMPLATE_FILE: &str = "coderhelm-bedrock-export.yaml";
pub const REPORT_PATH: &str = "/api/knowledge/bedrock-setup/report";

/// Where a setup is.
pub const WAITING: &str = "waiting";
pub const STARTED: &str = "started";
pub const BUCKET: &str = "bucket";
pub const READY: &str = "ready";
pub const FAILED: &str = "failed";
pub const REPLACED: &str = "replaced";

/// One press of "Set up in AWS".
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Setup {
    pub team_id: String,
    pub token_hash: String,
    pub created_at: String,
    pub created_by: String,
    pub expires_at: i64,
    pub stack_name: String,
    pub region: String,
    /// The bases to copy and the admin's confirmation of the warning.
    pub bases: Vec<String>,
    pub acknowledged_by: String,
    pub creates_knowledge_base: bool,
    /// An existing knowledge base the stack adds the data source to.
    #[serde(default)]
    pub knowledge_base_id: String,
    /// The stack that reported first; no other may report after it.
    #[serde(default)]
    pub stack_id: String,
    pub state: String,
    /// What went wrong, in plain words, when `failed`.
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub updated_at: String,
    /// Each save adds one; a save expects the one it read.
    #[serde(default)]
    pub version: u32,
}

impl Setup {
    pub fn done(&self) -> bool {
        matches!(self.state.as_str(), READY | FAILED | REPLACED)
    }
}

/// What the stack made, as its reporter sends it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Outputs {
    #[serde(default)]
    pub bucket_name: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub knowledge_base_arn: String,
    #[serde(default)]
    pub data_source_id: String,
}

/// A report from the stack.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Report {
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub step: String,
    #[serde(default)]
    pub stack_id: String,
    #[serde(default)]
    pub outputs: Option<Outputs>,
    #[serde(default)]
    pub reason: String,
}

/// Why a report was refused, in words the stack shows in its events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    Unknown,
    Expired,
    Spent,
    Replaced,
    OtherStack,
    WrongAccount(String),
    OutOfOrder,
    BadOutputs(String),
}

impl Refused {
    pub fn message(&self) -> String {
        match self {
            Refused::Unknown => "This setup link isn't one Coderhelm made. Start again from Knowledge, Use in Bedrock.".into(),
            Refused::Expired => "This setup link has expired (they work for two hours). Start again from Knowledge, Use in Bedrock.".into(),
            Refused::Spent => "This setup link was already used. Start again from Knowledge, Use in Bedrock.".into(),
            Refused::Replaced => "A newer setup replaced this one. Use the newest link from Knowledge, Use in Bedrock.".into(),
            Refused::OtherStack => "Another stack already reported with this setup link. Start again from Knowledge, Use in Bedrock.".into(),
            Refused::WrongAccount(a) => format!("Create this stack in AWS account {a}, the install's own."),
            Refused::OutOfOrder => "The stack reported a step out of order. Start again from Knowledge, Use in Bedrock.".into(),
            Refused::BadOutputs(m) => format!("Coderhelm can't use what the stack made: {m}"),
        }
    }
}

/// A new token and its hash.
pub fn generate() -> (String, String) {
    use base64::Engine;
    use rand::RngCore;
    let mut secret = [0u8; 32];
    rand::rng().fill_bytes(&mut secret);
    let token = format!(
        "{TOKEN_PREFIX}{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret)
    );
    let h = hash(&token);
    (token, h)
}

pub fn hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn token_shape_ok(token: &str) -> bool {
    token.strip_prefix(TOKEN_PREFIX).is_some_and(|r| {
        (40..=64).contains(&r.len())
            && r.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    })
}

fn account_of_stack(stack_id: &str) -> &str {
    // arn:aws:cloudformation:<region>:<account>:stack/<name>/<id>
    let p: Vec<&str> = stack_id.splitn(6, ':').collect();
    if p.len() == 6 && p[2] == "cloudformation" && p[5].starts_with("stack/") {
        p[4]
    } else {
        ""
    }
}

/// The S3 target the stack's outputs describe, checked: the install's own
/// bucket (named for its prefix and account), a clean folder, and a
/// knowledge base in the install's own account and the stack's region.
pub fn check_outputs(
    o: &Outputs,
    install_prefix: &str,
    own_account: &str,
) -> Result<Target, String> {
    let want = format!("{install_prefix}-kb-export-{own_account}");
    if install_prefix.is_empty() || own_account.is_empty() || o.bucket_name.trim() != want {
        return Err(format!("the bucket isn't this install's ({want})."));
    }
    let region = o.region.trim();
    if region.is_empty()
        || !region
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err("the stack didn't say its region.".into());
    }
    let kb = o.knowledge_base_arn.trim();
    if !kb.is_empty() {
        let arn = common::knowledge::remote::parse_kb_arn(kb)?;
        if arn.account != own_account || arn.region != region {
            return Err("the knowledge base isn't in this install's account and region.".into());
        }
    }
    bs::check_target(
        &Target::S3 {
            bucket: o.bucket_name.clone(),
            prefix: o.prefix.clone(),
            region: region.to_string(),
            knowledge_base_arn: kb.to_string(),
            data_source_id: o.data_source_id.clone(),
        },
        true,
    )
}

/// What a report does to the setup it names: the setup after it, or why it
/// is refused. `current` is the team's current setup's hash.
pub fn apply(
    setup: &Setup,
    current: &str,
    r: &Report,
    own_account: &str,
    install_prefix: &str,
    now_secs: i64,
) -> Result<(Setup, Option<Target>), Refused> {
    if !token_shape_ok(&r.token) {
        return Err(Refused::Unknown);
    }
    let got = hash(&r.token);
    if setup.token_hash.len() != got.len()
        || !bool::from(setup.token_hash.as_bytes().ct_eq(got.as_bytes()))
    {
        return Err(Refused::Unknown);
    }
    if setup.state == REPLACED || current != setup.token_hash {
        return Err(Refused::Replaced);
    }
    if setup.done() {
        return Err(Refused::Spent);
    }
    if now_secs >= setup.expires_at {
        return Err(Refused::Expired);
    }
    let acct = account_of_stack(&r.stack_id);
    if acct.is_empty() || (!own_account.is_empty() && acct != own_account) {
        return Err(Refused::WrongAccount(own_account.to_string()));
    }
    if !setup.stack_id.is_empty() && setup.stack_id != r.stack_id {
        return Err(Refused::OtherStack);
    }
    let mut next = setup.clone();
    next.stack_id = r.stack_id.clone();
    let mut target = None;
    let from = setup.state.as_str();
    match r.step.as_str() {
        STARTED if matches!(from, WAITING | STARTED) => next.state = STARTED.into(),
        BUCKET if matches!(from, STARTED | BUCKET) => next.state = BUCKET.into(),
        READY if matches!(from, STARTED | BUCKET) => {
            let o = r.outputs.clone().unwrap_or_default();
            match check_outputs(&o, install_prefix, own_account) {
                Ok(t) => {
                    next.state = READY.into();
                    target = Some(t);
                }
                Err(m) => return Err(Refused::BadOutputs(m)),
            }
        }
        FAILED => {
            next.state = FAILED.into();
            next.message = failure_words(&r.reason);
        }
        _ => return Err(Refused::OutOfOrder),
    }
    Ok((next, target))
}

/// The stack's failure in plain words: the resource and CloudFormation's
/// reason, with the common causes said simply.
pub fn failure_words(reason: &str) -> String {
    let r = common::truncate_str(reason.trim(), 600);
    if r.is_empty() {
        return "The stack failed and rolled back. Its events in the CloudFormation console say why.".into();
    }
    let lower = r.to_ascii_lowercase();
    let hint = if lower.contains("s3vectors")
        || lower.contains("s3 vectors")
        || lower.contains("vectorbucket")
    {
        " S3 Vectors may not be available in this region: choose Use an existing knowledge base, or run the install in a region that has it."
    } else if lower.contains("bedrock")
        && (lower.contains("not available")
            || lower.contains("not supported")
            || lower.contains("unrecognized"))
    {
        " Bedrock knowledge bases may not be available in this region."
    } else if lower.contains("already exists") {
        " A stack from an earlier setup may still exist: delete it, or choose Use an existing knowledge base."
    } else if lower.contains("not authorized")
        || lower.contains("accessdenied")
        || lower.contains("access denied")
    {
        " The person who created the stack needs permission to create IAM roles, S3 buckets and Bedrock knowledge bases."
    } else {
        ""
    };
    format!("The stack failed: {r}.{hint}").replace("..", ".")
}

/// The quick-create link that opens the stack with everything filled in.
#[allow(clippy::too_many_arguments)]
pub fn launch_url(
    template_url: &str,
    region: &str,
    install_prefix: &str,
    account: &str,
    report_url: &str,
    token: &str,
    creates_knowledge_base: bool,
    existing: Option<(&str, &str)>,
) -> String {
    let enc = |s: &str| urlencoding::encode(s).into_owned();
    let mut u = format!(
        "https://console.aws.amazon.com/cloudformation/home?region={region}#/stacks/quickcreate?stackName={}&templateURL={}&param_InstallPrefix={}&param_InstallAccount={account}&param_CoderHelmUrl={}&param_SetupToken={}&param_CreateKnowledgeBase={}",
        enc(&format!("{install_prefix}{STACK_SUFFIX}")),
        enc(template_url),
        enc(install_prefix),
        enc(report_url),
        enc(token),
        if creates_knowledge_base { "yes" } else { "no" },
    );
    if let Some((kb, role)) = existing {
        u.push_str(&format!("&param_KnowledgeBaseId={}", enc(kb)));
        if !role.is_empty() {
            u.push_str(&format!("&param_KnowledgeBaseRoleName={}", enc(role)));
        }
    }
    u
}

/// The export template's URL: published with the release, beside the log
/// reader's. `None` when the install has no release copy to point at.
pub fn template_url(log_reader_url: &str) -> Option<String> {
    log_reader_url
        .contains("/launch/")
        .then(|| log_reader_url.strip_suffix("coderhelm-log-reader.yaml"))
        .flatten()
        .map(|base| format!("{base}{TEMPLATE_FILE}"))
}

/// The page's view: never the hash.
pub fn view(s: &Setup, now_secs: i64) -> serde_json::Value {
    serde_json::json!({
        "state": if !s.done() && now_secs >= s.expires_at { "expired" } else { s.state.as_str() },
        "message": s.message,
        "stack_name": s.stack_name,
        "region": s.region,
        "created_at": s.created_at,
        "created_by": s.created_by,
        "expires_at": chrono::DateTime::from_timestamp(s.expires_at, 0).map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        "creates_knowledge_base": s.creates_knowledge_base,
        "updated_at": s.updated_at,
    })
}

// ── Storage ─────────────────────────────────────────────────────────

#[async_trait::async_trait]
pub trait SetupStore: Send + Sync {
    async fn by_hash(&self, hash: &str) -> Result<Option<Setup>, String>;
    /// The team's current setup's hash ("" for none).
    async fn current(&self, team_id: &str) -> Result<String, String>;
    /// Saves `s` if its stored version is still `s.version - 1` (or it is
    /// new, with version 1). False when another save came first.
    async fn save(&self, s: &Setup) -> Result<bool, String>;
    async fn set_current(&self, team_id: &str, hash: &str) -> Result<(), String>;
}

/// Starts a setup: saves it as the team's current one, the one before it
/// marked replaced so its link stops working.
pub async fn start(store: &dyn SetupStore, s: &Setup) -> Result<(), String> {
    let before = store.current(&s.team_id).await?;
    if !before.is_empty() {
        if let Some(mut old) = store.by_hash(&before).await? {
            if !old.done() {
                old.state = REPLACED.into();
                old.version += 1;
                let _ = store.save(&old).await?;
            }
        }
    }
    let mut s = s.clone();
    s.version = 1;
    if !store.save(&s).await? {
        return Err("a setup with this token already exists".into());
    }
    store.set_current(&s.team_id, &s.token_hash).await
}

/// Takes a report: the setup after it and, on `ready`, the target to copy
/// into. A race with another report of the same setup is refused, so a
/// token completes at most once.
pub async fn receive(
    store: &dyn SetupStore,
    r: &Report,
    own_account: &str,
    install_prefix: &str,
    now_secs: i64,
    now: &str,
) -> Result<Result<(Setup, Option<Target>), Refused>, String> {
    if !token_shape_ok(&r.token) {
        return Ok(Err(Refused::Unknown));
    }
    let Some(setup) = store.by_hash(&hash(&r.token)).await? else {
        return Ok(Err(Refused::Unknown));
    };
    let current = store.current(&setup.team_id).await?;
    let (mut next, target) = match apply(&setup, &current, r, own_account, install_prefix, now_secs)
    {
        Ok(x) => x,
        Err(e) => return Ok(Err(e)),
    };
    next.updated_at = now.to_string();
    next.version = setup.version + 1;
    if !store.save(&next).await? {
        return Ok(Err(Refused::Spent));
    }
    Ok(Ok((next, target)))
}

pub struct DynamoSetups<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub table: &'a str,
}

pub fn store(state: &crate::AppState) -> DynamoSetups<'_> {
    DynamoSetups {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
    }
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn err<E: std::error::Error + 'static>(e: aws_sdk_dynamodb::error::SdkError<E>) -> String {
    format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e))
}

#[async_trait::async_trait]
impl SetupStore for DynamoSetups<'_> {
    async fn by_hash(&self, hash: &str) -> Result<Option<Setup>, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", s(&format!("{PK_PREFIX}{hash}")))
            .key("sk", s(SK))
            .consistent_read(true)
            .send()
            .await
            .map_err(err)?;
        Ok(r.item()
            .and_then(|i| i.get("data"))
            .and_then(|d| d.as_s().ok())
            .and_then(|d| serde_json::from_str(d).ok()))
    }

    async fn current(&self, team_id: &str) -> Result<String, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", s(team_id))
            .key("sk", s(SK))
            .consistent_read(true)
            .send()
            .await
            .map_err(err)?;
        Ok(r.item()
            .and_then(|i| i.get("token_hash"))
            .and_then(|d| d.as_s().ok())
            .cloned()
            .unwrap_or_default())
    }

    async fn save(&self, st: &Setup) -> Result<bool, String> {
        let data = serde_json::to_string(st).map_err(|e| e.to_string())?;
        // Kept a week past its expiry, for the page to say what happened.
        let ttl = st.expires_at + 7 * 86_400;
        let mut put = self
            .dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", s(&format!("{PK_PREFIX}{}", st.token_hash)))
            .item("sk", s(SK))
            .item("data", s(&data))
            .item("version", AttributeValue::N(st.version.to_string()))
            .item("ttl", AttributeValue::N(ttl.to_string()));
        put = if st.version <= 1 {
            put.condition_expression("attribute_not_exists(pk)")
        } else {
            put.condition_expression("version = :v")
                .expression_attribute_values(":v", AttributeValue::N((st.version - 1).to_string()))
        };
        match put.send().await {
            Ok(_) => Ok(true),
            Err(e)
                if e.as_service_error()
                    .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
            {
                Ok(false)
            }
            Err(e) => Err(err(e)),
        }
    }

    async fn set_current(&self, team_id: &str, hash: &str) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", s(team_id))
            .item("sk", s(SK))
            .item("token_hash", s(hash))
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct Mem {
        setups: Mutex<HashMap<String, Setup>>,
        current: Mutex<HashMap<String, String>>,
    }

    #[async_trait::async_trait]
    impl SetupStore for Mem {
        async fn by_hash(&self, hash: &str) -> Result<Option<Setup>, String> {
            Ok(self.setups.lock().unwrap().get(hash).cloned())
        }
        async fn current(&self, team_id: &str) -> Result<String, String> {
            Ok(self
                .current
                .lock()
                .unwrap()
                .get(team_id)
                .cloned()
                .unwrap_or_default())
        }
        async fn save(&self, st: &Setup) -> Result<bool, String> {
            let mut m = self.setups.lock().unwrap();
            let ok = match m.get(&st.token_hash) {
                None => st.version <= 1,
                Some(old) => st.version > 1 && old.version == st.version - 1,
            };
            if ok {
                m.insert(st.token_hash.clone(), st.clone());
            }
            Ok(ok)
        }
        async fn set_current(&self, team_id: &str, hash: &str) -> Result<(), String> {
            self.current
                .lock()
                .unwrap()
                .insert(team_id.into(), hash.into());
            Ok(())
        }
    }

    const ACCOUNT: &str = "111122223333";
    const PREFIX: &str = "coderhelm-prod";
    const STACK: &str =
        "arn:aws:cloudformation:us-east-1:111122223333:stack/coderhelm-prod-bedrock-export/1";
    const NOW: i64 = 1_800_000_000;

    fn setup(team: &str) -> (String, Setup) {
        let (token, h) = generate();
        (
            token,
            Setup {
                team_id: team.into(),
                token_hash: h,
                expires_at: NOW + TOKEN_TTL_SECS,
                region: "us-east-1".into(),
                bases: vec!["kb1".into()],
                creates_knowledge_base: true,
                state: WAITING.into(),
                ..Default::default()
            },
        )
    }

    fn outputs() -> Outputs {
        Outputs {
            bucket_name: format!("{PREFIX}-kb-export-{ACCOUNT}"),
            prefix: "coderhelm/".into(),
            region: "us-east-1".into(),
            knowledge_base_arn: format!(
                "arn:aws:bedrock:us-east-1:{ACCOUNT}:knowledge-base/ABCDE12345"
            ),
            data_source_id: "DS12345678".into(),
        }
    }

    fn report(token: &str, step: &str) -> Report {
        Report {
            token: token.into(),
            step: step.into(),
            stack_id: STACK.into(),
            outputs: (step == READY).then(outputs),
            reason: String::new(),
        }
    }

    async fn send(st: &Mem, r: &Report, at: i64) -> Result<(Setup, Option<Target>), Refused> {
        receive(st, r, ACCOUNT, PREFIX, at, "now").await.unwrap()
    }

    #[test]
    fn a_token_is_long_random_and_kept_only_as_its_hash() {
        let (a, ha) = generate();
        let (b, _) = generate();
        assert_ne!(a, b);
        assert!(token_shape_ok(&a), "{a}");
        assert_eq!(ha, hash(&a));
        assert_eq!(ha.len(), 64);
        let (_, s) = setup("T");
        assert!(!view(&s, NOW).to_string().contains(&s.token_hash));
    }

    #[tokio::test]
    async fn the_steps_go_in_order_and_ready_picks_up_the_outputs() {
        let st = Mem::default();
        let (token, s) = setup("T");
        start(&st, &s).await.unwrap();
        assert_eq!(
            send(&st, &report(&token, STARTED), NOW)
                .await
                .unwrap()
                .0
                .state,
            STARTED
        );
        assert_eq!(
            send(&st, &report(&token, BUCKET), NOW)
                .await
                .unwrap()
                .0
                .state,
            BUCKET
        );
        let (done, target) = send(&st, &report(&token, READY), NOW + 60).await.unwrap();
        assert_eq!(done.state, READY);
        assert_eq!(done.stack_id, STACK);
        assert_eq!(
            target.unwrap(),
            Target::S3 {
                bucket: format!("{PREFIX}-kb-export-{ACCOUNT}"),
                prefix: "coderhelm/".into(),
                region: "us-east-1".into(),
                knowledge_base_arn: format!(
                    "arn:aws:bedrock:us-east-1:{ACCOUNT}:knowledge-base/ABCDE12345"
                ),
                data_source_id: "DS12345678".into(),
            }
        );
    }

    #[tokio::test]
    async fn a_token_completes_once() {
        let st = Mem::default();
        let (token, s) = setup("T");
        start(&st, &s).await.unwrap();
        send(&st, &report(&token, STARTED), NOW).await.unwrap();
        send(&st, &report(&token, READY), NOW).await.unwrap();
        for step in [STARTED, BUCKET, READY, FAILED] {
            assert_eq!(
                send(&st, &report(&token, step), NOW).await.unwrap_err(),
                Refused::Spent,
                "{step}"
            );
        }
        // A failed setup is spent too.
        let (token, s) = setup("T2");
        start(&st, &s).await.unwrap();
        let f = send(
            &st,
            &Report {
                reason: "VectorBucket: not available".into(),
                ..report(&token, FAILED)
            },
            NOW,
        )
        .await
        .unwrap()
        .0;
        assert!(
            f.message.starts_with("The stack failed: VectorBucket"),
            "{}",
            f.message
        );
        assert!(
            f.message.contains("S3 Vectors may not be available"),
            "{}",
            f.message
        );
        assert_eq!(
            send(&st, &report(&token, STARTED), NOW).await.unwrap_err(),
            Refused::Spent
        );
    }

    #[tokio::test]
    async fn a_token_expires() {
        let st = Mem::default();
        let (token, s) = setup("T");
        start(&st, &s).await.unwrap();
        let late = NOW + TOKEN_TTL_SECS;
        assert_eq!(
            send(&st, &report(&token, STARTED), late).await.unwrap_err(),
            Refused::Expired
        );
        assert_eq!(
            view(&st.by_hash(&s.token_hash).await.unwrap().unwrap(), late)["state"],
            "expired"
        );
        assert!(send(&st, &report(&token, STARTED), late - 1).await.is_ok());
    }

    #[tokio::test]
    async fn a_token_is_bound_to_its_team_its_account_and_its_first_stack() {
        let st = Mem::default();
        let (token, s) = setup("T");
        start(&st, &s).await.unwrap();
        // Unknown or malformed tokens.
        let (other, _) = generate();
        assert_eq!(
            send(&st, &report(&other, STARTED), NOW).await.unwrap_err(),
            Refused::Unknown
        );
        assert_eq!(
            send(&st, &report("chbs_short", STARTED), NOW)
                .await
                .unwrap_err(),
            Refused::Unknown
        );
        // Another account.
        let elsewhere = Report {
            stack_id: STACK.replace(ACCOUNT, "999988887777"),
            ..report(&token, STARTED)
        };
        assert_eq!(
            send(&st, &elsewhere, NOW).await.unwrap_err(),
            Refused::WrongAccount(ACCOUNT.into())
        );
        // The first stack holds it.
        send(&st, &report(&token, STARTED), NOW).await.unwrap();
        let second = Report {
            stack_id: format!("{STACK}2"),
            ..report(&token, BUCKET)
        };
        assert_eq!(
            send(&st, &second, NOW).await.unwrap_err(),
            Refused::OtherStack
        );
        // A newer setup of the team replaces it.
        let (newer, s2) = setup("T");
        start(&st, &s2).await.unwrap();
        assert_eq!(
            send(&st, &report(&token, BUCKET), NOW).await.unwrap_err(),
            Refused::Replaced
        );
        assert!(send(&st, &report(&newer, STARTED), NOW).await.is_ok());
        // Another team's setup doesn't touch this one.
        let (_, s3) = setup("U");
        start(&st, &s3).await.unwrap();
        assert!(send(&st, &report(&newer, BUCKET), NOW).await.is_ok());
    }

    #[tokio::test]
    async fn steps_out_of_order_are_refused() {
        let st = Mem::default();
        let (token, s) = setup("T");
        start(&st, &s).await.unwrap();
        assert_eq!(
            send(&st, &report(&token, READY), NOW).await.unwrap_err(),
            Refused::OutOfOrder
        );
        assert_eq!(
            send(&st, &report(&token, BUCKET), NOW).await.unwrap_err(),
            Refused::OutOfOrder
        );
        assert_eq!(
            send(&st, &report(&token, "anything"), NOW)
                .await
                .unwrap_err(),
            Refused::OutOfOrder
        );
    }

    #[test]
    fn outputs_must_be_the_installs_own() {
        assert!(check_outputs(&outputs(), PREFIX, ACCOUNT).is_ok());
        let bad = |f: &dyn Fn(&mut Outputs)| {
            let mut o = outputs();
            f(&mut o);
            check_outputs(&o, PREFIX, ACCOUNT).unwrap_err()
        };
        assert!(bad(&|o| o.bucket_name = "someone-elses-bucket".into())
            .contains("isn't this install's"));
        assert!(bad(
            &|o| o.knowledge_base_arn = o.knowledge_base_arn.replace(ACCOUNT, "999988887777")
        )
        .contains("account and region"));
        assert!(bad(
            &|o| o.knowledge_base_arn = o.knowledge_base_arn.replace("us-east-1", "eu-west-1")
        )
        .contains("account and region"));
        assert!(bad(&|o| o.data_source_id = "nope".into()).contains("data source"));
        assert!(bad(&|o| o.prefix = "../x".into()).contains("folder"));
        assert!(bad(&|o| o.region = String::new()).contains("region"));
        // Without a knowledge base (CreateKnowledgeBase no): the bucket alone.
        let mut o = outputs();
        o.knowledge_base_arn.clear();
        o.data_source_id.clear();
        assert!(check_outputs(&o, PREFIX, ACCOUNT).is_ok());
        // An install that doesn't know its prefix accepts nothing.
        assert!(check_outputs(&outputs(), "", ACCOUNT).is_err());
    }

    #[test]
    fn the_link_fills_in_every_parameter() {
        let u = launch_url(
            "https://b.s3.amazonaws.com/launch/v1.2.3/coderhelm-bedrock-export.yaml",
            "us-east-1",
            PREFIX,
            ACCOUNT,
            "https://ch.example.org/api/knowledge/bedrock-setup/report",
            "chbs_abc",
            true,
            None,
        );
        assert!(u.starts_with("https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/quickcreate?stackName=coderhelm-prod-bedrock-export&"));
        for p in [
            "templateURL=https%3A%2F%2Fb.s3.amazonaws.com%2Flaunch%2Fv1.2.3%2Fcoderhelm-bedrock-export.yaml",
            "param_InstallPrefix=coderhelm-prod",
            "param_InstallAccount=111122223333",
            "param_CoderHelmUrl=https%3A%2F%2Fch.example.org%2Fapi%2Fknowledge%2Fbedrock-setup%2Freport",
            "param_SetupToken=chbs_abc",
            "param_CreateKnowledgeBase=yes",
        ] {
            assert!(u.contains(p), "{p} in {u}");
        }
        assert!(!u.contains("KnowledgeBaseId"));
        let u = launch_url(
            "t",
            "us-east-1",
            PREFIX,
            ACCOUNT,
            "r",
            "chbs_abc",
            false,
            Some(("ABCDE12345", "MyRole")),
        );
        assert!(u.contains("param_CreateKnowledgeBase=no&param_KnowledgeBaseId=ABCDE12345&param_KnowledgeBaseRoleName=MyRole"));
        assert_eq!(
            template_url("https://b.s3.amazonaws.com/launch/v1.2.3/coderhelm-log-reader.yaml")
                .as_deref(),
            Some("https://b.s3.amazonaws.com/launch/v1.2.3/coderhelm-bedrock-export.yaml")
        );
        // The hosted copy has no export template beside it.
        assert_eq!(
            template_url("https://coderhelm-public.s3.amazonaws.com/cfn/coderhelm-log-reader.yaml"),
            None
        );
    }

    #[test]
    fn the_stack_template_matches_this_module() {
        let t = bs::EXPORT_TEMPLATE;
        assert!(t.contains("AllowedPattern: \"^(|chbs_[A-Za-z0-9_-]{40,64})$\""));
        assert!(t.contains("NoEcho: true"));
        for step in [STARTED, BUCKET, READY] {
            assert!(t.contains(&format!("Step: {step}")), "{step}");
        }
        assert!(t.contains("report(p, ev, \"failed\""));
        for o in [
            "BucketName:",
            "Prefix:",
            "Region:",
            "KnowledgeBaseArn:",
            "DataSourceId:",
        ] {
            assert!(t.contains(&format!("        {o}")), "{o}");
        }
        // The report comes before anything is made.
        assert!(t.contains("DependsOn: ReportStarted"));
    }
}
