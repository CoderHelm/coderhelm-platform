//! Whether a team's CoderHelmLogReader role has every permission this
//! release uses ([`crate::log_reader::ABILITIES`]), checked live.
//!
//! The exact way is IAM's policy simulator on the role itself
//! (`iam:SimulatePrincipalPolicy`, which the current template allows on
//! that one role): it answers per action, and says when a permissions
//! boundary or an organization policy (SCP) is what blocks it. A role made
//! before that permission falls back to the cheapest real read per ability
//! (one list call each), where a refusal marks the ability's actions
//! missing. Nothing is changed in the account.
//!
//! After someone presses Update, the check also looks for this release's
//! update stack (when the role may describe stacks), so a failure comes
//! back as one sentence and a fix: the stack failed, it isn't in the role's
//! account, a boundary or an SCP blocks the permissions.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::aws::{assume, AwsTarget, Session};
use crate::log_reader::{self, ABILITIES};

/// What the simulator said about one action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Simulated {
    pub action: String,
    pub allowed: bool,
    /// `Some(false)`: a permissions boundary on the role blocks it.
    pub boundary_allows: Option<bool>,
    /// `Some(false)`: an organization policy (SCP) blocks it.
    pub org_allows: Option<bool>,
}

/// This release's update stack, as CloudFormation describes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum UpdateStack {
    /// The role may not describe stacks (roles made before it could).
    Unknown,
    NotFound,
    Found {
        status: String,
        reason: String,
    },
}

/// One live check of a role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionCheck {
    pub account: String,
    pub region: String,
    pub permissions_id: String,
    pub checked_at: String,
    /// The role could be taken on.
    pub works: bool,
    /// `simulate` or `probe` (empty when the role couldn't be taken on).
    pub method: String,
    /// Actions of this release the role lacks.
    pub missing: Vec<String>,
    /// Missing actions a boundary blocks, and ones an SCP blocks.
    pub boundary_blocks: Vec<String>,
    pub scp_blocks: Vec<String>,
    pub update_stack: UpdateStack,
    pub error: String,
}

/// What the page shows for one account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Problem {
    pub code: &'static str,
    pub message: String,
    pub fix: String,
}

impl PermissionCheck {
    pub fn new(target: &AwsTarget, now: &str) -> Self {
        PermissionCheck {
            account: target.account().to_string(),
            region: target.region.clone(),
            permissions_id: log_reader::permissions_id(),
            checked_at: now.to_string(),
            works: false,
            method: String::new(),
            missing: Vec::new(),
            boundary_blocks: Vec::new(),
            scp_blocks: Vec::new(),
            update_stack: UpdateStack::Unknown,
            error: String::new(),
        }
    }

    /// `up_to_date`, `update_needed` or `error`.
    pub fn status(&self) -> &'static str {
        if !self.works {
            "error"
        } else if self.missing.is_empty() {
            "up_to_date"
        } else {
            "update_needed"
        }
    }

    /// The abilities that lack at least one action, in page order.
    pub fn missing_abilities(&self) -> Vec<&'static str> {
        ABILITIES
            .iter()
            .filter(|a| {
                a.actions
                    .iter()
                    .any(|x| self.missing.iter().any(|m| m == x))
            })
            .map(|a| a.key)
            .collect()
    }

    /// Why an update can't fix it, or why it hasn't worked yet, in one
    /// sentence and a fix. `pressed` says someone pressed Update for this
    /// account, so a missing stack is worth naming.
    pub fn problem(&self, pressed: bool) -> Option<Problem> {
        let a = &self.account;
        if !self.works {
            return Some(Problem {
                code: "role",
                message: if self.error.is_empty() {
                    format!("Coderhelm couldn't take on the role in account {a}.")
                } else {
                    self.error.clone()
                },
                fix: format!("Check that the CoderHelmLogReader role still exists in account {a} and trusts this Coderhelm install with your External ID."),
            });
        }
        if self.missing.is_empty() {
            return None;
        }
        if !self.scp_blocks.is_empty() {
            return Some(Problem {
                code: "scp",
                message: format!(
                    "An organization policy (SCP) blocks {} of these permissions in account {a}.",
                    self.scp_blocks.len()
                ),
                fix: "Ask your AWS organization's admin to allow these read-only actions for the CoderHelmLogReader role, then check again.".into(),
            });
        }
        if !self.boundary_blocks.is_empty() {
            return Some(Problem {
                code: "boundary",
                message: format!(
                    "A permissions boundary on CoderHelmLogReader blocks {} of these permissions.",
                    self.boundary_blocks.len()
                ),
                fix: "Remove the boundary from the role, or add these read-only actions to it, then check again.".into(),
            });
        }
        let stack = log_reader::update_stack_name();
        match &self.update_stack {
            UpdateStack::Found { status, reason } if failed(status) => Some(Problem {
                code: "stack_failed",
                message: if reason.is_empty() {
                    format!("The update stack {stack} failed in AWS ({status}).")
                } else {
                    format!("The update stack {stack} failed in AWS ({status}: {reason}).")
                },
                fix: "Open the stack's Events tab for the cause, delete the failed stack, then press Update again.".into(),
            }),
            UpdateStack::Found { status, .. } if status.ends_with("_IN_PROGRESS") => Some(Problem {
                code: "stack_in_progress",
                message: format!("The update stack {stack} is still being created ({status})."),
                fix: "Wait for CREATE_COMPLETE; this page checks again by itself.".into(),
            }),
            UpdateStack::NotFound if pressed => Some(Problem {
                code: "stack_not_found",
                message: format!("There's no stack named {stack} in account {a}, region {}.", self.region),
                fix: format!("Sign in to AWS account {a}, the role's account, and press Update again: the stack must be created there."),
            }),
            UpdateStack::Unknown if pressed => Some(Problem {
                code: "not_yet",
                message: "The new permissions aren't on the role yet.".into(),
                fix: format!("Make sure you created the stack while signed in to account {a} and that it reached CREATE_COMPLETE, then check again."),
            }),
            _ => None,
        }
    }
}

impl PermissionCheck {
    /// Checked against this release's permissions (an older check is run
    /// again before it is shown as current).
    pub fn is_current(&self) -> bool {
        self.permissions_id == log_reader::permissions_id()
    }

    /// What the dashboard shows for the account: the state, every
    /// permission of this release grouped by what it reads (and which are
    /// missing), the one-click update and any problem. `template_url` is
    /// the install's role template ([`crate::public_urls::log_reader_template_url`]).
    pub fn view(&self, pressed: bool, template_url: &str) -> Value {
        let missing: Vec<Value> = self
            .missing
            .iter()
            .map(|a| {
                let ab = log_reader::ability_of(a);
                json!({
                    "action": a,
                    "ability": ab.map(|x| x.key),
                    "ability_name": ab.map(|x| x.name),
                })
            })
            .collect();
        let abilities: Vec<Value> = ABILITIES
            .iter()
            .map(|a| {
                let gone: Vec<&str> = a
                    .actions
                    .iter()
                    .copied()
                    .filter(|x| self.missing.iter().any(|m| m == x))
                    .collect();
                json!({
                    "key": a.key,
                    "name": a.name,
                    "why": a.why,
                    "actions": a.actions,
                    "missing": gone,
                })
            })
            .collect();
        let update = (self.status() == "update_needed")
            .then(|| log_reader::update_template_url(template_url))
            .flatten()
            .map(|t| {
                json!({
                    "url": log_reader::update_url(&t, &self.region),
                    "stack_name": log_reader::update_stack_name(),
                    "template_url": t,
                    "sign_in_account": self.account,
                    "stacks_url": log_reader::stack_list_url(&self.region),
                })
            });
        json!({
            "account": self.account,
            "region": self.region,
            "status": self.status(),
            "method": self.method,
            "checked_at": self.checked_at,
            "permissions_id": self.permissions_id,
            "current": self.is_current(),
            "missing_count": self.missing.len(),
            "missing": missing,
            "missing_abilities": self.missing_abilities(),
            "abilities": abilities,
            "update": update,
            "problem": self.problem(pressed),
        })
    }
}

fn failed(status: &str) -> bool {
    status.contains("FAILED") || status.contains("ROLLBACK")
}

/// Pure: the simulator's answers folded into a check.
pub fn apply_simulation(c: &mut PermissionCheck, results: &[Simulated]) {
    c.method = "simulate".into();
    for r in results {
        if r.allowed || c.missing.contains(&r.action) {
            continue;
        }
        c.missing.push(r.action.clone());
        if r.org_allows == Some(false) {
            c.scp_blocks.push(r.action.clone());
        } else if r.boundary_allows == Some(false) {
            c.boundary_blocks.push(r.action.clone());
        }
    }
    order(&mut c.missing);
}

/// Pure: per-ability probe answers (`Some(false)`: refused) folded into a
/// check. The simulator itself was refused, so its action is missing too.
pub fn apply_probes(c: &mut PermissionCheck, probes: &[(&str, Option<bool>)]) {
    c.method = "probe".into();
    let mut missing: Vec<String> = vec!["iam:SimulatePrincipalPolicy".into()];
    for (key, ok) in probes {
        if *ok == Some(false) {
            if let Some(a) = log_reader::ability(key) {
                missing.extend(a.actions.iter().map(|s| s.to_string()));
            }
        }
    }
    missing.dedup();
    c.missing = missing;
    order(&mut c.missing);
}

/// Template order, once each.
fn order(v: &mut Vec<String>) {
    let all = log_reader::all_actions();
    v.sort_by_key(|a| all.iter().position(|x| x == a).unwrap_or(usize::MAX));
    v.dedup();
}

/// The resource each action is simulated on: one that the template's
/// statement for it covers.
pub fn simulated_resource(action: &str, role_arn: &str, region: &str, account: &str) -> String {
    match action {
        "logs:StartQuery"
        | "logs:GetQueryResults"
        | "logs:StopQuery"
        | "logs:FilterLogEvents"
        | "logs:GetLogEvents" => {
            format!("arn:aws:logs:{region}:{account}:log-group:coderhelm-permission-check")
        }
        "airflow:InvokeRestApi" => {
            format!("arn:aws:airflow:{region}:{account}:role/coderhelm-permission-check/Viewer")
        }
        "iam:SimulatePrincipalPolicy" => role_arn.to_string(),
        _ => "*".to_string(),
    }
}

macro_rules! client {
    ($krate:ident, $s:expr, $region:expr) => {{
        use $krate::config::{BehaviorVersion, Credentials, Region};
        let s: &Session = $s;
        let creds = Credentials::new(
            &s.access_key_id,
            &s.secret_access_key,
            Some(s.session_token.clone()),
            None,
            "coderhelm-team-role",
        );
        $krate::Client::from_conf(
            $krate::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new($region))
                .credentials_provider(creds)
                .build(),
        )
    }};
}

use aws_sdk_sts::error::ProvideErrorMetadata;

fn refused(e: &impl ProvideErrorMetadata) -> bool {
    if e.code() == Some("ClientException") {
        return e
            .message()
            .is_some_and(|m| m.contains("not authorized") || m.contains("AccessDenied"));
    }
    crate::log_repo::live::is_refusal(e.code())
}

/// The simulator's answers, or `None` when the role may not ask it.
async fn simulate(s: &Session, target: &AwsTarget) -> Result<Vec<Simulated>, bool> {
    let iam = client!(aws_sdk_iam, s, "us-east-1".to_string());
    let account = target.account();
    let mut groups: Vec<(String, Vec<String>)> = Vec::new();
    for a in log_reader::all_actions() {
        let r = simulated_resource(a, &target.role_arn, &target.region, account);
        match groups.iter_mut().find(|(res, _)| *res == r) {
            Some((_, v)) => v.push(a.to_string()),
            None => groups.push((r, vec![a.to_string()])),
        }
    }
    let mut out = Vec::new();
    for (resource, actions) in groups {
        let r = iam
            .simulate_principal_policy()
            .policy_source_arn(&target.role_arn)
            .set_action_names(Some(actions))
            .resource_arns(resource)
            .send()
            .await;
        match r {
            Ok(o) => {
                for e in o.evaluation_results() {
                    use aws_sdk_iam::types::PolicyEvaluationDecisionType as D;
                    out.push(Simulated {
                        action: e.eval_action_name().to_string(),
                        allowed: matches!(e.eval_decision(), D::Allowed),
                        boundary_allows: e
                            .permissions_boundary_decision_detail()
                            .map(|d| d.allowed_by_permissions_boundary()),
                        org_allows: e
                            .organizations_decision_detail()
                            .map(|d| d.allowed_by_organizations()),
                    });
                }
            }
            // Refused: the role predates the permission. Anything else:
            // the simulator didn't answer; fall back the same way.
            Err(e) => return Err(refused(&e)),
        }
    }
    Ok(out)
}

/// The cheapest real read per ability: `Some(false)` when refused.
async fn probes(s: &Session) -> Vec<(&'static str, Option<bool>)> {
    let ok = |r: Result<(), bool>| match r {
        Ok(()) => Some(true),
        Err(true) => Some(false),
        Err(false) => None,
    };
    let logs = super::aws::logs_client(s)
        .describe_log_groups()
        .limit(1)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| refused(&e));
    let metrics = super::metrics::probe(s).await.ok();
    let airflow = match super::airflow::list_environments(s).await {
        Ok(Some(_)) => Some(true),
        Ok(None) => Some(false),
        Err(_) => None,
    };
    let ecs = client!(aws_sdk_ecs, s, s.region.clone())
        .list_clusters()
        .max_results(1)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| refused(&e));
    let code = client!(aws_sdk_lambda, s, s.region.clone())
        .list_functions()
        .max_items(1)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| refused(&e));
    vec![
        ("logs", ok(logs)),
        ("metrics", metrics),
        ("airflow", airflow),
        ("ecs", ok(ecs)),
        ("code", ok(code)),
    ]
}

async fn update_stack(s: &Session) -> UpdateStack {
    let cfn = client!(aws_sdk_cloudformation, s, s.region.clone());
    match cfn
        .describe_stacks()
        .stack_name(log_reader::update_stack_name())
        .send()
        .await
    {
        Ok(o) => match o.stacks().first() {
            Some(st) => UpdateStack::Found {
                status: st
                    .stack_status()
                    .map(|x| x.as_str().to_string())
                    .unwrap_or_default(),
                reason: crate::truncate_str(st.stack_status_reason().unwrap_or(""), 300)
                    .to_string(),
            },
            None => UpdateStack::NotFound,
        },
        Err(e) if refused(&e) => UpdateStack::Unknown,
        Err(e) => {
            let not_found = e.code() == Some("ValidationError")
                && e.message().is_some_and(|m| m.contains("does not exist"));
            if not_found {
                UpdateStack::NotFound
            } else {
                UpdateStack::Unknown
            }
        }
    }
}

/// Takes on the role and checks every permission of this release.
pub async fn check(
    sts: &aws_sdk_sts::Client,
    target: &AwsTarget,
    actor: &str,
    now: &str,
) -> PermissionCheck {
    let mut c = PermissionCheck::new(target, now);
    let session = match assume(sts, target, actor).await {
        Ok(s) => s,
        Err(e) => {
            c.error = e.message;
            return c;
        }
    };
    c.works = true;
    match simulate(&session, target).await {
        Ok(results) => apply_simulation(&mut c, &results),
        Err(_) => {
            let p = probes(&session).await;
            apply_probes(&mut c, &p);
        }
    }
    if !c.missing.is_empty() {
        c.update_stack = update_stack(&session).await;
    }
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> AwsTarget {
        AwsTarget {
            role_arn: "arn:aws:iam::111122223333:role/CoderHelmLogReader".into(),
            external_id: "x".into(),
            region: "eu-west-1".into(),
            ..Default::default()
        }
    }

    fn sim(action: &str, allowed: bool) -> Simulated {
        Simulated {
            action: action.into(),
            allowed,
            boundary_allows: None,
            org_allows: None,
        }
    }

    fn checked() -> PermissionCheck {
        let mut c = PermissionCheck::new(&target(), "2026-10-26T00:00:00Z");
        c.works = true;
        c
    }

    #[test]
    fn everything_allowed_is_up_to_date() {
        let mut c = checked();
        let all: Vec<Simulated> = log_reader::all_actions()
            .iter()
            .map(|a| sim(a, true))
            .collect();
        apply_simulation(&mut c, &all);
        assert_eq!(c.status(), "up_to_date");
        assert!(c.problem(true).is_none());
        assert_eq!(c.method, "simulate");
    }

    #[test]
    fn missing_actions_are_listed_in_template_order() {
        let mut c = checked();
        apply_simulation(
            &mut c,
            &[
                sim("ecs:DescribeTasks", false),
                sim("ecs:ListTasks", false),
                sim("logs:GetLogEvents", true),
            ],
        );
        assert_eq!(c.status(), "update_needed");
        assert_eq!(c.missing, vec!["ecs:ListTasks", "ecs:DescribeTasks"]);
        assert_eq!(c.missing_abilities(), vec!["ecs"]);
        // Nothing pressed yet: no problem to show, just the update.
        assert!(c.problem(false).is_none());
    }

    #[test]
    fn a_boundary_or_an_scp_is_named() {
        let mut c = checked();
        apply_simulation(
            &mut c,
            &[
                Simulated {
                    boundary_allows: Some(false),
                    ..sim("ecs:ListTasks", false)
                },
                Simulated {
                    org_allows: Some(false),
                    ..sim("ecs:DescribeTasks", false)
                },
            ],
        );
        assert_eq!(c.problem(false).unwrap().code, "scp");
        let mut b = checked();
        apply_simulation(
            &mut b,
            &[Simulated {
                boundary_allows: Some(false),
                ..sim("ecs:ListTasks", false)
            }],
        );
        let p = b.problem(false).unwrap();
        assert_eq!(p.code, "boundary");
        assert!(p.message.contains("permissions boundary"));
    }

    #[test]
    fn an_old_role_falls_back_to_probes() {
        let mut c = checked();
        apply_probes(
            &mut c,
            &[
                ("logs", Some(true)),
                ("metrics", Some(true)),
                ("airflow", None),
                ("ecs", Some(false)),
                ("code", Some(false)),
            ],
        );
        assert_eq!(c.method, "probe");
        assert!(c
            .missing
            .contains(&"iam:SimulatePrincipalPolicy".to_string()));
        assert!(c.missing.contains(&"ecs:DescribeTasks".to_string()));
        assert!(c.missing.contains(&"lambda:GetFunction".to_string()));
        assert!(!c.missing.iter().any(|m| m.starts_with("airflow:")));
        assert_eq!(c.missing_abilities(), vec!["check", "ecs", "code"]);
    }

    #[test]
    fn after_update_the_stack_says_what_went_wrong() {
        let mut c = checked();
        c.missing = vec!["ecs:ListTasks".into()];
        c.update_stack = UpdateStack::Found {
            status: "ROLLBACK_COMPLETE".into(),
            reason: "The role with name CoderHelmLogReader cannot be found.".into(),
        };
        let p = c.problem(true).unwrap();
        assert_eq!(p.code, "stack_failed");
        assert!(p.message.contains("cannot be found"));
        c.update_stack = UpdateStack::NotFound;
        assert!(c.problem(false).is_none());
        let p = c.problem(true).unwrap();
        assert_eq!(p.code, "stack_not_found");
        assert!(p.fix.contains("111122223333"));
        c.update_stack = UpdateStack::Unknown;
        assert_eq!(c.problem(true).unwrap().code, "not_yet");
        c.update_stack = UpdateStack::Found {
            status: "CREATE_IN_PROGRESS".into(),
            reason: String::new(),
        };
        assert_eq!(c.problem(true).unwrap().code, "stack_in_progress");
    }

    #[test]
    fn the_view_offers_the_update_for_this_release() {
        let mut c = checked();
        c.missing = vec!["ecs:ListTasks".into(), "ecs:DescribeTasks".into()];
        let v = c.view(
            false,
            "https://b.s3.amazonaws.com/launch/v1.2.3/coderhelm-log-reader.yaml",
        );
        assert_eq!(v["status"], "update_needed");
        assert_eq!(v["missing_count"], 2);
        assert_eq!(v["missing"][0]["ability"], "ecs");
        assert_eq!(v["update"]["sign_in_account"], "111122223333");
        assert!(v["update"]["url"]
            .as_str()
            .unwrap()
            .contains("region=eu-west-1"));
        assert!(v["update"]["template_url"]
            .as_str()
            .unwrap()
            .ends_with("/coderhelm-log-reader-update.yaml"));
        let ecs = v["abilities"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["key"] == "ecs")
            .unwrap();
        assert_eq!(ecs["missing"].as_array().unwrap().len(), 2);
        assert!(v["problem"].is_null());
        // A template URL with no update template next to it: no link.
        assert!(c.view(false, "https://b.s3.amazonaws.com/mine.yaml")["update"].is_null());
        // Round trip through storage.
        let back: PermissionCheck =
            serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
        assert_eq!(back, c);
        assert!(back.is_current());
    }

    #[test]
    fn a_role_that_cannot_be_taken_on_is_an_error() {
        let mut c = PermissionCheck::new(&target(), "t");
        c.error = "Coderhelm couldn't use the role in your AWS account (AccessDenied).".into();
        assert_eq!(c.status(), "error");
        assert_eq!(c.problem(false).unwrap().code, "role");
    }

    #[test]
    fn each_action_is_simulated_on_a_resource_its_statement_covers() {
        let t = target();
        assert_eq!(
            simulated_resource("logs:StartQuery", &t.role_arn, "eu-west-1", "111122223333"),
            "arn:aws:logs:eu-west-1:111122223333:log-group:coderhelm-permission-check"
        );
        assert!(simulated_resource(
            "airflow:InvokeRestApi",
            &t.role_arn,
            "eu-west-1",
            "111122223333"
        )
        .ends_with("/Viewer"));
        assert_eq!(
            simulated_resource("iam:SimulatePrincipalPolicy", &t.role_arn, "r", "a"),
            t.role_arn
        );
        assert_eq!(
            simulated_resource("ecs:ListTasks", &t.role_arn, "r", "a"),
            "*"
        );
    }
}
