//! Which AWS account, role and region Coderhelm uses for a team, and whether
//! each works: the role is taken on, AWS is asked who we are, and (when
//! asked) each kind of read is tried once. Nothing is changed.

use serde::Serialize;

use super::aws::{assume, AwsTarget, Session};
use super::StepError;

/// What one role can do right now.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RoleCheck {
    /// What uses it: a data source's name, `CloudWatch metrics`, `Airflow`.
    pub used_for: Vec<String>,
    pub account: String,
    pub role: String,
    pub region: String,
    /// The role could be taken on and AWS answered who we are.
    pub works: bool,
    /// The session AWS reports (`arn:aws:sts::…:assumed-role/…`).
    pub caller: String,
    pub error: String,
    /// `None` when not tried.
    pub can_read_logs: Option<bool>,
    pub can_read_metrics: Option<bool>,
    pub can_read_airflow: Option<bool>,
    /// MWAA environments the role can see.
    pub airflow_environments: Vec<String>,
}

impl RoleCheck {
    /// Capabilities the role lacks among the ones tried.
    pub fn missing(&self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if self.can_read_metrics == Some(false) {
            out.push("metrics");
        }
        if self.can_read_airflow == Some(false) {
            out.push("airflow");
        }
        out
    }

    /// One line for an agent.
    pub fn line(&self) -> String {
        let mut s = format!(
            "- account {} · role {} · region {} · used for {}: ",
            self.account,
            self.role,
            self.region,
            self.used_for.join(", ")
        );
        if !self.works {
            s.push_str(&format!("NOT WORKING ({})", self.error));
            return s;
        }
        s.push_str("works");
        let cap = |name: &str, v: Option<bool>| match v {
            Some(true) => format!(", can read {name}"),
            Some(false) => format!(", cannot read {name}"),
            None => String::new(),
        };
        s.push_str(&cap("logs", self.can_read_logs));
        s.push_str(&cap("metrics", self.can_read_metrics));
        s.push_str(&cap("Airflow", self.can_read_airflow));
        if !self.airflow_environments.is_empty() {
            s.push_str(&format!(
                " (Airflow environments: {})",
                self.airflow_environments.join(", ")
            ));
        }
        s
    }
}

fn sts_client(s: &Session) -> aws_sdk_sts::Client {
    use aws_sdk_sts::config::{BehaviorVersion, Credentials, Region};
    let creds = Credentials::new(
        &s.access_key_id,
        &s.secret_access_key,
        Some(s.session_token.clone()),
        None,
        "coderhelm-team-role",
    );
    aws_sdk_sts::Client::from_conf(
        aws_sdk_sts::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(s.region.clone()))
            .credentials_provider(creds)
            .build(),
    )
}

/// Takes on the role and asks AWS who we are. With `probe`, also tries one
/// read of logs, metrics and Airflow.
pub async fn check(
    sts: &aws_sdk_sts::Client,
    target: &AwsTarget,
    used_for: Vec<String>,
    actor: &str,
    probe: bool,
) -> RoleCheck {
    let mut out = RoleCheck {
        used_for,
        account: target.account().to_string(),
        role: target.role_arn.rsplit('/').next().unwrap_or("").to_string(),
        region: target.region.clone(),
        ..Default::default()
    };
    let session = match assume(sts, target, actor).await {
        Ok(s) => s,
        Err(e) => {
            out.error = e.message;
            return out;
        }
    };
    match sts_client(&session).get_caller_identity().send().await {
        Ok(who) => {
            out.works = true;
            out.caller = who.arn().unwrap_or("").to_string();
        }
        Err(e) => {
            out.error = super::error_detail(
                e.code().unwrap_or(""),
                e.message().unwrap_or("AWS didn't say who we are"),
            );
            return out;
        }
    }
    if probe {
        out.can_read_logs = Some(
            super::aws::logs_client(&session)
                .describe_log_groups()
                .limit(1)
                .send()
                .await
                .is_ok(),
        );
        out.can_read_metrics = super::metrics::probe(&session).await.ok();
        match super::airflow::list_environments(&session).await {
            Ok(Some(envs)) => {
                out.can_read_airflow = Some(true);
                out.airflow_environments = envs;
            }
            Ok(None) => out.can_read_airflow = Some(false),
            Err(_) => out.can_read_airflow = None,
        }
    }
    out
}

use aws_sdk_sts::error::ProvideErrorMetadata;

/// A target for the team's log reader connection, from the saved
/// connection's role, region and private ID.
pub fn log_reader_target(
    role_arn: &str,
    region: &str,
    external_id: &str,
) -> Result<AwsTarget, StepError> {
    let t = AwsTarget {
        role_arn: role_arn.to_string(),
        external_id: external_id.to_string(),
        region: region.to_string(),
        ..Default::default()
    };
    super::aws::check_target(&t)?;
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_line_says_what_works() {
        let c = RoleCheck {
            used_for: vec!["CloudWatch metrics".into()],
            account: "123456789012".into(),
            role: "CoderHelmLogReader".into(),
            region: "us-east-1".into(),
            works: true,
            can_read_logs: Some(true),
            can_read_metrics: Some(false),
            can_read_airflow: Some(true),
            airflow_environments: vec!["prod".into()],
            ..Default::default()
        };
        let l = c.line();
        assert!(
            l.contains("works, can read logs, cannot read metrics, can read Airflow"),
            "{l}"
        );
        assert!(l.contains("prod"));
        assert_eq!(c.missing(), vec!["metrics"]);
        let broken = RoleCheck {
            error: "denied".into(),
            works: false,
            ..c
        };
        assert!(broken.line().contains("NOT WORKING (denied)"));
    }

    #[test]
    fn only_coderhelm_roles_make_a_target() {
        assert!(log_reader_target(
            "arn:aws:iam::123456789012:role/CoderHelmLogReader",
            "us-east-1",
            "x"
        )
        .is_ok());
        assert!(
            log_reader_target("arn:aws:iam::123456789012:role/Admin", "us-east-1", "x").is_err()
        );
        assert!(log_reader_target(
            "arn:aws:iam::123456789012:role/CoderHelmLogReader",
            "mars",
            "x"
        )
        .is_err());
    }
}
