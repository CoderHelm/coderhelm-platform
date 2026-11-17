//! Moving data sources off the script-made `CoderhelmReadOnly` role and
//! onto CoderHelm's role stack (`CoderHelmLogReader`), whose one template
//! declares every read (`infra/cfn/coderhelm-log-reader.yaml`).
//!
//! The older setup script made `CoderhelmReadOnly` and wrote its policies
//! by hand, so a release could never reach it. Nothing about a working
//! connection is changed until the stack's role is proven on it:
//!
//! 1. The account's update link names every database and warehouse the
//!    team's data sources read there ([`data_sources_in`]), so once an AWS
//!    admin creates the update stack the stack's role may query them
//!    (`aws_role_reads::role_update` says why: "N data sources still read
//!    through the script-made role").
//! 2. Each day the permissions sweep tries every such source with the
//!    stack's role ([`candidate`]): the same database, user, secret and
//!    log groups, only the role and the team's private ID of the account's
//!    connection. Only a check that passes end to end (the role is taken
//!    on, the read-only user signs in, tables are listed) switches the
//!    source ([`try_move`]); anything else leaves it on the old role and
//!    tries again the next day.
//! 3. The old role keeps working throughout. Once no source uses it, an
//!    admin may delete it; CoderHelm never changes or deletes it.

use crate::live::aws::{AwsTarget, LOG_READER_ROLE, ROLE_PREFIX};
use crate::live::aws_account::AwsAccount;
use crate::live::store::Source;

/// The target reads through a role the older script made.
pub fn is_legacy(t: &AwsTarget) -> bool {
    t.role_arn
        .rsplit('/')
        .next()
        .is_some_and(|n| n.starts_with(ROLE_PREFIX))
}

/// Pure: the same target on `account`'s stack role (its connection's role
/// and private ID), when `t` is a script-made role's target in that
/// account. `None` for anything else.
pub fn candidate(t: &AwsTarget, account: &AwsAccount) -> Option<AwsTarget> {
    let stack_role = account.role_arn.rsplit('/').next() == Some(LOG_READER_ROLE);
    (is_legacy(t)
        && stack_role
        && !account.external_id.is_empty()
        && t.account() == account.account)
        .then(|| AwsTarget {
            role_arn: account.role_arn.clone(),
            external_id: account.external_id.clone(),
            ..t.clone()
        })
}

/// Pure: the Aurora clusters (ARNs) and Redshift clusters (names) the
/// team's data sources read in `account`, sorted and once each: what the
/// role stack's DatabaseClusters and Warehouses must name.
pub fn data_sources_in(sources: &[Source], account: &str) -> (Vec<String>, Vec<String>) {
    let mut dbs: Vec<String> = Vec::new();
    let mut whs: Vec<String> = Vec::new();
    for t in sources.iter().filter_map(|s| s.aws.as_ref()) {
        if t.account() != account {
            continue;
        }
        if !t.cluster_arn.is_empty() && !dbs.contains(&t.cluster_arn) {
            dbs.push(t.cluster_arn.clone());
        }
        if !t.cluster_id.is_empty() && !whs.contains(&t.cluster_id) {
            whs.push(t.cluster_id.clone());
        }
    }
    dbs.sort();
    whs.sort();
    (dbs, whs)
}

/// Pure: how many of the team's data sources in `account` still read
/// through a script-made role.
pub fn legacy_in(sources: &[Source], account: &str) -> usize {
    sources
        .iter()
        .filter_map(|s| s.aws.as_ref())
        .filter(|t| t.account() == account && is_legacy(t))
        .count()
}

/// Proves `candidate` reads what `source` reads, end to end, and returns
/// it; or why not. Nothing in the account is changed.
pub async fn try_move(
    sts: &aws_sdk_sts::Client,
    source: &Source,
    candidate: &AwsTarget,
) -> Result<AwsTarget, String> {
    let session = crate::live::aws::assume_exact(sts, candidate, "role-move").await;
    if !candidate.cluster_id.is_empty() {
        let s = session.map_err(|e| e.message)?;
        let r = crate::live::redshift::connect_checks(&s, candidate).await;
        return if r.ok {
            Ok(candidate.clone())
        } else {
            Err(first_failure(r.checks.iter().map(|c| (c.ok, &c.detail))))
        };
    }
    if !candidate.cluster_arn.is_empty() {
        let r = crate::live::db::connect_checks(session, candidate).await;
        return if r.ok {
            Ok(candidate.clone())
        } else {
            Err(first_failure(r.checks.iter().map(|c| (c.ok, &c.detail))))
        };
    }
    // A logs source: the log groups it names must still be listed.
    let s = session.map_err(|e| e.message)?;
    let groups = crate::live::logs::list_groups(&s)
        .await
        .map_err(|e| e.message)?;
    let wanted = &source
        .aws
        .as_ref()
        .map(|t| t.log_groups.clone())
        .unwrap_or_default();
    let all_seen = wanted.iter().all(|g| {
        groups.iter().any(|lg| match g.strip_suffix('*') {
            Some(p) => lg.name.starts_with(p),
            None => lg.name == *g,
        })
    });
    if all_seen {
        Ok(candidate.clone())
    } else {
        Err("the stack's role doesn't list every log group the source reads".into())
    }
}

fn first_failure<'a>(checks: impl Iterator<Item = (bool, &'a String)>) -> String {
    checks
        .filter(|(ok, _)| !ok)
        .map(|(_, d)| d.clone())
        .next()
        .unwrap_or_else(|| "the check didn't pass".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account() -> AwsAccount {
        AwsAccount {
            account: "111122223333".into(),
            role_arn: "arn:aws:iam::111122223333:role/CoderHelmLogReader".into(),
            external_id: "0b5f1a5e-0000-4000-8000-000000000000".into(),
            region: "us-east-1".into(),
            ..Default::default()
        }
    }

    fn db() -> AwsTarget {
        AwsTarget {
            role_arn: "arn:aws:iam::111122223333:role/CoderhelmReadOnly".into(),
            external_id: "0b5f1a5e-0000-4000-8000-000000000000".into(),
            region: "us-east-1".into(),
            cluster_arn: "arn:aws:rds:us-east-1:111122223333:cluster:app-db".into(),
            secret_arn:
                "arn:aws:secretsmanager:us-east-1:111122223333:secret:coderhelm-readonly-app-db-AbCdEf"
                    .into(),
            database: "app".into(),
            ..Default::default()
        }
    }

    fn source(t: AwsTarget) -> Source {
        use crate::live::connectors::{ConnKind, Connector};
        let mut s = Source::from_connection(&Connector::new(ConnKind::Rest), "x").unwrap();
        s.aws = Some(t);
        s
    }

    #[test]
    fn a_script_made_source_moves_to_the_stack_role_with_the_same_database() {
        let c = candidate(&db(), &account()).expect("a candidate");
        assert_eq!(
            c.role_arn,
            "arn:aws:iam::111122223333:role/CoderHelmLogReader"
        );
        // Everything that names the data stays the same.
        assert_eq!(c.cluster_arn, db().cluster_arn);
        assert_eq!(c.secret_arn, db().secret_arn);
        assert_eq!(c.database, "app");
        assert!(crate::live::aws::check_target(&c).is_ok());
        // Only a script-made role, only in the account, only onto the stack's role.
        let on_stack = AwsTarget {
            role_arn: account().role_arn,
            ..db()
        };
        assert!(candidate(&on_stack, &account()).is_none());
        let elsewhere = AwsTarget {
            role_arn: "arn:aws:iam::444455556666:role/CoderhelmReadOnly".into(),
            ..db()
        };
        assert!(candidate(&elsewhere, &account()).is_none());
        let mut no_id = account();
        no_id.external_id.clear();
        assert!(candidate(&db(), &no_id).is_none());
    }

    #[test]
    fn the_update_link_names_every_database_and_warehouse_in_the_account() {
        let wh = AwsTarget {
            role_arn: "arn:aws:iam::111122223333:role/CoderhelmReadOnly".into(),
            region: "us-east-1".into(),
            engine: "redshift".into(),
            cluster_id: "analytics".into(),
            db_user: "coderhelm_readonly".into(),
            databases: vec!["dev".into()],
            ..Default::default()
        };
        let other = AwsTarget {
            cluster_arn: "arn:aws:rds:us-east-1:444455556666:cluster:x".into(),
            role_arn: "arn:aws:iam::444455556666:role/CoderhelmReadOnly".into(),
            ..db()
        };
        let sources = vec![source(db()), source(db()), source(wh), source(other)];
        let (dbs, whs) = data_sources_in(&sources, "111122223333");
        assert_eq!(dbs, vec![db().cluster_arn]);
        assert_eq!(whs, vec!["analytics".to_string()]);
        assert_eq!(legacy_in(&sources, "111122223333"), 3);
        let mut a = account();
        a.database_clusters = dbs;
        a.warehouses = whs;
        let u = crate::log_reader::update_url("https://b/x/y.yaml", "us-east-1", &a.chosen());
        assert!(u.contains("param_DatabaseClusters=") && u.contains("param_Warehouses=analytics"));
    }
}
