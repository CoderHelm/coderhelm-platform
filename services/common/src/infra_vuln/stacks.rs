//! Which CloudFormation stack made a resource, asked of CloudFormation
//! itself: `DescribeStackResources` with the resource's physical id.
//!
//! CloudFormation does not put its `aws:cloudformation:*` tags on every
//! resource type, so the tags alone miss resources a stack made. This
//! lookup does not depend on tags. It returns the stack's name and the
//! resource's logical id and nothing else: no template, no parameters, no
//! outputs.
//!
//! It needs `cloudformation:DescribeStackResources` in the read role's
//! "Infrastructure vulnerabilities" permissions. A role without it gets
//! [`Owner::Denied`] once, the read notes the permission, and resources
//! are placed by their tags and by the code alone.

use crate::live::aws::Session;

/// What CloudFormation said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owner {
    /// A stack made it.
    Stack { stack: String, logical_id: String },
    /// No stack owns this physical id.
    None,
    /// The role may not ask (the permission is not granted yet).
    Denied(String),
    /// The call failed for another reason.
    Failed(String),
}

/// What an error from the call means.
pub fn read_error(code: Option<&str>, message: &str) -> Owner {
    let c = code.unwrap_or("");
    if c.contains("AccessDenied") || c == "UnauthorizedOperation" {
        return Owner::Denied(crate::truncate_str(message, 240).to_string());
    }
    // "Stack for <id> does not exist".
    if c == "ValidationError" && message.contains("does not exist") {
        return Owner::None;
    }
    Owner::Failed(format!("{c}: {}", crate::truncate_str(message, 200)))
}

/// The physical id CloudFormation knows a resource by, when it is one the
/// lookup works for: a database's identifier, a cache's replication group
/// id, a domain's name, an Auto Scaling group's name.
pub fn physical_id(r: &super::inventory::Resource) -> Option<String> {
    use super::inventory::Kind;
    match r.kind {
        Kind::RdsCluster
        | Kind::RdsInstance
        | Kind::ElasticacheGroup
        | Kind::OpensearchDomain
        | Kind::AutoScalingGroup => Some(r.name.clone()).filter(|n| !n.is_empty()),
        _ => None,
    }
}

/// Asks CloudFormation which stack owns `physical_id`.
pub async fn owner_of(s: &Session, physical_id: &str) -> Owner {
    use aws_sdk_cloudformation::config::{BehaviorVersion, Credentials, Region};
    use aws_sdk_cloudformation::error::ProvideErrorMetadata;
    let creds = Credentials::new(
        &s.access_key_id,
        &s.secret_access_key,
        Some(s.session_token.clone()),
        None,
        "coderhelm-team-role",
    );
    let cfn = aws_sdk_cloudformation::Client::from_conf(
        aws_sdk_cloudformation::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(s.region.clone()))
            .credentials_provider(creds)
            .build(),
    );
    match cfn
        .describe_stack_resources()
        .physical_resource_id(physical_id)
        .send()
        .await
    {
        Ok(o) => o
            .stack_resources()
            .iter()
            .find(|r| r.physical_resource_id() == Some(physical_id))
            .map(|r| Owner::Stack {
                stack: r.stack_name().unwrap_or("").to_string(),
                logical_id: r.logical_resource_id().unwrap_or("").to_string(),
            })
            .unwrap_or(Owner::None),
        Err(e) => match e.as_service_error() {
            Some(se) => read_error(se.code(), se.message().unwrap_or("")),
            None => Owner::Failed("AWS didn't answer".into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra_vuln::inventory::{Kind, Resource};

    #[test]
    fn a_refusal_names_itself_and_a_resource_no_stack_made_is_not_an_error() {
        assert!(matches!(
            read_error(
                Some("AccessDenied"),
                "User is not authorized to perform: cloudformation:DescribeStackResources"
            ),
            Owner::Denied(_)
        ));
        assert_eq!(
            read_error(
                Some("ValidationError"),
                "Stack for orders-prod does not exist"
            ),
            Owner::None
        );
        assert!(matches!(
            read_error(Some("Throttling"), "Rate exceeded"),
            Owner::Failed(_)
        ));
    }

    #[test]
    fn the_lookup_is_asked_only_for_kinds_whose_physical_id_is_their_name() {
        let r = |kind| Resource {
            kind,
            name: "orders".into(),
            ..Default::default()
        };
        assert_eq!(
            physical_id(&r(Kind::RdsInstance)).as_deref(),
            Some("orders")
        );
        assert_eq!(
            physical_id(&r(Kind::OpensearchDomain)).as_deref(),
            Some("orders")
        );
        assert_eq!(physical_id(&r(Kind::EksNodegroup)), None);
        assert_eq!(physical_id(&r(Kind::LambdaFunction)), None);
    }
}
