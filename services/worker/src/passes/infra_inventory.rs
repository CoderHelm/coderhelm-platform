//! A team's running AWS resources, read once a day through the optional
//! "Infrastructure vulnerabilities" permissions of its CoderHelmLogReader
//! role (`common::infra_vuln::inventory` has the model).
//!
//! For each connected account an admin switched the option on: one read
//! in the account's region (describe and list calls only), the resources
//! saved as `RES#<account>#<region>#<id>` in the AWS insights table for
//! [`KEEP_DAYS`](common::infra_vuln::inventory::KEEP_DAYS) days, and a
//! summary of the read (`RES_READ#<account>#<region>`) with any call AWS
//! refused, naming the permission.
//!
//! Where each resource is defined comes from its tags (a repository tag,
//! its CloudFormation stack) or a search of the team's infrastructure
//! repositories for its AMI, group or name. Lambda functions come from the
//! team's Terraform only: the role never reads function settings, which
//! hold environment variables.

use std::collections::{BTreeMap, BTreeSet};

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{Duration, Utc};
use common::infra_vuln::inventory::{self as inv, Resource};
use common::infra_vuln::raw::{
    RawAccount, RawAsg, RawCacheUpdate, RawDatabase, RawDomain, RawEksCluster, RawImage,
    RawInstance, RawLoadBalancer, RawNodegroup, RawPending, RawService,
};
use common::live::aws::Session;
use common::live::aws_account::{self, AwsAccount};
use tracing::{info, warn};

use crate::passes::{attr_n, attr_s};
use crate::WorkerState;

/// Pages read per list call (each up to 100 to 1,000 items).
const MAX_PAGES: usize = 10;
/// Repositories and files searched to place resources.
const MAX_REPOS: usize = 8;
const MAX_FILES: usize = 40;
/// Prefix of a read's summary row.
pub const READ_PREFIX: &str = "RES_READ#";

macro_rules! client {
    ($krate:ident, $s:expr) => {{
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
                .region(Region::new(s.region.clone()))
                .credentials_provider(creds)
                .build(),
        )
    }};
}

#[derive(Debug, Default)]
pub struct InventoryReport {
    pub accounts: usize,
    pub resources: usize,
}

fn tags_of<'a>(it: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<(String, String)> {
    it.into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

macro_rules! note {
    ($raw:expr, $call:expr, $e:expr) => {{
        let e = &$e;
        let w = match e.as_service_error() {
            Some(se) => {
                use aws_sdk_dynamodb::error::ProvideErrorMetadata;
                format!(
                    "{}: {}",
                    se.code().unwrap_or("error"),
                    se.message().unwrap_or("")
                )
            }
            None => "AWS didn't answer".to_string(),
        };
        $raw.errors
            .push(($call.to_string(), common::truncate_str(&w, 240).to_string()));
    }};
}

async fn read_ec2(s: &Session, raw: &mut RawAccount) {
    let ec2 = client!(aws_sdk_ec2, s);
    let mut token: Option<String> = None;
    let running = aws_sdk_ec2::types::Filter::builder()
        .name("instance-state-name")
        .values("running")
        .build();
    for _ in 0..MAX_PAGES {
        match ec2
            .describe_instances()
            .filters(running.clone())
            .set_next_token(token.take())
            .max_results(500)
            .send()
            .await
        {
            Ok(o) => {
                for r in o.reservations() {
                    for i in r.instances() {
                        raw.instances.push(RawInstance {
                            id: i.instance_id().unwrap_or("").into(),
                            image_id: i.image_id().unwrap_or("").into(),
                            launch_time: i.launch_time().map(|t| t.to_string()).unwrap_or_default(),
                            public_ip: i.public_ip_address().is_some_and(|a| !a.is_empty()),
                            tags: tags_of(
                                i.tags()
                                    .iter()
                                    .map(|t| (t.key().unwrap_or(""), t.value().unwrap_or(""))),
                            ),
                        });
                    }
                }
                token = o.next_token().map(String::from);
                if token.is_none() {
                    break;
                }
            }
            Err(e) => {
                note!(raw, "DescribeInstances", e);
                return;
            }
        }
    }
    let ids: BTreeSet<String> = raw
        .instances
        .iter()
        .map(|i| i.image_id.clone())
        .filter(|i| !i.is_empty())
        .collect();
    for chunk in ids.into_iter().collect::<Vec<_>>().chunks(100) {
        match ec2
            .describe_images()
            .set_image_ids(Some(chunk.to_vec()))
            .include_deprecated(true)
            .send()
            .await
        {
            Ok(o) => raw.images.extend(o.images().iter().map(|m| RawImage {
                id: m.image_id().unwrap_or("").into(),
                name: m.name().unwrap_or("").into(),
                creation_date: m.creation_date().unwrap_or("").into(),
            })),
            Err(e) => note!(raw, "DescribeImages", e),
        }
    }
}

async fn read_asgs(s: &Session, raw: &mut RawAccount) {
    let asg = client!(aws_sdk_autoscaling, s);
    let mut token: Option<String> = None;
    for _ in 0..MAX_PAGES {
        match asg
            .describe_auto_scaling_groups()
            .set_next_token(token.take())
            .max_records(100)
            .send()
            .await
        {
            Ok(o) => {
                for g in o.auto_scaling_groups() {
                    let lt = g.launch_template().or_else(|| {
                        g.mixed_instances_policy()
                            .and_then(|m| m.launch_template())
                            .and_then(|l| l.launch_template_specification())
                    });
                    raw.asgs.push(RawAsg {
                        name: g.auto_scaling_group_name().unwrap_or("").into(),
                        arn: g.auto_scaling_group_arn().unwrap_or("").into(),
                        launch_template_id: lt
                            .and_then(|l| l.launch_template_id())
                            .unwrap_or("")
                            .into(),
                        launch_template_name: lt
                            .and_then(|l| l.launch_template_name())
                            .unwrap_or("")
                            .into(),
                        target_groups: g.target_group_arns().to_vec(),
                        desired: g.desired_capacity().unwrap_or(0).max(0) as u32,
                        tags: tags_of(
                            g.tags()
                                .iter()
                                .map(|t| (t.key().unwrap_or(""), t.value().unwrap_or(""))),
                        ),
                    });
                }
                token = o.next_token().map(String::from);
                if token.is_none() {
                    break;
                }
            }
            Err(e) => {
                note!(raw, "DescribeAutoScalingGroups", e);
                return;
            }
        }
    }
}

async fn read_lbs(s: &Session, raw: &mut RawAccount) {
    let tgs: BTreeSet<String> = raw
        .asgs
        .iter()
        .flat_map(|g| g.target_groups.clone())
        .chain(raw.services.iter().flat_map(|s| s.target_groups.clone()))
        .collect();
    if tgs.is_empty() {
        return;
    }
    let elb = client!(aws_sdk_elasticloadbalancingv2, s);
    let mut by_lb: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for chunk in tgs.into_iter().collect::<Vec<_>>().chunks(20) {
        match elb
            .describe_target_groups()
            .set_target_group_arns(Some(chunk.to_vec()))
            .send()
            .await
        {
            Ok(o) => {
                for t in o.target_groups() {
                    for lb in t.load_balancer_arns() {
                        by_lb
                            .entry(lb.clone())
                            .or_default()
                            .push(t.target_group_arn().unwrap_or("").into());
                    }
                }
            }
            Err(e) => {
                note!(raw, "DescribeTargetGroups", e);
                return;
            }
        }
    }
    for chunk in by_lb.keys().cloned().collect::<Vec<_>>().chunks(20) {
        match elb
            .describe_load_balancers()
            .set_load_balancer_arns(Some(chunk.to_vec()))
            .send()
            .await
        {
            Ok(o) => {
                for l in o.load_balancers() {
                    let arn = l.load_balancer_arn().unwrap_or("").to_string();
                    raw.load_balancers.push(RawLoadBalancer {
                        name: l.load_balancer_name().unwrap_or("").into(),
                        scheme: l
                            .scheme()
                            .map(|x| x.as_str().to_string())
                            .unwrap_or_default(),
                        target_groups: by_lb.get(&arn).cloned().unwrap_or_default(),
                        arn,
                    });
                }
            }
            Err(e) => note!(raw, "DescribeLoadBalancers", e),
        }
    }
}

async fn read_eks(s: &Session, raw: &mut RawAccount) {
    let eks = client!(aws_sdk_eks, s);
    let clusters = match eks.list_clusters().max_results(100).send().await {
        Ok(o) => o.clusters().to_vec(),
        Err(e) => {
            note!(raw, "ListClusters (EKS)", e);
            return;
        }
    };
    for name in clusters.iter().take(50) {
        match eks.describe_cluster().name(name).send().await {
            Ok(o) => {
                if let Some(c) = o.cluster() {
                    raw.clusters.push(RawEksCluster {
                        name: name.clone(),
                        arn: c.arn().unwrap_or("").into(),
                        version: c.version().unwrap_or("").into(),
                        platform_version: c.platform_version().unwrap_or("").into(),
                        public_endpoint: c
                            .resources_vpc_config()
                            .is_some_and(|v| v.endpoint_public_access()),
                        auto_mode: c
                            .compute_config()
                            .and_then(|x| x.enabled())
                            .unwrap_or(false),
                        tags: c
                            .tags()
                            .map(|t| tags_of(t.iter().map(|(k, v)| (k.as_str(), v.as_str()))))
                            .unwrap_or_default(),
                    });
                }
            }
            Err(e) => note!(raw, "DescribeCluster", e),
        }
        let groups = match eks
            .list_nodegroups()
            .cluster_name(name)
            .max_results(100)
            .send()
            .await
        {
            Ok(o) => o.nodegroups().to_vec(),
            Err(e) => {
                note!(raw, "ListNodegroups", e);
                continue;
            }
        };
        for g in groups.iter().take(50) {
            match eks
                .describe_nodegroup()
                .cluster_name(name)
                .nodegroup_name(g)
                .send()
                .await
            {
                Ok(o) => {
                    if let Some(n) = o.nodegroup() {
                        raw.nodegroups.push(RawNodegroup {
                            cluster: name.clone(),
                            name: g.clone(),
                            arn: n.nodegroup_arn().unwrap_or("").into(),
                            release_version: n.release_version().unwrap_or("").into(),
                            version: n.version().unwrap_or("").into(),
                            ami_type: n
                                .ami_type()
                                .map(|a| a.as_str().to_string())
                                .unwrap_or_default(),
                            desired: n
                                .scaling_config()
                                .and_then(|c| c.desired_size())
                                .unwrap_or(0)
                                .max(0) as u32,
                            launch_template_id: n
                                .launch_template()
                                .and_then(|l| l.id())
                                .unwrap_or("")
                                .into(),
                            tags: n
                                .tags()
                                .map(|t| tags_of(t.iter().map(|(k, v)| (k.as_str(), v.as_str()))))
                                .unwrap_or_default(),
                        });
                    }
                }
                Err(e) => note!(raw, "DescribeNodegroup", e),
            }
        }
    }
}

/// ECS services on Fargate, with the existing ECS permissions (images and
/// start times; the task definition's environment is never read).
async fn read_ecs(s: &Session, raw: &mut RawAccount) {
    let ecs = client!(aws_sdk_ecs, s);
    let Ok(clusters) = ecs.list_clusters().max_results(100).send().await else {
        return;
    };
    for c in clusters.cluster_arns().iter().take(20) {
        let Ok(svcs) = ecs.list_services().cluster(c).max_results(100).send().await else {
            continue;
        };
        for chunk in svcs.service_arns().chunks(10).take(5) {
            let Ok(d) = ecs
                .describe_services()
                .cluster(c)
                .set_services(Some(chunk.to_vec()))
                .send()
                .await
            else {
                continue;
            };
            for sv in d.services() {
                let launch = sv
                    .launch_type()
                    .map(|l| l.as_str().to_string())
                    .unwrap_or_else(|| {
                        if sv
                            .capacity_provider_strategy()
                            .iter()
                            .any(|p| p.capacity_provider().starts_with("FARGATE"))
                        {
                            "FARGATE".into()
                        } else {
                            String::new()
                        }
                    });
                let mut service = RawService {
                    cluster: c.rsplit('/').next().unwrap_or(c).to_string(),
                    name: sv.service_name().unwrap_or("").into(),
                    arn: sv.service_arn().unwrap_or("").into(),
                    launch_type: launch,
                    platform_version: sv.platform_version().unwrap_or("").into(),
                    public_ip: sv
                        .network_configuration()
                        .and_then(|n| n.awsvpc_configuration())
                        .and_then(|a| a.assign_public_ip())
                        .is_some_and(|p| p.as_str() == "ENABLED"),
                    target_groups: sv
                        .load_balancers()
                        .iter()
                        .filter_map(|l| l.target_group_arn().map(String::from))
                        .collect(),
                    tasks: sv.running_count().max(0) as u32,
                    ..Default::default()
                };
                if service.launch_type == "FARGATE" {
                    if let Ok(t) = ecs
                        .list_tasks()
                        .cluster(c)
                        .service_name(&service.name)
                        .max_results(100)
                        .send()
                        .await
                    {
                        if !t.task_arns().is_empty() {
                            if let Ok(dt) = ecs
                                .describe_tasks()
                                .cluster(c)
                                .set_tasks(Some(t.task_arns().iter().take(100).cloned().collect()))
                                .send()
                                .await
                            {
                                let mut oldest: Option<String> = None;
                                for task in dt.tasks() {
                                    if let Some(st) = task.started_at().map(|d| {
                                        d.fmt(aws_smithy_types::date_time::Format::DateTime)
                                            .unwrap_or_default()
                                    }) {
                                        if oldest.as_ref().is_none_or(|o| st < *o) {
                                            oldest = Some(st);
                                        }
                                    }
                                    for ct in task.containers() {
                                        let pair = (
                                            ct.image().unwrap_or("").to_string(),
                                            ct.image_digest().unwrap_or("").to_string(),
                                        );
                                        if !pair.0.is_empty() && !service.images.contains(&pair) {
                                            service.images.push(pair);
                                        }
                                    }
                                }
                                service.oldest_task_started = oldest.unwrap_or_default();
                            }
                        }
                    }
                }
                raw.services.push(service);
            }
        }
    }
}

async fn read_rds(s: &Session, raw: &mut RawAccount) {
    let rds = client!(aws_sdk_rds, s);
    match rds.describe_db_clusters().max_records(100).send().await {
        Ok(o) => raw.db_clusters.extend(o.db_clusters().iter().map(|c| {
            RawDatabase {
                id: c.db_cluster_identifier().unwrap_or("").into(),
                arn: c.db_cluster_arn().unwrap_or("").into(),
                engine: c.engine().unwrap_or("").into(),
                engine_version: c.engine_version().unwrap_or("").into(),
                public: c.publicly_accessible().unwrap_or(false),
                cluster: String::new(),
                tags: tags_of(
                    c.tag_list()
                        .iter()
                        .map(|t| (t.key().unwrap_or(""), t.value().unwrap_or(""))),
                ),
            }
        })),
        Err(e) => note!(raw, "DescribeDBClusters", e),
    }
    match rds.describe_db_instances().max_records(100).send().await {
        Ok(o) => raw.db_instances.extend(o.db_instances().iter().map(|d| {
            RawDatabase {
                id: d.db_instance_identifier().unwrap_or("").into(),
                arn: d.db_instance_arn().unwrap_or("").into(),
                engine: d.engine().unwrap_or("").into(),
                engine_version: d.engine_version().unwrap_or("").into(),
                public: d.publicly_accessible().unwrap_or(false),
                cluster: d.db_cluster_identifier().unwrap_or("").into(),
                tags: tags_of(
                    d.tag_list()
                        .iter()
                        .map(|t| (t.key().unwrap_or(""), t.value().unwrap_or(""))),
                ),
            }
        })),
        Err(e) => note!(raw, "DescribeDBInstances", e),
    }
    match rds
        .describe_pending_maintenance_actions()
        .max_records(100)
        .send()
        .await
    {
        Ok(o) => {
            for r in o.pending_maintenance_actions() {
                for a in r.pending_maintenance_action_details() {
                    let d = |t: Option<&aws_smithy_types::DateTime>| {
                        t.and_then(|x| x.fmt(aws_smithy_types::date_time::Format::DateTime).ok())
                            .unwrap_or_default()
                    };
                    raw.pending.push(RawPending {
                        resource_arn: r.resource_identifier().unwrap_or("").into(),
                        action: a.action().unwrap_or("").into(),
                        description: a.description().unwrap_or("").into(),
                        auto_applied_after: d(a.auto_applied_after_date()),
                        forced_apply: d(a.forced_apply_date()),
                        current_apply: d(a.current_apply_date()),
                    });
                }
            }
        }
        Err(e) => note!(raw, "DescribePendingMaintenanceActions", e),
    }
}

async fn read_elasticache(s: &Session, raw: &mut RawAccount) {
    let ec = client!(aws_sdk_elasticache, s);
    let mut severity: BTreeMap<String, (String, String, String)> = BTreeMap::new();
    match ec
        .describe_service_updates()
        .service_update_status(aws_sdk_elasticache::types::ServiceUpdateStatus::Available)
        .max_records(100)
        .send()
        .await
    {
        Ok(o) => {
            for u in o.service_updates() {
                severity.insert(
                    u.service_update_name().unwrap_or("").into(),
                    (
                        u.service_update_severity()
                            .map(|x| x.as_str().to_string())
                            .unwrap_or_default(),
                        u.service_update_type()
                            .map(|x| x.as_str().to_string())
                            .unwrap_or_default(),
                        u.service_update_recommended_apply_by_date()
                            .and_then(|x| x.fmt(aws_smithy_types::date_time::Format::DateTime).ok())
                            .unwrap_or_default(),
                    ),
                );
            }
        }
        Err(e) => {
            note!(raw, "DescribeServiceUpdates", e);
            return;
        }
    }
    if severity.is_empty() {
        return;
    }
    match ec
        .describe_update_actions()
        .service_update_status(aws_sdk_elasticache::types::ServiceUpdateStatus::Available)
        .max_records(100)
        .send()
        .await
    {
        Ok(o) => {
            for a in o.update_actions() {
                let name = a.service_update_name().unwrap_or("").to_string();
                let (sev, ty, by) = severity.get(&name).cloned().unwrap_or_default();
                raw.cache_updates.push(RawCacheUpdate {
                    group_id: a
                        .replication_group_id()
                        .or(a.cache_cluster_id())
                        .unwrap_or("")
                        .into(),
                    status: a
                        .update_action_status()
                        .map(|x| x.as_str().to_string())
                        .unwrap_or_default(),
                    severity: sev,
                    update_type: ty,
                    apply_by: a
                        .service_update_recommended_apply_by_date()
                        .and_then(|x| x.fmt(aws_smithy_types::date_time::Format::DateTime).ok())
                        .unwrap_or(by),
                    engine: a.engine().unwrap_or("").into(),
                    engine_version: String::new(),
                    service_update: name,
                });
            }
        }
        Err(e) => note!(raw, "DescribeUpdateActions", e),
    }
}

async fn read_opensearch(s: &Session, raw: &mut RawAccount) {
    let os = client!(aws_sdk_opensearch, s);
    let names: Vec<String> = match os.list_domain_names().send().await {
        Ok(o) => o
            .domain_names()
            .iter()
            .filter_map(|d| d.domain_name().map(String::from))
            .collect(),
        Err(e) => {
            note!(raw, "ListDomainNames", e);
            return;
        }
    };
    for chunk in names.chunks(5) {
        match os
            .describe_domains()
            .set_domain_names(Some(chunk.to_vec()))
            .send()
            .await
        {
            Ok(o) => {
                for d in o.domain_status_list() {
                    let sw = d.service_software_options();
                    raw.domains.push(RawDomain {
                        name: d.domain_name().to_string(),
                        arn: d.arn().to_string(),
                        engine_version: d.engine_version().unwrap_or("").into(),
                        current: sw.and_then(|x| x.current_version()).unwrap_or("").into(),
                        new: sw.and_then(|x| x.new_version()).unwrap_or("").into(),
                        update_available: sw.and_then(|x| x.update_available()).unwrap_or(false),
                        required: sw
                            .and_then(|x| x.optional_deployment())
                            .map(|o| !o)
                            .unwrap_or(false),
                        automated_update_date: sw
                            .and_then(|x| x.automated_update_date())
                            .and_then(|x| x.fmt(aws_smithy_types::date_time::Format::DateTime).ok())
                            .unwrap_or_default(),
                        public: d.vpc_options().is_none(),
                    });
                }
            }
            Err(e) => note!(raw, "DescribeDomains", e),
        }
    }
}

/// Kernels of SSM-managed instances, and AWS's latest AL2023 AMIs here.
async fn read_ssm(s: &Session, raw: &mut RawAccount) {
    let ssm = client!(aws_sdk_ssm, s);
    let names: Vec<String> = common::infra_vuln::feeds::AL2023_PARAMETERS
        .iter()
        .map(|(_, p)| p.to_string())
        .collect();
    match ssm.get_parameters().set_names(Some(names)).send().await {
        Ok(o) => {
            for p in o.parameters() {
                if let Some((arch, _)) = common::infra_vuln::feeds::AL2023_PARAMETERS
                    .iter()
                    .find(|(_, n)| Some(*n) == p.name())
                {
                    raw.latest_al2023
                        .insert(arch.to_string(), p.value().unwrap_or("").to_string());
                }
            }
        }
        Err(e) => note!(raw, "GetParameters", e),
    }
    let ids: Vec<String> = raw
        .instances
        .iter()
        .map(|i| i.id.clone())
        .take(25)
        .collect();
    for id in ids {
        let f = aws_sdk_ssm::types::InventoryFilter::builder()
            .key("AWS:Application.Name")
            .values("kernel")
            .r#type(aws_sdk_ssm::types::InventoryQueryOperatorType::Equal)
            .build();
        let Ok(f) = f else {
            continue;
        };
        match ssm
            .list_inventory_entries()
            .instance_id(&id)
            .type_name("AWS:Application")
            .filters(f)
            .max_results(10)
            .send()
            .await
        {
            Ok(o) => {
                // The newest kernel package is the one that boots after a reboot;
                // the running one may be older until then.
                let mut best = String::new();
                for e in o.entries() {
                    let v = format!(
                        "{}-{}",
                        e.get("Version").map(String::as_str).unwrap_or(""),
                        e.get("Release").map(String::as_str).unwrap_or("")
                    );
                    if v > best {
                        best = v;
                    }
                }
                if !best.is_empty() && best != "-" {
                    raw.kernels.push((id, best));
                }
            }
            // Instances not managed by SSM say so; that's not a missing permission.
            Err(e) => {
                if e.as_service_error().is_some_and(|x| {
                    use aws_sdk_dynamodb::error::ProvideErrorMetadata;
                    x.code() == Some("AccessDeniedException")
                }) {
                    note!(raw, "ListInventoryEntries", e);
                    break;
                }
            }
        }
    }
}

async fn read_scans(s: &Session, raw: &mut RawAccount) {
    let ecr = client!(aws_sdk_ecr, s);
    let mut seen = BTreeSet::new();
    let images: Vec<(String, String)> =
        raw.services.iter().flat_map(|s| s.images.clone()).collect();
    for (image, digest) in images.into_iter().take(30) {
        // Only images in this account's ECR: `<acct>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>`.
        let Some((host, rest)) = image.split_once('/') else {
            continue;
        };
        if !host.contains(".dkr.ecr.") || digest.is_empty() || !seen.insert(digest.clone()) {
            continue;
        }
        let repo = rest.split([':', '@']).next().unwrap_or(rest).to_string();
        let id = aws_sdk_ecr::types::ImageIdentifier::builder()
            .image_digest(&digest)
            .build();
        match ecr
            .describe_image_scan_findings()
            .repository_name(&repo)
            .image_id(id)
            .max_results(100)
            .send()
            .await
        {
            Ok(o) => {
                let f = o.image_scan_findings();
                let counts = f.and_then(|x| x.finding_severity_counts());
                let n = |k: &str| {
                    counts
                        .and_then(|c| c.iter().find(|(s, _)| s.as_str() == k).map(|(_, v)| *v))
                        .unwrap_or(0)
                        .max(0) as u32
                };
                let mut cves: Vec<String> = f
                    .map(|x| {
                        x.findings()
                            .iter()
                            .filter_map(|y| y.name().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                cves.extend(
                    f.map(|x| {
                        x.enhanced_findings()
                            .iter()
                            .filter_map(|y| {
                                y.package_vulnerability_details()
                                    .and_then(|p| p.vulnerability_id())
                                    .map(String::from)
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
                );
                cves.sort();
                cves.dedup();
                cves.truncate(50);
                raw.scans.push(common::infra_vuln::raw::RawScan {
                    repository: repo,
                    digest,
                    critical: n("CRITICAL"),
                    high: n("HIGH"),
                    cves,
                });
            }
            Err(e) => {
                let refused = e.as_service_error().is_some_and(|x| {
                    use aws_sdk_dynamodb::error::ProvideErrorMetadata;
                    x.code() == Some("AccessDeniedException")
                });
                if refused {
                    note!(raw, "DescribeImageScanFindings", e);
                    break;
                }
            }
        }
    }
}

/// Reads the account (describe and list calls only).
pub async fn read(s: &Session, account: &str) -> RawAccount {
    let mut raw = RawAccount {
        account: account.to_string(),
        region: s.region.clone(),
        read_at: Utc::now().to_rfc3339(),
        ..Default::default()
    };
    read_ec2(s, &mut raw).await;
    read_asgs(s, &mut raw).await;
    read_eks(s, &mut raw).await;
    read_ecs(s, &mut raw).await;
    read_lbs(s, &mut raw).await;
    read_rds(s, &mut raw).await;
    read_elasticache(s, &mut raw).await;
    read_opensearch(s, &mut raw).await;
    read_ssm(s, &mut raw).await;
    read_scans(s, &mut raw).await;
    raw
}

/// Places each resource in the team's code, and adds the Lambda functions
/// its Terraform defines.
async fn place(
    state: &WorkerState,
    team_id: &str,
    account: &str,
    region: &str,
    resources: &mut Vec<Resource>,
) {
    let Some(iid) = super::review_reminder::team_installation_id(state, team_id).await else {
        return;
    };
    let Ok(gh) = crate::clients::github::GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        iid,
        &state.http,
    ) else {
        return;
    };
    let repos: Vec<String> = super::infra_repo::team_repos(state, team_id)
        .await
        .into_iter()
        .filter(|r| r.enabled)
        .map(|r| r.full_name)
        .take(MAX_REPOS)
        .collect();
    let mut files: Vec<(String, String, String)> = Vec::new();
    let mut fetched: BTreeSet<(String, String)> = BTreeSet::new();
    let mut wanted: Vec<(String, String)> = Vec::new();
    let mut terms: Vec<String> = resources.iter().flat_map(inv::search_terms).collect();
    terms.push("aws_lambda_function".into());
    terms.sort();
    terms.dedup();
    for repo in &repos {
        let Some((owner, name)) = repo.split_once('/') else {
            continue;
        };
        for term in terms.iter().take(30) {
            if let Ok(hits) = gh.search_code(owner, name, "HEAD", term).await {
                for h in hits
                    .into_iter()
                    .filter(|h| super::infra_repo::is_iac_path(&h.path))
                {
                    wanted.push((repo.clone(), h.path));
                }
            }
        }
    }
    for (repo, path) in wanted {
        if files.len() >= MAX_FILES || !fetched.insert((repo.clone(), path.clone())) {
            continue;
        }
        let Some((owner, name)) = repo.split_once('/') else {
            continue;
        };
        if let Ok(c) = gh.get_file_content(owner, name, &path).await {
            files.push((repo, path, c));
        }
    }
    for r in resources.iter_mut() {
        if r.defined_in.is_none() {
            r.defined_in = inv::locate(r, &repos, &files);
        }
    }
    let now = Utc::now().to_rfc3339();
    for (repo, path, content) in &files {
        if path.ends_with(".tf") {
            resources.extend(inv::lambdas_from_terraform(
                repo, path, content, account, region, &now,
            ));
        }
    }
}

async fn save(
    state: &WorkerState,
    team_id: &str,
    raw: &RawAccount,
    resources: &[Resource],
) -> usize {
    let ttl = (Utc::now() + Duration::days(inv::KEEP_DAYS)).timestamp();
    let mut n = 0;
    for r in resources {
        let Ok(j) = serde_json::to_string(r) else {
            continue;
        };
        let ok = state
            .dynamo
            .put_item()
            .table_name(&state.config.aws_insights_table_name)
            .item("pk", attr_s(team_id))
            .item("sk", attr_s(&r.sk()))
            .item("json", attr_s(&j))
            .item("kind", attr_s(&format!("{:?}", r.kind)))
            .item("ttl", attr_n(ttl))
            .send()
            .await;
        match ok {
            Ok(_) => n += 1,
            Err(e) => warn!(team_id, error = %e, "infra inventory: a resource could not be saved"),
        }
    }
    let summary = serde_json::json!({
        "account": raw.account,
        "region": raw.region,
        "read_at": raw.read_at,
        "resources": n,
        "errors": raw.errors.iter().map(|(c, w)| serde_json::json!({ "call": c, "why": w, "permission": common::infra_vuln::raw::CALLS.iter().find(|(x, _)| x == c).map(|(_, p)| *p) })).collect::<Vec<_>>(),
        "latest_al2023": raw.latest_al2023,
    });
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.aws_insights_table_name)
        .item("pk", attr_s(team_id))
        .item(
            "sk",
            attr_s(&format!("{READ_PREFIX}{}#{}", raw.account, raw.region)),
        )
        .item("json", attr_s(&summary.to_string()))
        .item("ttl", attr_n((Utc::now() + Duration::days(30)).timestamp()))
        .send()
        .await;
    n
}

/// Every connected account an admin switched the option on for, by team.
async fn accounts(state: &WorkerState) -> Vec<(String, AwsAccount)> {
    let mut out = Vec::new();
    let mut start = None;
    for _ in 0..50 {
        let Ok(page) = state
            .dynamo
            .scan()
            .table_name(&state.config.aws_insights_table_name)
            .filter_expression("begins_with(sk, :p) AND infra_on = :on")
            .expression_attribute_values(":p", AttributeValue::S(aws_account::PREFIX.into()))
            .expression_attribute_values(":on", AttributeValue::Bool(true))
            .set_exclusive_start_key(start)
            .send()
            .await
        else {
            break;
        };
        for item in page.items() {
            let team = item
                .get("pk")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            if let (false, Some(a)) = (team.is_empty(), AwsAccount::from_item(item)) {
                out.push((team, a));
            }
        }
        start = page.last_evaluated_key().cloned();
        if start.is_none() {
            break;
        }
    }
    out
}

/// One read now, for one account (a person's "check now", or the sweep).
pub async fn read_account(
    state: &WorkerState,
    team_id: &str,
    a: &AwsAccount,
) -> Result<usize, String> {
    let session = common::live::aws::assume(&state.sts, &a.target(), "infra-inventory")
        .await
        .map_err(|e| e.message)?;
    let raw = read(&session, &a.account).await;
    let mut resources = raw.resources();
    place(state, team_id, &a.account, &a.region, &mut resources).await;
    Ok(save(state, team_id, &raw, &resources).await)
}

/// The daily read, on the review-reminder tick: each account once a day.
pub async fn sweep(state: &WorkerState) -> InventoryReport {
    let mut report = InventoryReport::default();
    let day = Utc::now().format("%Y%m%d").to_string();
    for (team, a) in accounts(state).await {
        if !a.infra_on() {
            continue;
        }
        let won = common::claim::claim(
            &state.dynamo,
            &state.config.settings_table_name,
            &team,
            &format!("INFRA_INVENTORY#{}#{day}", a.account),
            2 * 86_400,
        )
        .await;
        if !matches!(won, common::claim::Claim::Won) {
            continue;
        }
        match read_account(state, &team, &a).await {
            Ok(n) => {
                report.accounts += 1;
                report.resources += n;
            }
            Err(e) => {
                warn!(team_id = %team, account = %a.account, error = %e, "infra inventory: the role couldn't be taken on")
            }
        }
    }
    info!(
        accounts = report.accounts,
        resources = report.resources,
        "Infrastructure inventory read"
    );
    report
}
