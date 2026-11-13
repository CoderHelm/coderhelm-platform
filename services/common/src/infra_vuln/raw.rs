//! What one read of an AWS account returned, before it becomes resources:
//! only the fields CoderHelm keeps (`worker::passes::infra_inventory` fills
//! it from the AWS SDK; tests fill it from recorded responses). Nothing
//! here can hold environment variables or user data: the reads that carry
//! them aren't made, and these types have no field for them.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::actions::{self, Line, Meaning};
use super::inventory::{self as inv, AmiInfo, Exposure, Kind, Pending, Resource, Versions};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawInstance {
    pub id: String,
    pub image_id: String,
    pub launch_time: String,
    /// Has a public IPv4 address (the address itself isn't kept).
    pub public_ip: bool,
    pub tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawImage {
    pub id: String,
    pub name: String,
    pub creation_date: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawAsg {
    pub name: String,
    pub arn: String,
    pub launch_template_id: String,
    pub launch_template_name: String,
    pub target_groups: Vec<String>,
    pub desired: u32,
    pub tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawLoadBalancer {
    pub arn: String,
    pub name: String,
    /// `internet-facing` or `internal`.
    pub scheme: String,
    /// The target groups that send to it.
    pub target_groups: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawEksCluster {
    pub name: String,
    pub arn: String,
    pub version: String,
    pub platform_version: String,
    pub public_endpoint: bool,
    /// EKS Auto Mode (AWS runs and replaces the nodes).
    pub auto_mode: bool,
    pub tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawNodegroup {
    pub cluster: String,
    pub name: String,
    pub arn: String,
    /// `1.31.0-20261015` (AL2023 and AL2) or `1.67.0-...` (Bottlerocket).
    pub release_version: String,
    pub version: String,
    pub ami_type: String,
    pub desired: u32,
    /// A custom launch template (its AMI is the team's, not EKS's).
    pub launch_template_id: String,
    pub tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawService {
    pub cluster: String,
    pub name: String,
    pub arn: String,
    /// `FARGATE` or `EC2`.
    pub launch_type: String,
    pub platform_version: String,
    pub public_ip: bool,
    pub target_groups: Vec<String>,
    pub tasks: u32,
    pub oldest_task_started: String,
    /// `(image, digest)` of each container that runs.
    pub images: Vec<(String, String)>,
    pub tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawDatabase {
    pub id: String,
    pub arn: String,
    pub engine: String,
    pub engine_version: String,
    pub public: bool,
    /// An instance of this cluster (its own record is the cluster's).
    pub cluster: String,
    pub tags: Vec<(String, String)>,
    /// An instance that is its cluster's writer.
    pub writer: bool,
    /// Settings that tell it apart in code and say how an update lands
    /// (`subnet_group`, `parameter_group`, `instance_class`, `multi_az`,
    /// `auto_minor`, `maintenance_window`, `backup_retention`, `replicas`,
    /// `global`). Never credentials.
    pub facts: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawPending {
    pub resource_arn: String,
    /// `system-update`, `db-upgrade`, `hardware-maintenance`, ...
    pub action: String,
    pub description: String,
    pub auto_applied_after: String,
    pub forced_apply: String,
    pub current_apply: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawCacheUpdate {
    pub group_id: String,
    pub service_update: String,
    /// `not-applied`, `waiting-to-start`, `in-progress`, `complete`, ...
    pub status: String,
    pub severity: String,
    pub update_type: String,
    pub apply_by: String,
    pub engine: String,
    pub engine_version: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawDomain {
    pub name: String,
    pub arn: String,
    pub engine_version: String,
    pub current: String,
    pub new: String,
    pub update_available: bool,
    /// The update is required (not optional): AWS isolates the domain if
    /// it isn't applied in time.
    pub required: bool,
    pub automated_update_date: String,
    pub public: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawScan {
    pub repository: String,
    pub digest: String,
    pub critical: u32,
    pub high: u32,
    pub cves: Vec<String>,
}

/// One Amazon Inspector finding, as read (only the fields kept).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawInspector {
    /// The resource it is on: an instance id, an ECR image's digest, a
    /// function ARN.
    pub resource_id: String,
    /// `AWS_EC2_INSTANCE`, `AWS_ECR_CONTAINER_IMAGE`, `AWS_LAMBDA_FUNCTION`.
    pub resource_type: String,
    pub hit: super::inventory::InspectorHit,
}

/// One AWS Health event and the resources it names.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawHealth {
    pub arn: String,
    pub service: String,
    /// `AWS_EC2_INSTANCE_RETIREMENT_SCHEDULED`, `AWS_RDS_SECURITY_NOTIFICATION`.
    pub event_type: String,
    /// `issue`, `accountNotification`, `scheduledChange`.
    pub category: String,
    pub status: String,
    pub start: String,
    pub description: String,
    /// Instance ids, ARNs or names of the affected resources.
    pub entities: Vec<String>,
}

/// An AWS Health event as EventBridge delivers it (`source: aws.health`).
pub fn parse_health_event(body: &str) -> Option<RawHealth> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    if v["source"].as_str() != Some("aws.health") {
        return None;
    }
    let d = &v["detail"];
    let s = |k: &str| d[k].as_str().unwrap_or("").to_string();
    Some(RawHealth {
        arn: s("eventArn"),
        service: s("service"),
        event_type: s("eventTypeCode"),
        category: s("eventTypeCategory"),
        status: s("statusCode"),
        start: s("startTime"),
        description: d["eventDescription"][0]["latestDescription"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        entities: d["affectedEntities"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|e| e["entityValue"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
    })
}

/// One read of one account in one region.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RawAccount {
    pub account: String,
    pub region: String,
    pub read_at: String,
    pub instances: Vec<RawInstance>,
    pub images: Vec<RawImage>,
    pub asgs: Vec<RawAsg>,
    pub load_balancers: Vec<RawLoadBalancer>,
    pub clusters: Vec<RawEksCluster>,
    pub nodegroups: Vec<RawNodegroup>,
    pub services: Vec<RawService>,
    pub db_clusters: Vec<RawDatabase>,
    pub db_instances: Vec<RawDatabase>,
    pub pending: Vec<RawPending>,
    pub cache_updates: Vec<RawCacheUpdate>,
    pub domains: Vec<RawDomain>,
    /// `(instance id, running kernel)` from SSM inventory.
    pub kernels: Vec<(String, String)>,
    pub scans: Vec<RawScan>,
    /// Amazon Inspector's active findings (when the team runs it).
    pub inspector: Vec<RawInspector>,
    /// AWS Health events (the Health API, or the EventBridge queue).
    pub health: Vec<RawHealth>,
    /// AWS's latest AL2023 AMI in this region, by architecture.
    pub latest_al2023: BTreeMap<String, String>,
    /// Calls that failed: `(call, why)`. A refused call names the
    /// permission the feature needs.
    pub errors: Vec<(String, String)>,
}

fn tag(tags: &[(String, String)], k: &str) -> Option<String> {
    tags.iter().find(|(x, _)| x == k).map(|(_, v)| v.clone())
}

impl RawAccount {
    fn public_tgs(&self) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        for lb in self
            .load_balancers
            .iter()
            .filter(|l| l.scheme == "internet-facing")
        {
            for tg in &lb.target_groups {
                out.insert(tg.clone(), lb.name.clone());
            }
        }
        out
    }

    /// What AWS lists as pending on one ARN.
    fn lines_for(&self, arn: &str, writer: bool) -> Vec<Line> {
        self.pending
            .iter()
            .filter(|p| p.resource_arn == arn)
            .map(|p| Line {
                arn: arn.to_string(),
                action: p.action.clone(),
                description: p.description.clone(),
                apply_by: [&p.forced_apply, &p.auto_applied_after, &p.current_apply]
                    .into_iter()
                    .find(|d| !d.is_empty())
                    .map(|d| d.chars().take(10).collect())
                    .unwrap_or_default(),
                auto_applied_after: p.auto_applied_after.chars().take(10).collect(),
                writer,
            })
            .collect()
    }

    /// The resources this read found, one per fix group.
    pub fn resources(&self) -> Vec<Resource> {
        let now = &self.read_at;
        let base = |id: &str, kind: Kind, name: &str, tags: &[(String, String)]| Resource {
            id: id.to_string(),
            kind,
            name: name.to_string(),
            account: self.account.clone(),
            region: self.region.clone(),
            group: id.to_string(),
            tags: inv::keep_tags(tags.iter().cloned()),
            source: "aws".into(),
            seen_at: now.clone(),
            count: 1,
            ..Default::default()
        };
        let images: BTreeMap<&str, &RawImage> =
            self.images.iter().map(|i| (i.id.as_str(), i)).collect();
        let kernels: BTreeMap<&str, &str> = self
            .kernels
            .iter()
            .map(|(i, k)| (i.as_str(), k.as_str()))
            .collect();
        let public_tgs = self.public_tgs();
        let mut out: Vec<Resource> = Vec::new();

        // EC2: instances, collapsed into their Auto Scaling group. Node
        // group members are the node group's record.
        let mut instances = Vec::new();
        for i in &self.instances {
            if tag(&i.tags, "eks:nodegroup-name").is_some() {
                continue;
            }
            let asg = tag(&i.tags, "aws:autoscaling:groupName");
            let mut r = base(
                &i.id,
                Kind::Ec2Instance,
                &tag(&i.tags, "Name").unwrap_or_else(|| i.id.clone()),
                &i.tags,
            );
            let img = images.get(i.image_id.as_str());
            r.versions = Versions {
                ami_id: i.image_id.clone(),
                ami_name: img.map(|m| m.name.clone()).unwrap_or_default(),
                ami: img.map(|m| inv::ami_info(&m.name)),
                kernel: kernels
                    .get(i.id.as_str())
                    .map(|k| k.to_string())
                    .unwrap_or_default(),
                ..Default::default()
            };
            r.inspector = self
                .inspector
                .iter()
                .filter(|x| x.resource_id == i.id)
                .map(|x| x.hit.clone())
                .collect();
            r.exposure = if i.public_ip {
                Exposure {
                    public: true,
                    why: "public IP address".into(),
                }
            } else {
                Exposure {
                    public: false,
                    why: "no public IP address".into(),
                }
            };
            if let Some(a) = &asg {
                r.group = a.clone();
                if let Some(g) = self.asgs.iter().find(|g| &g.name == a) {
                    if let Some(lb) = g.target_groups.iter().find_map(|t| public_tgs.get(t)) {
                        r.exposure = Exposure {
                            public: true,
                            why: format!("behind internet-facing load balancer {lb}"),
                        };
                    }
                    r.name = g.name.clone();
                    r.tags = inv::keep_tags(g.tags.iter().cloned().chain(i.tags.iter().cloned()));
                    if !g.launch_template_id.is_empty() {
                        r.tags.insert(
                            "coderhelm:launch-template".into(),
                            if g.launch_template_name.is_empty() {
                                g.launch_template_id.clone()
                            } else {
                                g.launch_template_name.clone()
                            },
                        );
                    }
                }
            }
            instances.push(r);
        }
        out.extend(inv::group_instances(instances));

        // EKS: the control plane (AWS's) and each node group.
        for c in &self.clusters {
            let mut r = base(&c.arn, Kind::EksCluster, &c.name, &c.tags);
            r.versions.k8s_version = c.version.clone();
            r.versions.platform_version = c.platform_version.clone();
            if c.auto_mode {
                r.tags
                    .insert("coderhelm:eks-auto-mode".into(), "true".into());
            }
            r.exposure = Exposure {
                public: c.public_endpoint,
                why: if c.public_endpoint {
                    "public API endpoint".into()
                } else {
                    "private API endpoint".into()
                },
            };
            out.push(r);
        }
        for n in &self.nodegroups {
            let mut r = base(&n.arn, Kind::EksNodegroup, &n.name, &n.tags);
            r.count = n.desired.max(1);
            let bottlerocket = n.ami_type.starts_with("BOTTLEROCKET");
            let date = n
                .release_version
                .rsplit('-')
                .next()
                .unwrap_or("")
                .to_string();
            r.versions = Versions {
                node_release: n.release_version.clone(),
                k8s_version: n.version.clone(),
                ami_type: n.ami_type.clone(),
                ami: Some(AmiInfo {
                    os: if bottlerocket {
                        inv::Os::Bottlerocket
                    } else if n.ami_type.starts_with("AL2023") {
                        inv::Os::Al2023
                    } else if n.ami_type.starts_with("AL2") {
                        inv::Os::Al2
                    } else {
                        inv::Os::Other
                    },
                    flavor: if bottlerocket {
                        "bottlerocket".into()
                    } else {
                        "eks".into()
                    },
                    release: if bottlerocket {
                        n.release_version
                            .split('-')
                            .next()
                            .unwrap_or("")
                            .to_string()
                    } else if date.len() == 8 {
                        format!("v{date}")
                    } else {
                        String::new()
                    },
                    date: if date.len() == 8 { date } else { String::new() },
                    kernel_line: String::new(),
                }),
                ..Default::default()
            };
            if !n.launch_template_id.is_empty() {
                r.tags.insert(
                    "coderhelm:launch-template".into(),
                    n.launch_template_id.clone(),
                );
            }
            let cluster_public = self
                .clusters
                .iter()
                .any(|c| c.name == n.cluster && c.public_endpoint);
            r.exposure = Exposure {
                public: false,
                why: if cluster_public {
                    "nodes in the cluster's subnets (the API endpoint is public)".into()
                } else {
                    "nodes in the cluster's subnets".into()
                },
            };
            r.tags.insert("coderhelm:cluster".into(), n.cluster.clone());
            out.push(r);
        }

        // ECS services on Fargate (AWS runs the hosts).
        for s in self.services.iter().filter(|s| s.launch_type == "FARGATE") {
            let mut r = base(&s.arn, Kind::EcsFargateService, &s.name, &s.tags);
            r.count = s.tasks;
            r.versions = Versions {
                launch_type: s.launch_type.clone(),
                platform_version: s.platform_version.clone(),
                oldest_task_started: s.oldest_task_started.clone(),
                image: s.images.first().map(|i| i.0.clone()).unwrap_or_default(),
                image_digest: s.images.first().map(|i| i.1.clone()).unwrap_or_default(),
                ..Default::default()
            };
            r.exposure = match (
                s.public_ip,
                s.target_groups.iter().find_map(|t| public_tgs.get(t)),
            ) {
                (_, Some(lb)) => Exposure {
                    public: true,
                    why: format!("behind internet-facing load balancer {lb}"),
                },
                (true, None) => Exposure {
                    public: true,
                    why: "tasks get public IP addresses".into(),
                },
                _ => Exposure {
                    public: false,
                    why: "private subnets only".into(),
                },
            };
            r.tags.insert("coderhelm:cluster".into(), s.cluster.clone());
            out.push(r);
        }

        // Databases: Aurora clusters, and instances not in a cluster.
        for d in &self.db_clusters {
            let mut r = base(&d.arn, Kind::RdsCluster, &d.id, &d.tags);
            r.versions.engine = d.engine.clone();
            r.versions.engine_version = d.engine_version.clone();
            let members: Vec<&RawDatabase> = self
                .db_instances
                .iter()
                .filter(|i| i.cluster == d.id)
                .collect();
            let public = d.public || members.iter().any(|m| m.public);
            r.count = members.len().max(1) as u32;
            r.exposure = Exposure {
                public,
                why: if public {
                    "publicly accessible database".into()
                } else {
                    "not publicly accessible".into()
                },
            };
            // Everything pending on the cluster and on its instances, as
            // one row per meaning (the same action name means different
            // things on the two kinds of ARN).
            let mut lines = self.lines_for(&d.arn, false);
            for m in &members {
                lines.extend(self.lines_for(&m.arn, m.writer));
            }
            r.pending = actions::reconcile(&d.engine, &d.engine_version, &lines);
            r.facts = d.facts.clone();
            r.facts.insert(
                "members".into(),
                members
                    .iter()
                    .map(|m| m.id.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
            );
            if let Some(w) = members.iter().find(|m| m.writer) {
                r.facts.insert("writer".into(), w.id.clone());
            }
            out.push(r);
        }
        for d in self.db_instances.iter().filter(|i| i.cluster.is_empty()) {
            let mut r = base(&d.arn, Kind::RdsInstance, &d.id, &d.tags);
            r.versions.engine = d.engine.clone();
            r.versions.engine_version = d.engine_version.clone();
            r.exposure = Exposure {
                public: d.public,
                why: if d.public {
                    "publicly accessible database".into()
                } else {
                    "not publicly accessible".into()
                },
            };
            r.pending =
                actions::reconcile(&d.engine, &d.engine_version, &self.lines_for(&d.arn, false));
            r.facts = d.facts.clone();
            out.push(r);
        }

        // ElastiCache: one record per replication group with updates.
        let mut groups: BTreeMap<&str, Vec<&RawCacheUpdate>> = BTreeMap::new();
        for u in &self.cache_updates {
            groups.entry(u.group_id.as_str()).or_default().push(u);
        }
        for (g, ups) in groups {
            let mut r = base(g, Kind::ElasticacheGroup, g, &[]);
            r.versions.engine = ups[0].engine.clone();
            r.versions.engine_version = ups[0].engine_version.clone();
            r.exposure = Exposure {
                public: false,
                why: "ElastiCache is reachable only inside its VPC".into(),
            };
            r.pending = ups
                .iter()
                .filter(|u| !matches!(u.status.as_str(), "complete" | "stopped" | "not-applicable"))
                .map(|u| Pending {
                    kind: "service-update".into(),
                    name: u.service_update.clone(),
                    description: format!("{} {}", u.severity, u.update_type)
                        .trim()
                        .to_string(),
                    apply_by: u.apply_by.chars().take(10).collect(),
                    auto_applied_after: String::new(),
                    status: u.status.clone(),
                    meaning: Meaning::ServiceUpdate,
                    arns: vec![g.to_string()],
                    ..Default::default()
                })
                .collect();
            out.push(r);
        }

        // OpenSearch domains.
        for d in &self.domains {
            let mut r = base(&d.arn, Kind::OpensearchDomain, &d.name, &[]);
            r.versions.engine = "OpenSearch".into();
            r.versions.engine_version = d.engine_version.clone();
            r.exposure = Exposure {
                public: d.public,
                why: if d.public {
                    "public domain endpoint".into()
                } else {
                    "VPC endpoint".into()
                },
            };
            if d.update_available {
                r.pending.push(Pending {
                    kind: "service-software".into(),
                    name: d.new.clone(),
                    description: format!(
                        "service software {} to {}{}",
                        d.current,
                        d.new,
                        if d.required { " (required)" } else { "" }
                    ),
                    apply_by: d.automated_update_date.chars().take(10).collect(),
                    auto_applied_after: d.automated_update_date.chars().take(10).collect(),
                    status: "available".into(),
                    meaning: Meaning::ServiceSoftware,
                    arns: vec![d.arn.clone()],
                    ..Default::default()
                });
            }
            out.push(r);
        }

        // Container images with scan results (ECR), and Inspector's
        // findings on images that run.
        for s in &self.scans {
            let mut r = base(
                &format!("{}@{}", s.repository, s.digest),
                Kind::ContainerImage,
                &s.repository,
                &[],
            );
            r.versions.image = s.repository.clone();
            r.versions.image_digest = s.digest.clone();
            r.exposure = Exposure {
                public: self.services.iter().any(|sv| {
                    sv.images.iter().any(|(_, d)| d == &s.digest)
                        && (sv.public_ip
                            || sv.target_groups.iter().any(|t| public_tgs.contains_key(t)))
                }),
                why: "as exposed as the services that run it".into(),
            };
            r.tags
                .insert("coderhelm:scan-critical".into(), s.critical.to_string());
            r.tags
                .insert("coderhelm:scan-high".into(), s.high.to_string());
            out.push(r);
        }
        // AWS Health events land on the resources they name.
        for e in self.health.iter().filter(|e| e.status != "closed") {
            for r in out.iter_mut() {
                let named = e.entities.iter().any(|x| {
                    x == &r.id
                        || x == &r.name
                        || r.id.ends_with(&format!("/{x}"))
                        || r.id.ends_with(&format!(":{x}"))
                });
                if named && !r.pending.iter().any(|p| p.name == e.event_type) {
                    r.pending.push(Pending {
                        kind: "health".into(),
                        name: e.event_type.clone(),
                        description: crate::truncate_str(&e.description, 400).to_string(),
                        apply_by: e.start.chars().take(10).collect(),
                        auto_applied_after: if e.category == "scheduledChange" {
                            e.start.chars().take(10).collect()
                        } else {
                            String::new()
                        },
                        status: e.status.clone(),
                        meaning: Meaning::Health,
                        ..Default::default()
                    });
                }
            }
        }
        out
    }
}

/// The calls a read makes and the permission each needs, so a refusal can
/// name it.
pub const CALLS: &[(&str, &str)] = &[
    ("DescribeInstances", "ec2:DescribeInstances"),
    ("DescribeImages", "ec2:DescribeImages"),
    (
        "DescribeAutoScalingGroups",
        "autoscaling:DescribeAutoScalingGroups",
    ),
    (
        "DescribeTargetGroups",
        "elasticloadbalancing:DescribeTargetGroups",
    ),
    (
        "DescribeLoadBalancers",
        "elasticloadbalancing:DescribeLoadBalancers",
    ),
    ("ListClusters (EKS)", "eks:ListClusters"),
    ("DescribeCluster", "eks:DescribeCluster"),
    ("ListNodegroups", "eks:ListNodegroups"),
    ("DescribeNodegroup", "eks:DescribeNodegroup"),
    ("DescribeDBClusters", "rds:DescribeDBClusters"),
    ("DescribeDBInstances", "rds:DescribeDBInstances"),
    (
        "DescribePendingMaintenanceActions",
        "rds:DescribePendingMaintenanceActions",
    ),
    (
        "DescribeServiceUpdates",
        "elasticache:DescribeServiceUpdates",
    ),
    ("DescribeUpdateActions", "elasticache:DescribeUpdateActions"),
    (
        "DescribeStackResources",
        "cloudformation:DescribeStackResources",
    ),
    ("ListDomainNames", "es:ListDomainNames"),
    ("DescribeDomains", "es:DescribeDomains"),
    ("DescribeImageScanFindings", "ecr:DescribeImageScanFindings"),
    ("GetParameters", "ssm:GetParameters"),
    ("ListInventoryEntries", "ssm:ListInventoryEntries"),
    ("ListFindings (Inspector)", "inspector2:ListFindings"),
    ("DescribeEvents (Health)", "health:DescribeEvents"),
    (
        "DescribeAffectedEntities (Health)",
        "health:DescribeAffectedEntities",
    ),
];

#[cfg(test)]
mod tests {
    use super::*;

    pub const FIXTURE: &str = include_str!("testdata/account-2026-160.json");

    fn read() -> RawAccount {
        serde_json::from_str(FIXTURE).expect("the recorded account")
    }

    #[test]
    fn the_recorded_account_becomes_one_record_per_fix_group() {
        let r = read().resources();
        let kinds: Vec<Kind> = r.iter().map(|x| x.kind).collect();
        assert_eq!(
            kinds
                .iter()
                .filter(|k| **k == Kind::AutoScalingGroup)
                .count(),
            1,
            "{kinds:?}"
        );
        assert_eq!(
            kinds.iter().filter(|k| **k == Kind::EksNodegroup).count(),
            1
        );
        assert_eq!(
            kinds
                .iter()
                .filter(|k| **k == Kind::EcsFargateService)
                .count(),
            1
        );
        assert_eq!(kinds.iter().filter(|k| **k == Kind::RdsCluster).count(), 1);
        let asg = r.iter().find(|x| x.kind == Kind::AutoScalingGroup).unwrap();
        assert_eq!(asg.count, 3, "three instances, one row");
        assert_eq!(
            asg.versions.ami.as_ref().unwrap().release,
            "2023.12.20261020"
        );
        assert!(asg.exposure.public, "{:?}", asg.exposure);
        assert!(asg.exposure.why.contains("internet-facing"));
        let ng = r.iter().find(|x| x.kind == Kind::EksNodegroup).unwrap();
        assert_eq!(ng.versions.ami.as_ref().unwrap().release, "v20261015");
        assert_eq!(ng.count, 4);
        let db = r.iter().find(|x| x.kind == Kind::RdsCluster).unwrap();
        assert_eq!(db.count, 2);
        assert!(!db.exposure.public);
        assert_eq!(db.pending.len(), 1);
        assert_eq!(db.pending[0].kind, "system-update");
        assert_eq!(db.pending[0].apply_by, "2026-12-01");
        let svc = r
            .iter()
            .find(|x| x.kind == Kind::EcsFargateService)
            .unwrap();
        assert_eq!(svc.versions.oldest_task_started, "2026-10-28T14:00:00Z");
        // Nothing secret: no addresses, no environment, no user data.
        let text = serde_json::to_string(&r).unwrap();
        for no in ["UserData", "Environment", "3.91.", "password"] {
            assert!(!text.contains(no), "{no} in {text}");
        }
    }

    #[test]
    fn every_call_names_a_permission_of_the_abilities() {
        for (_, action) in CALLS {
            let found = ["infra", "inspector", "health"].iter().any(|k| {
                crate::log_reader::ability(k)
                    .unwrap()
                    .actions
                    .contains(action)
            });
            assert!(found, "{action}");
        }
    }

    #[test]
    fn a_health_event_from_eventbridge_reads_its_entities() {
        let body = r#"{"version":"0","source":"aws.health","detail-type":"AWS Health Event","detail":{"eventArn":"arn:aws:health:us-east-1::event/EC2/AWS_EC2_INSTANCE_RETIREMENT_SCHEDULED/x","service":"EC2","eventTypeCode":"AWS_EC2_INSTANCE_RETIREMENT_SCHEDULED","eventTypeCategory":"scheduledChange","statusCode":"upcoming","startTime":"Sat, 21 Nov 2026 00:00:00 GMT","eventDescription":[{"language":"en_US","latestDescription":"Your instance will be retired."}],"affectedEntities":[{"entityValue":"i-0a1b2c3d4e5f60001"}]}}"#;
        let e = parse_health_event(body).unwrap();
        assert_eq!(e.event_type, "AWS_EC2_INSTANCE_RETIREMENT_SCHEDULED");
        assert_eq!(e.entities, vec!["i-0a1b2c3d4e5f60001"]);
        assert!(parse_health_event(r#"{"source":"aws.ec2"}"#).is_none());
    }

    #[test]
    fn inspector_and_health_land_on_the_resources_they_name() {
        let mut raw = read();
        raw.inspector.push(RawInspector {
            resource_id: "i-0a1b2c3d4e5f60002".into(),
            resource_type: "AWS_EC2_INSTANCE".into(),
            hit: super::super::inventory::InspectorHit {
                id: "f1".into(),
                vulnerability: "CVE-2026-98432".into(),
                severity: "HIGH".into(),
                exploit_available: true,
                reachable: Some(true),
                ..Default::default()
            },
        });
        raw.health.push(RawHealth {
            arn: "arn:aws:health:us-east-1::event/RDS/AWS_RDS_SECURITY_NOTIFICATION/x".into(),
            event_type: "AWS_RDS_SECURITY_NOTIFICATION".into(),
            category: "accountNotification".into(),
            status: "open".into(),
            start: "2026-11-01T00:00:00Z".into(),
            entities: vec!["orders".into()],
            ..Default::default()
        });
        let r = raw.resources();
        let asg = r.iter().find(|x| x.kind == Kind::AutoScalingGroup).unwrap();
        assert_eq!(
            asg.inspector.len(),
            1,
            "the group carries its instances' Inspector findings"
        );
        let db = r.iter().find(|x| x.kind == Kind::RdsCluster).unwrap();
        assert!(db
            .pending
            .iter()
            .any(|p| p.kind == "health" && p.name == "AWS_RDS_SECURITY_NOTIFICATION"));
    }
}
