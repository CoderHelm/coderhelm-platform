//! A team's running AWS resources, as read through the optional
//! "Infrastructure vulnerabilities" permissions of its CoderHelmLogReader
//! role (`worker::passes::infra_inventory` reads; this is the model).
//!
//! One record per resource in the AWS insights table, `RES#<account>#<region>#<id>`,
//! with its versions (AMI and the release in its name, kernel when known,
//! image digest, node group release, engine version), whether it is
//! reachable from the internet, its tags, what AWS has pending for it, and
//! where it is defined (repository, file, Terraform address).
//!
//! What is never stored: environment variables, user data, secrets, task
//! definitions' environment. The reads that would return them
//! (`lambda:GetFunctionConfiguration`, `lambda:ListFunctions`,
//! `ec2:DescribeLaunchTemplateVersions`, `ec2:DescribeInstanceAttribute`)
//! aren't in the role; Lambda functions come from the team's
//! infrastructure code instead ([`lambdas_from_terraform`]).

use std::collections::BTreeMap;

use regex::Regex;
use serde::{Deserialize, Serialize};

pub const RESOURCE_PREFIX: &str = "RES#";
/// A resource not seen for this long ages out (the sweep is daily).
pub const KEEP_DAYS: i64 = 3;
/// Tags kept per resource, and their lengths.
const MAX_TAGS: usize = 30;
const MAX_TAG_LEN: usize = 256;

pub fn resource_sk(account: &str, region: &str, id: &str) -> String {
    format!("{RESOURCE_PREFIX}{account}#{region}#{id}")
}

/// What a resource is.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    #[default]
    Ec2Instance,
    AutoScalingGroup,
    EksNodegroup,
    EksCluster,
    EcsFargateService,
    LambdaFunction,
    RdsInstance,
    RdsCluster,
    ElasticacheGroup,
    OpensearchDomain,
    ContainerImage,
}

impl Kind {
    pub fn label(self) -> &'static str {
        match self {
            Kind::Ec2Instance => "EC2 instance",
            Kind::AutoScalingGroup => "Auto Scaling group",
            Kind::EksNodegroup => "EKS node group",
            Kind::EksCluster => "EKS cluster",
            Kind::EcsFargateService => "ECS service on Fargate",
            Kind::LambdaFunction => "Lambda function",
            Kind::RdsInstance => "RDS instance",
            Kind::RdsCluster => "Aurora cluster",
            Kind::ElasticacheGroup => "ElastiCache cluster",
            Kind::OpensearchDomain => "OpenSearch domain",
            Kind::ContainerImage => "Container image",
        }
    }
}

/// The operating system an AMI runs, read from its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Os {
    Al2023,
    Al2,
    Bottlerocket,
    Ubuntu,
    Debian,
    Windows,
    #[default]
    Other,
}

/// What an AMI's name says.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AmiInfo {
    pub os: Os,
    /// `al2023` (plain), `ecs` (ECS optimized), `eks` (EKS optimized),
    /// `bottlerocket`.
    pub flavor: String,
    /// AL2023 release (`2023.12.20261020`); AL2 build date (`20261020`);
    /// ECS optimized build (`20261015`); EKS release (`v20261015`);
    /// Bottlerocket version (`1.67.0`).
    pub release: String,
    /// The build date in the name, YYYYMMDD, when there is one.
    pub date: String,
    /// The kernel line in the name (`6.1`, `5.10`).
    pub kernel_line: String,
}

fn re(p: &str) -> Regex {
    Regex::new(p).expect("a valid pattern")
}

/// Reads an Amazon AMI name.
pub fn ami_info(name: &str) -> AmiInfo {
    let first = |p: &str| {
        re(p)
            .captures(name)
            .map(|c| c[1].to_string())
            .unwrap_or_default()
    };
    let kernel_line = first(r"kernel-(\d+\.\d+)");
    let date = first(r"(20\d{6})");
    let n = name.to_ascii_lowercase();
    if n.starts_with("amazon-eks-node-al2023") || n.starts_with("amazon-eks-node-") {
        return AmiInfo {
            os: if n.contains("al2023") {
                Os::Al2023
            } else {
                Os::Al2
            },
            flavor: "eks".into(),
            release: first(r"-(v\d{8})"),
            date,
            kernel_line,
        };
    }
    if n.starts_with("bottlerocket-") {
        return AmiInfo {
            os: Os::Bottlerocket,
            flavor: "bottlerocket".into(),
            release: first(r"-v(\d+\.\d+\.\d+)"),
            date,
            kernel_line,
        };
    }
    if n.starts_with("al2023-ami-ecs") {
        return AmiInfo {
            os: Os::Al2023,
            flavor: "ecs".into(),
            release: date.clone(),
            date,
            kernel_line,
        };
    }
    if n.starts_with("al2023-ami") {
        return AmiInfo {
            os: Os::Al2023,
            flavor: "al2023".into(),
            release: first(r"(2023\.\d+\.\d{8})"),
            date,
            kernel_line,
        };
    }
    if n.starts_with("amzn2-ami-ecs") {
        return AmiInfo {
            os: Os::Al2,
            flavor: "ecs".into(),
            release: date.clone(),
            date,
            kernel_line,
        };
    }
    if n.starts_with("amzn2-ami") {
        return AmiInfo {
            os: Os::Al2,
            flavor: "al2".into(),
            release: date.clone(),
            date,
            kernel_line: if kernel_line.is_empty() {
                "4.14".into()
            } else {
                kernel_line
            },
        };
    }
    let os = if n.contains("ubuntu") {
        Os::Ubuntu
    } else if n.starts_with("debian") {
        Os::Debian
    } else if n.contains("windows") {
        Os::Windows
    } else {
        Os::Other
    };
    AmiInfo {
        os,
        flavor: String::new(),
        release: String::new(),
        date,
        kernel_line,
    }
}

/// The versions a resource runs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Versions {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ami_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ami_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ami: Option<AmiInfo>,
    /// The running kernel (`6.1.155-180.256.amzn2023`), from SSM inventory
    /// when the instance is managed by SSM.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kernel: String,
    /// An image reference (`repo:tag`) and the digest that runs.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub image: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub image_digest: String,
    /// EKS node group release (`1.31.0-20261015`) and Kubernetes version.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub node_release: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub k8s_version: String,
    /// EKS AMI type (`AL2023_x86_64_STANDARD`, `BOTTLEROCKET_x86_64`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ami_type: String,
    /// `FARGATE` or `EC2`, and when the oldest running task started.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub launch_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub oldest_task_started: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub platform_version: String,
    /// Database or cache engine and its version.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub engine: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub engine_version: String,
    /// Lambda runtime and package type (`Zip`, `Image`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub runtime: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub package_type: String,
}

/// Whether the internet can reach the resource, and why it was decided.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exposure {
    pub public: bool,
    /// `public IP 3.91.x.x`, `behind internet-facing load balancer web-alb`,
    /// `publicly accessible database`, `private subnet only`.
    pub why: String,
}

/// Something AWS has waiting for the resource (applied in AWS, not in code).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pending {
    /// `system-update`, `db-upgrade`, `service-update`, `service-software`.
    pub kind: String,
    /// The update's name (`elasticache-20261106-intel`) or description.
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// The date AWS says to apply it by (YYYY-MM-DD), empty when none.
    #[serde(default)]
    pub apply_by: String,
    /// When AWS applies it itself, empty when it won't.
    #[serde(default)]
    pub auto_applied_after: String,
    #[serde(default)]
    pub status: String,
}

/// One Amazon Inspector finding on a resource (when the team runs it).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectorHit {
    /// The finding's id (the end of its ARN).
    pub id: String,
    /// `CVE-...` or another vulnerability id.
    pub vulnerability: String,
    /// CRITICAL, HIGH, MEDIUM, LOW, as Inspector rates it.
    pub severity: String,
    pub exploit_available: bool,
    pub fix_available: bool,
    /// Inspector's network reachability: `Some(true)` when a path from
    /// the internet reaches an open port.
    pub reachable: Option<bool>,
    /// `kernel 6.1.155 (fixed in 6.1.189)`.
    pub packages: Vec<String>,
    pub remediation: String,
}

/// Where a resource is defined in the team's code.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Location {
    pub repo: String,
    #[serde(default)]
    pub file: String,
    /// `aws_launch_template.web`, `module.eks.aws_eks_node_group.main`.
    #[serde(default)]
    pub address: String,
    #[serde(default)]
    pub line: u32,
    /// How it was found: `tag`, `cloudformation`, `search`.
    pub how: String,
}

/// One running resource.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resource {
    /// The ARN or AWS id.
    pub id: String,
    pub kind: Kind,
    pub name: String,
    pub account: String,
    pub region: String,
    /// What one fix changes for all of them: the launch template, Auto
    /// Scaling group, node group, cluster. Resources with the same group
    /// are one row of a finding.
    pub group: String,
    pub versions: Versions,
    pub exposure: Exposure,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub pending: Vec<Pending>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defined_in: Option<Location>,
    /// `aws` (read from the account) or `iac` (from the team's code).
    pub source: String,
    pub seen_at: String,
    /// How many running things this record stands for (instances in an
    /// Auto Scaling group, nodes in a node group, tasks of a service).
    #[serde(default)]
    pub count: u32,
    /// Amazon Inspector's findings on it, when the team runs Inspector.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inspector: Vec<InspectorHit>,
}

impl Resource {
    pub fn sk(&self) -> String {
        resource_sk(&self.account, &self.region, &self.id)
    }

    /// The AMI name's reading, when the resource runs an AMI.
    pub fn ami(&self) -> Option<&AmiInfo> {
        self.versions.ami.as_ref()
    }
}

/// Tags as kept: at most [`MAX_TAGS`], each value cut to [`MAX_TAG_LEN`],
/// and none whose key reads like it holds a secret.
pub fn keep_tags(tags: impl IntoIterator<Item = (String, String)>) -> BTreeMap<String, String> {
    let secret = [
        "password", "secret", "token", "apikey", "api_key", "private",
    ];
    let mut out = BTreeMap::new();
    for (k, v) in tags {
        let lk = k.to_ascii_lowercase();
        if secret.iter().any(|s| lk.contains(s)) {
            continue;
        }
        if out.len() >= MAX_TAGS {
            break;
        }
        out.insert(
            crate::truncate_str(&k, 128).to_string(),
            crate::truncate_str(&v, MAX_TAG_LEN).to_string(),
        );
    }
    out
}

// ── Where it is defined ─────────────────────────────────────────────────

/// Tags that name the repository directly.
const REPO_TAGS: &[&str] = &[
    "repo",
    "repository",
    "git_repo",
    "gitrepo",
    "source_repo",
    "github_repo",
];

/// A repository named by a tag (`org/repo` or a GitHub URL), when it is one
/// of the team's.
pub fn repo_from_tags(tags: &BTreeMap<String, String>, team_repos: &[String]) -> Option<String> {
    for (k, v) in tags {
        if !REPO_TAGS.contains(&k.to_ascii_lowercase().as_str()) {
            continue;
        }
        let v = v
            .trim()
            .trim_end_matches(".git")
            .trim_start_matches("https://github.com/")
            .trim_start_matches("git@github.com:");
        if let Some(r) = team_repos.iter().find(|r| r.eq_ignore_ascii_case(v)) {
            return Some(r.clone());
        }
        // A bare repository name, unique among the team's.
        let by_name: Vec<&String> = team_repos
            .iter()
            .filter(|r| {
                r.rsplit('/')
                    .next()
                    .is_some_and(|n| n.eq_ignore_ascii_case(v))
            })
            .collect();
        if by_name.len() == 1 {
            return Some(by_name[0].clone());
        }
    }
    None
}

/// The CloudFormation stack that made the resource, from AWS's own tag.
pub fn stack_of(tags: &BTreeMap<String, String>) -> Option<&str> {
    tags.get("aws:cloudformation:stack-name")
        .map(String::as_str)
}

/// The names to search the team's infrastructure code for: the AMI id,
/// the group, the resource's own name, its CloudFormation logical id.
pub fn search_terms(r: &Resource) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |s: &str| {
        let s = s.trim();
        if s.len() >= 4 && !out.iter().any(|x| x == s) {
            out.push(s.to_string());
        }
    };
    if !r.versions.ami_id.is_empty() {
        push(&r.versions.ami_id);
    }
    // A group that is an ARN ends in an id nobody writes in code.
    if !r.group.starts_with("arn:") {
        push(&r.group);
    }
    push(&r.name);
    if let Some(l) = r.tags.get("aws:cloudformation:logical-id") {
        push(l);
    }
    out.truncate(4);
    out
}

/// The Terraform block around `line` (1-based) in a `.tf` file: its
/// address (`aws_launch_template.web`), with `module.<name>.` when the file
/// is a module called by that name elsewhere (not known here, so plain).
pub fn terraform_address(content: &str, line: usize) -> Option<(String, u32)> {
    let block = re(r#"^\s*resource\s+"([a-z0-9_]+)"\s+"([A-Za-z0-9_-]+)""#);
    let lines: Vec<&str> = content.lines().collect();
    let upto = line.min(lines.len());
    for i in (0..upto).rev() {
        if let Some(c) = block.captures(lines[i]) {
            return Some((format!("{}.{}", &c[1], &c[2]), (i + 1) as u32));
        }
    }
    None
}

/// Where `term` appears in a file: its 1-based line.
pub fn line_of(content: &str, term: &str) -> Option<usize> {
    content
        .lines()
        .position(|l| l.contains(term))
        .map(|i| i + 1)
}

/// Picks where a resource is defined from what was found: a repository a
/// tag names wins; else the first infrastructure file a search found for
/// the earliest term (`files` are `(repo, path, content)`).
pub fn locate(
    r: &Resource,
    team_repos: &[String],
    files: &[(String, String, String)],
) -> Option<Location> {
    let tagged = repo_from_tags(&r.tags, team_repos);
    for term in search_terms(r) {
        for (repo, path, content) in files {
            if tagged.as_ref().is_some_and(|t| t != repo) {
                continue;
            }
            let Some(line) = line_of(content, &term) else {
                continue;
            };
            let (address, line) = if path.ends_with(".tf") {
                terraform_address(content, line).unwrap_or((String::new(), line as u32))
            } else {
                (String::new(), line as u32)
            };
            return Some(Location {
                repo: repo.clone(),
                file: path.clone(),
                address,
                line,
                how: if tagged.is_some() {
                    "tag".into()
                } else {
                    "search".into()
                },
            });
        }
    }
    if let Some(repo) = tagged {
        return Some(Location {
            repo,
            how: "tag".into(),
            ..Default::default()
        });
    }
    stack_of(&r.tags).map(|s| Location {
        repo: String::new(),
        file: String::new(),
        address: format!("stack {s}"),
        line: 0,
        how: "cloudformation".into(),
    })
}

/// Lambda functions as the team's Terraform defines them (`aws_lambda_function`
/// blocks with a `runtime`): the role can't read function settings, since
/// they hold environment variables.
pub fn lambdas_from_terraform(
    repo: &str,
    path: &str,
    content: &str,
    account: &str,
    region: &str,
    now: &str,
) -> Vec<Resource> {
    let block = re(r#"(?m)^\s*resource\s+"aws_lambda_function"\s+"([A-Za-z0-9_-]+)"\s*\{"#);
    let runtime = re(r#"(?m)^\s*runtime\s*=\s*"([a-z0-9.]+)""#);
    let package = re(r#"(?m)^\s*package_type\s*=\s*"([A-Za-z]+)""#);
    let fname = re(r#"(?m)^\s*function_name\s*=\s*"([^"$]+)""#);
    let mut out = Vec::new();
    let starts: Vec<(usize, String)> = block
        .captures_iter(content)
        .map(|c| (c.get(0).unwrap().start(), c[1].to_string()))
        .collect();
    for (i, (start, name)) in starts.iter().enumerate() {
        let end = starts.get(i + 1).map(|(s, _)| *s).unwrap_or(content.len());
        let body = &content[*start..end];
        let rt = runtime
            .captures(body)
            .map(|c| c[1].to_string())
            .unwrap_or_default();
        let pt = package
            .captures(body)
            .map(|c| c[1].to_string())
            .unwrap_or_else(|| "Zip".into());
        if rt.is_empty() && pt != "Image" {
            continue;
        }
        let fn_name = fname
            .captures(body)
            .map(|c| c[1].to_string())
            .unwrap_or_else(|| name.clone());
        let line = content[..*start].lines().count() as u32 + 1;
        out.push(Resource {
            id: format!("iac:{repo}:{path}:aws_lambda_function.{name}"),
            kind: Kind::LambdaFunction,
            name: fn_name,
            account: account.into(),
            region: region.into(),
            group: format!("aws_lambda_function.{name}"),
            versions: Versions {
                runtime: rt,
                package_type: pt,
                ..Default::default()
            },
            exposure: Exposure {
                public: false,
                why: "Lambda functions have no inbound ports".into(),
            },
            defined_in: Some(Location {
                repo: repo.into(),
                file: path.into(),
                address: format!("aws_lambda_function.{name}"),
                line,
                how: "search".into(),
            }),
            source: "iac".into(),
            seen_at: now.into(),
            count: 1,
            ..Default::default()
        });
    }
    out
}

/// Instances that one fix changes together collapse into their group's
/// record: one row per launch template or Auto Scaling group, counting
/// the instances and keeping the oldest AMI seen (what a fix must replace).
pub fn group_instances(instances: Vec<Resource>) -> Vec<Resource> {
    let mut by: BTreeMap<String, Resource> = BTreeMap::new();
    for i in instances {
        let key = if i.group.is_empty() {
            i.id.clone()
        } else {
            i.group.clone()
        };
        match by.get_mut(&key) {
            Some(g) => {
                g.count += i.count.max(1);
                g.exposure.public |= i.exposure.public;
                if i.exposure.public && !g.exposure.why.starts_with("public") {
                    g.exposure.why = i.exposure.why.clone();
                }
                let older = match (&g.versions.ami, &i.versions.ami) {
                    (Some(a), Some(b)) => b.date < a.date,
                    (None, Some(_)) => true,
                    _ => false,
                };
                if older {
                    g.versions = i.versions.clone();
                }
                for h in i.inspector {
                    if !g
                        .inspector
                        .iter()
                        .any(|x| x.vulnerability == h.vulnerability)
                    {
                        g.inspector.push(h);
                    }
                }
            }
            None => {
                let mut g = i;
                g.count = g.count.max(1);
                if !g.group.is_empty() && g.group != g.id {
                    g.id = g.group.clone();
                    g.kind = Kind::AutoScalingGroup;
                }
                by.insert(key, g);
            }
        }
    }
    by.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ami_names_say_os_flavor_and_release() {
        let a = ami_info("al2023-ami-2023.12.20261020.0-kernel-6.1-x86_64");
        assert_eq!(
            (
                a.os,
                a.flavor.as_str(),
                a.release.as_str(),
                a.kernel_line.as_str()
            ),
            (Os::Al2023, "al2023", "2023.12.20261020", "6.1")
        );
        let e = ami_info("amazon-eks-node-al2023-x86_64-standard-1.31-v20261015");
        assert_eq!(
            (e.flavor.as_str(), e.release.as_str()),
            ("eks", "v20261015")
        );
        let c = ami_info("al2023-ami-ecs-hvm-2023.0.20261015-kernel-6.1-x86_64");
        assert_eq!((c.flavor.as_str(), c.release.as_str()), ("ecs", "20261015"));
        let two = ami_info("amzn2-ami-kernel-5.10-hvm-2.0.20261020.0-x86_64-gp2");
        assert_eq!(
            (two.os, two.release.as_str(), two.kernel_line.as_str()),
            (Os::Al2, "20261020", "5.10")
        );
        let br = ami_info("bottlerocket-aws-k8s-1.31-x86_64-v1.67.0-7ac1a2b3");
        assert_eq!((br.os, br.release.as_str()), (Os::Bottlerocket, "1.67.0"));
        assert_eq!(
            ami_info("ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-20261001").os,
            Os::Ubuntu
        );
        assert_eq!(ami_info("my-golden-image").os, Os::Other);
    }

    #[test]
    fn tags_that_look_secret_are_dropped() {
        let t = keep_tags(vec![
            ("Name".to_string(), "web".to_string()),
            ("db_password".to_string(), "hunter2".to_string()),
            ("ApiToken".to_string(), "x".to_string()),
            ("Repo".to_string(), "acme/infra".to_string()),
        ]);
        assert_eq!(t.len(), 2);
        assert!(!t.values().any(|v| v == "hunter2"));
    }

    #[test]
    fn a_repo_tag_names_one_of_the_teams_repositories() {
        let repos = vec!["acme/infra".to_string(), "acme/web".to_string()];
        let t = keep_tags(vec![(
            "Repository".into(),
            "https://github.com/acme/infra.git".into(),
        )]);
        assert_eq!(repo_from_tags(&t, &repos).as_deref(), Some("acme/infra"));
        let bare = keep_tags(vec![("repo".into(), "web".into())]);
        assert_eq!(repo_from_tags(&bare, &repos).as_deref(), Some("acme/web"));
        let other = keep_tags(vec![("repo".into(), "someone/else".into())]);
        assert_eq!(repo_from_tags(&other, &repos), None);
    }

    const TF: &str = r#"
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "aws_launch_template" "web" {
  name_prefix   = "web-"
  image_id      = "ami-0abc1234def567890"
  instance_type = "t3.small"
}

resource "aws_lambda_function" "thumbs" {
  function_name = "thumbs"
  runtime       = "python3.12"
  handler       = "app.handler"
  environment {
    variables = { SECRET = var.secret }
  }
}

resource "aws_lambda_function" "render" {
  function_name = "render"
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.r.repository_url}:latest"
}
"#;

    #[test]
    fn the_terraform_address_around_a_line() {
        let line = line_of(TF, "ami-0abc1234def567890").unwrap();
        assert_eq!(
            terraform_address(TF, line),
            Some(("aws_launch_template.web".into(), 6))
        );
        assert_eq!(terraform_address(TF, 2), None);
    }

    #[test]
    fn a_resource_is_located_by_its_ami_in_the_teams_code() {
        let r = Resource {
            id: "lt-0123".into(),
            group: "lt-0123".into(),
            versions: Versions {
                ami_id: "ami-0abc1234def567890".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let files = vec![(
            "acme/infra".to_string(),
            "envs/prod/web.tf".to_string(),
            TF.to_string(),
        )];
        let l = locate(&r, &["acme/infra".into()], &files).unwrap();
        assert_eq!(l.address, "aws_launch_template.web");
        assert_eq!(l.file, "envs/prod/web.tf");
        assert_eq!(l.how, "search");
        // Nothing found and no tags: unknown.
        assert!(locate(&r, &[], &[]).is_none());
        // A CloudFormation stack tag says at least which stack.
        let mut s = r.clone();
        s.tags
            .insert("aws:cloudformation:stack-name".into(), "web-prod".into());
        assert_eq!(locate(&s, &[], &[]).unwrap().how, "cloudformation");
    }

    #[test]
    fn lambdas_come_from_terraform_without_their_environment() {
        let l = lambdas_from_terraform(
            "acme/infra",
            "lambda.tf",
            TF,
            "111122223333",
            "us-east-1",
            "t",
        );
        assert_eq!(l.len(), 2);
        let zip = l.iter().find(|r| r.name == "thumbs").unwrap();
        assert_eq!(zip.versions.runtime, "python3.12");
        assert_eq!(zip.versions.package_type, "Zip");
        assert_eq!(
            zip.defined_in.as_ref().unwrap().address,
            "aws_lambda_function.thumbs"
        );
        let img = l.iter().find(|r| r.name == "render").unwrap();
        assert_eq!(img.versions.package_type, "Image");
        let text = serde_json::to_string(&l).unwrap();
        assert!(
            !text.contains("SECRET") && !text.contains("var.secret"),
            "{text}"
        );
    }

    #[test]
    fn instances_collapse_into_their_group() {
        let inst = |id: &str, date: &str, public: bool| Resource {
            id: id.into(),
            kind: Kind::Ec2Instance,
            group: "web-asg".into(),
            versions: Versions {
                ami_id: format!("ami-{date}"),
                ami: Some(AmiInfo {
                    date: date.into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            exposure: Exposure {
                public,
                why: if public {
                    "public IP".into()
                } else {
                    "private subnet only".into()
                },
            },
            count: 1,
            ..Default::default()
        };
        let g = group_instances(vec![
            inst("i-1", "20261020", false),
            inst("i-2", "20261001", true),
            inst("i-3", "20261020", false),
        ]);
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].count, 3);
        assert_eq!(g[0].id, "web-asg");
        assert_eq!(g[0].kind, Kind::AutoScalingGroup);
        assert_eq!(
            g[0].versions.ami_id, "ami-20261001",
            "the oldest AMI decides"
        );
        assert!(g[0].exposure.public);
        let solo = Resource {
            id: "i-9".into(),
            count: 1,
            ..Default::default()
        };
        assert_eq!(group_instances(vec![solo])[0].kind, Kind::Ec2Instance);
    }

    #[test]
    fn keys_are_per_account_region_and_id() {
        assert_eq!(
            resource_sk("111122223333", "us-east-1", "lt-1"),
            "RES#111122223333#us-east-1#lt-1"
        );
    }
}
