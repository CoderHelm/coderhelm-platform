//! Where a live resource is defined in a team's code, across every
//! connected repository and layout, with how it was found and how sure
//! that is.
//!
//! In order of certainty:
//!
//! 1. **CloudFormation's own records.** A resource a stack made carries
//!    `aws:cloudformation:stack-name` and `logical-id` tags; where tags are
//!    not propagated, the worker asks CloudFormation which stack owns the
//!    physical id. The logical id is a literal key in a hand-written, SAM
//!    or Serverless Framework template, and the readable part of a CDK
//!    construct path ([`in_stack`]).
//! 2. **Terraform and OpenTofu**, by resource type and attributes
//!    ([`super::terraform`]).
//! 3. **The name written in an infrastructure file** of any tool, as a
//!    last resort, graded lower.
//!
//! Files are recognised as infrastructure code by what is in them, not
//! only by their name ([`tool_of`]). What to search a repository for is
//! ranked so every resource gets its strongest name before any gets a
//! second ([`ranked_terms`]).

use std::collections::BTreeMap;

use regex::Regex;

use super::inventory::{Confidence, Edit, Kind, Location, Resource};
use super::terraform;

/// One file of a team's repository.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceFile {
    pub repo: String,
    pub path: String,
    pub content: String,
}

/// A place a searched name appears, in any kind of file (a workflow that
/// deploys a stack, a `samconfig.toml`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Mention {
    pub repo: String,
    pub path: String,
    pub term: String,
}

fn re(p: &str) -> Regex {
    Regex::new(p).expect("a valid pattern")
}

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn ext(path: &str) -> &str {
    file_name(path)
        .rsplit_once('.')
        .map(|(_, e)| e)
        .unwrap_or("")
}

/// Directories whose files are never a team's own infrastructure code.
fn vendored(path: &str) -> bool {
    let p = format!("/{}", path.to_ascii_lowercase());
    [
        "/node_modules/",
        "/vendor/",
        "/.terraform/",
        "/cdk.out/",
        "/dist/",
        "/build/",
        "/.serverless/",
        "/.aws-sam/",
        "/.git/",
        "/site-packages/",
    ]
    .iter()
    .any(|d| p.contains(d))
}

/// Whether a path could hold infrastructure code, by its name alone:
/// worth reading to find out ([`tool_of`] decides).
pub fn maybe_iac_path(path: &str) -> bool {
    if vendored(path) {
        return false;
    }
    let p = path.to_ascii_lowercase();
    let name = file_name(&p);
    matches!(
        ext(&p),
        "tf" | "tofu"
            | "tfvars"
            | "hcl"
            | "yaml"
            | "yml"
            | "json"
            | "ts"
            | "js"
            | "mjs"
            | "py"
            | "go"
            | "java"
            | "cs"
            | "toml"
    ) && !name.ends_with(".d.ts")
        && !name.ends_with(".test.ts")
        && !name.ends_with(".spec.ts")
        && !matches!(
            name,
            "package.json"
                | "package-lock.json"
                | "tsconfig.json"
                | "pyproject.toml"
                | "cargo.toml"
        )
}

/// Whether a path names infrastructure code without reading it.
pub fn iac_by_name(path: &str) -> bool {
    if vendored(path) {
        return false;
    }
    let p = path.to_ascii_lowercase();
    let name = file_name(&p);
    let dirs = format!("/{p}");
    matches!(ext(&p), "tf" | "tofu" | "tfvars" | "hcl")
        || p.ends_with(".tf.json")
        || matches!(
            name,
            "serverless.yml"
                | "serverless.yaml"
                | "serverless.ts"
                | "serverless.js"
                | "samconfig.toml"
                | "samconfig.yaml"
                | "cdk.json"
                | "pulumi.yaml"
                | "pulumi.yml"
        )
        || (name.starts_with("pulumi.") && matches!(ext(&p), "yaml" | "yml"))
        || (matches!(ext(&p), "yaml" | "yml" | "json")
            && (p.contains("cloudformation")
                || p.contains("cfn")
                || name.contains("template")
                || name.contains("stack")
                || name.starts_with("sam.")))
        || (matches!(ext(&p), "ts" | "js" | "mjs" | "py" | "go" | "java" | "cs")
            && (name.contains("stack")
                || name.contains("construct")
                || [
                    "/cdk/",
                    "/pulumi/",
                    "/infra/",
                    "/infrastructure/",
                    "/iac/",
                    "/stacks/",
                ]
                .iter()
                .any(|d| dirs.contains(d))))
}

/// The tool a file is written for, from its content: `terraform`,
/// `opentofu`, `cloudformation`, `sam`, `serverless`, `cdk`, `pulumi`.
/// `None` when it is not infrastructure code.
pub fn tool_of(path: &str, content: &str) -> Option<&'static str> {
    if vendored(path) {
        return None;
    }
    let p = path.to_ascii_lowercase();
    let name = file_name(&p);
    match ext(&p) {
        "tofu" => return Some("opentofu"),
        "tf" | "tfvars" | "hcl" => return Some("terraform"),
        _ => {}
    }
    if p.ends_with(".tf.json") {
        return Some("terraform");
    }
    if name == "cdk.json" {
        return Some("cdk");
    }
    if name.starts_with("samconfig.") {
        return Some("sam");
    }
    if name.starts_with("pulumi.") && matches!(ext(&p), "yaml" | "yml") {
        return Some("pulumi");
    }
    let has = |w: &str| content.contains(w);
    match ext(&p) {
        "yaml" | "yml" | "json" => {
            if name.starts_with("serverless.") && has("provider") && has("service") {
                return Some("serverless");
            }
            if has("AWS::Serverless-2016-10-31") {
                return Some("sam");
            }
            let resources = has("Resources:") || has("\"Resources\"");
            let typed = re(r#"["']?Type["']?\s*:\s*["']?(AWS|Custom|Alexa)::"#).is_match(content);
            if has("AWSTemplateFormatVersion") || (resources && typed) {
                return Some("cloudformation");
            }
            if has("provider:") && has("functions:") && has("service:") {
                return Some("serverless");
            }
            None
        }
        "ts" | "js" | "mjs" | "py" | "go" | "java" | "cs" => {
            if name.starts_with("serverless.") && has("provider") && has("service") {
                return Some("serverless");
            }
            if [
                "aws-cdk-lib",
                "@aws-cdk/",
                "aws_cdk",
                "software.amazon.awscdk",
                "github.com/aws/aws-cdk-go",
                "Amazon.CDK",
            ]
            .iter()
            .any(|w| has(w))
            {
                return Some("cdk");
            }
            if [
                "@pulumi/aws",
                "pulumi_aws",
                "github.com/pulumi/pulumi-aws",
                "Pulumi.Aws",
                "com.pulumi.aws",
            ]
            .iter()
            .any(|w| has(w))
            {
                return Some("pulumi");
            }
            None
        }
        _ => None,
    }
}

/// What to search a repository for to find its infrastructure code by
/// content, whatever its files are called.
pub const MARKERS: &[&str] = &[
    "AWSTemplateFormatVersion",
    "AWS::Serverless-2016-10-31",
    "aws-cdk-lib",
    "aws_cdk",
    "@pulumi/aws",
    "pulumi_aws",
    "resource \"aws_",
    "terraform-aws-modules",
];

const ENVS: &[(&str, &[&str])] = &[
    ("production", &["prod", "production", "prd", "live"]),
    (
        "staging",
        &["staging", "stage", "stg", "preprod", "uat", "preview"],
    ),
    (
        "development",
        &["dev", "development", "sandbox", "sbx", "nonprod"],
    ),
    ("test", &["test", "testing", "qa", "ci"]),
];

/// The environment a name, a path or a tag value speaks of: `production`,
/// `staging`, `development`, `test`, or empty.
pub fn environment_of(words: &[&str]) -> String {
    for w in words {
        let l = w.to_ascii_lowercase();
        for token in l.split(|c: char| !c.is_ascii_alphanumeric()) {
            for (env, names) in ENVS {
                if names.contains(&token) {
                    return (*env).to_string();
                }
            }
        }
    }
    String::new()
}

/// A resource's environment: its environment tag first, then its name.
pub fn environment_of_resource(r: &Resource) -> String {
    for (k, v) in &r.tags {
        if matches!(
            k.to_ascii_lowercase().as_str(),
            "environment" | "env" | "stage" | "tier"
        ) {
            let e = environment_of(&[v]);
            if !e.is_empty() {
                return e;
            }
        }
    }
    environment_of(&[
        &r.name,
        r.tags
            .get("aws:cloudformation:stack-name")
            .map(String::as_str)
            .unwrap_or(""),
    ])
}

/// The stack that made a resource, by AWS's own records.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StackRef {
    pub stack: String,
    pub logical_id: String,
    /// `cloudformation_tag` (the resource's own tags) or
    /// `cloudformation_lookup` (CloudFormation asked by physical id).
    pub how: &'static str,
}

/// Tag the worker sets when CloudFormation named the stack by lookup.
pub const LOOKUP_TAG: &str = "coderhelm:cloudformation-lookup";

pub fn stack_ref(r: &Resource) -> Option<StackRef> {
    let stack = r.tags.get("aws:cloudformation:stack-name")?.clone();
    Some(StackRef {
        stack,
        logical_id: r
            .tags
            .get("aws:cloudformation:logical-id")
            .cloned()
            .unwrap_or_default(),
        how: if r.tags.contains_key(LOOKUP_TAG) {
            "cloudformation_lookup"
        } else {
            "cloudformation_tag"
        },
    })
}

/// What a kind is in a template, the property a version change edits, and
/// the words that name it in a CDK construct class.
struct CfnSpec {
    types: &'static [&'static str],
    version: &'static str,
    classes: &'static [&'static str],
}

fn cfn_spec(kind: Kind) -> CfnSpec {
    match kind {
        Kind::RdsInstance => CfnSpec {
            types: &["AWS::RDS::DBInstance"],
            version: "EngineVersion",
            classes: &["databaseinstance", "cfndbinstance", "dbinstance"],
        },
        Kind::RdsCluster => CfnSpec {
            types: &["AWS::RDS::DBCluster"],
            version: "EngineVersion",
            classes: &[
                "databasecluster",
                "cfndbcluster",
                "serverlesscluster",
                "dbcluster",
            ],
        },
        Kind::ElasticacheGroup => CfnSpec {
            types: &[
                "AWS::ElastiCache::ReplicationGroup",
                "AWS::ElastiCache::CacheCluster",
            ],
            version: "EngineVersion",
            classes: &["cfnreplicationgroup", "cfncachecluster", "replicationgroup"],
        },
        Kind::OpensearchDomain => CfnSpec {
            types: &[
                "AWS::OpenSearchService::Domain",
                "AWS::Elasticsearch::Domain",
            ],
            version: "EngineVersion",
            classes: &["domain", "cfndomain"],
        },
        Kind::EksNodegroup => CfnSpec {
            types: &["AWS::EKS::Nodegroup"],
            version: "ReleaseVersion",
            classes: &["nodegroup", "cfnnodegroup"],
        },
        Kind::LambdaFunction => CfnSpec {
            types: &["AWS::Lambda::Function", "AWS::Serverless::Function"],
            version: "Runtime",
            classes: &["function"],
        },
        Kind::AutoScalingGroup | Kind::Ec2Instance => CfnSpec {
            types: &[
                "AWS::AutoScaling::AutoScalingGroup",
                "AWS::EC2::LaunchTemplate",
                "AWS::EC2::Instance",
            ],
            version: "ImageId",
            classes: &["autoscalinggroup", "launchtemplate", "instance"],
        },
        _ => CfnSpec {
            types: &[],
            version: "",
            classes: &[],
        },
    }
}

fn indent(l: &str) -> usize {
    l.len() - l.trim_start().len()
}

/// A logical id's resource in a template: the line of its key, and the
/// lines of the resource (to the next key at the same depth).
pub(crate) fn template_resource<'a>(
    lines: &[&'a str],
    logical: &str,
    types: &[&str],
) -> Option<(usize, Vec<(usize, &'a str)>)> {
    let key = re(&format!(r#"^\s*["']?{}["']?\s*:"#, regex::escape(logical)));
    for (i, l) in lines.iter().enumerate() {
        if !key.is_match(l) {
            continue;
        }
        let base = indent(l);
        let body: Vec<(usize, &str)> = lines
            .iter()
            .enumerate()
            .skip(i + 1)
            .take_while(|(_, x)| x.trim().is_empty() || indent(x) > base)
            .map(|(n, x)| (n, *x))
            .collect();
        let typed = body
            .iter()
            .take(12)
            .any(|(_, x)| types.iter().any(|t| x.contains(t)));
        if typed || types.is_empty() {
            return Some((i, body));
        }
    }
    None
}

fn unquote(s: &str) -> String {
    s.trim()
        .trim_end_matches(',')
        .trim()
        .trim_matches(|c| c == '"' || c == '\'')
        .to_string()
}

/// The line that sets `property` for a template resource: the property
/// itself when it is a literal, or the default of the parameter it refers
/// to.
fn template_edit(
    path: &str,
    lines: &[&str],
    body: &[(usize, &str)],
    property: &str,
) -> Option<Edit> {
    let prop = re(&format!(
        r#"^\s*["']?{}["']?\s*:\s*(.*)$"#,
        regex::escape(property)
    ));
    let (n, value) = body
        .iter()
        .find_map(|(n, l)| prop.captures(l).map(|c| (*n, c[1].trim().to_string())))?;
    let reference = re(r#"(?:!Ref\s+|["']?Ref["']?\s*:\s*)["']?([A-Za-z0-9]+)"#)
        .captures(&value)
        .map(|c| c[1].to_string())
        .or_else(|| {
            // `EngineVersion:` then `Ref: Param` on the next line.
            value.is_empty().then(|| {
                lines
                    .get(n + 1)
                    .and_then(|l| re(r#"["']?Ref["']?\s*:\s*["']?([A-Za-z0-9]+)"#).captures(l))
                    .map(|c| c[1].to_string())
            })?
        });
    if let Some(param) = reference {
        let (_, pbody) = template_resource(lines, &param, &[])?;
        let default = re(r#"^\s*["']?Default["']?\s*:\s*(.*)$"#);
        let (dn, dv) = pbody
            .iter()
            .find_map(|(n, l)| default.captures(l).map(|c| (*n, unquote(&c[1]))))?;
        return Some(Edit {
            file: path.to_string(),
            line: (dn + 1) as u32,
            attribute: param,
            value: dv,
            via: "parameter".into(),
            shared: "the stack may be deployed with a value for this parameter set outside the template (a deploy command, a pipeline, `samconfig.toml`): the default changes nothing where one is".into(),
        });
    }
    if value.starts_with('!') || value.starts_with('{') || value.is_empty() {
        return None;
    }
    Some(Edit {
        file: path.to_string(),
        line: (n + 1) as u32,
        attribute: property.to_string(),
        value: unquote(&value),
        via: "template".into(),
        shared: String::new(),
    })
}

/// A construct created in CDK code: its class, id and line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Construct {
    pub(crate) class: String,
    pub(crate) id: String,
    pub(crate) line: usize,
}

pub(crate) fn constructs(content: &str) -> Vec<Construct> {
    // `new rds.DatabaseCluster(this, 'Orders', ...` and
    // `rds.DatabaseCluster(self, "Orders", ...`.
    let call = re(
        r#"(?:new\s+)?([A-Za-z_][\w.]*)\s*\(\s*(?:this|self|scope|stack|app|[a-z]\w*)\s*,\s*["'`]([^"'`$]+)["'`]"#,
    );
    let mut out = Vec::new();
    for (i, l) in content.lines().enumerate() {
        for c in call.captures_iter(l) {
            out.push(Construct {
                class: c[1].to_string(),
                id: c[2].to_string(),
                line: i,
            });
        }
    }
    out
}

fn alnum(s: &str) -> String {
    s.chars().filter(|c| c.is_ascii_alphanumeric()).collect()
}

/// The readable part of a CDK logical id (`OrdersDatabase1A2B3C4D` is
/// `OrdersDatabase` and an 8-character hash). `None` for an id a person
/// wrote.
pub fn cdk_readable(logical: &str) -> Option<&str> {
    let at = logical.len().checked_sub(8)?;
    let (head, tail) = logical.split_at_checked(at)?;
    (!head.is_empty()
        && tail
            .chars()
            .all(|c| c.is_ascii_digit() || ('A'..='F').contains(&c))
        && tail.chars().any(|c| c.is_ascii_digit()))
    .then_some(head)
}

/// Everything read of a team's repositories, prepared once for every
/// resource placed against it: each file's tool, and Terraform parsed.
pub struct Code<'a> {
    pub files: &'a [SourceFile],
    pub mentions: &'a [Mention],
    tools: Vec<Option<&'static str>>,
    terraform: terraform::Index<'a>,
}

impl<'a> Code<'a> {
    pub fn new(files: &'a [SourceFile], mentions: &'a [Mention]) -> Code<'a> {
        Code {
            files,
            mentions,
            tools: files.iter().map(|f| tool_of(&f.path, &f.content)).collect(),
            terraform: terraform::index(files),
        }
    }

    /// Each file with the tool it is written for.
    fn tooled(&self) -> impl Iterator<Item = (&'a SourceFile, &'static str)> + '_ {
        self.files
            .iter()
            .zip(self.tools.iter())
            .filter_map(|(f, t)| t.map(|t| (f, t)))
    }
}

/// The resource a stack made, in the team's code.
pub fn in_stack(
    r: &Resource,
    stack: &StackRef,
    files: &[SourceFile],
    mentions: &[Mention],
) -> Option<Location> {
    in_stack_of(r, stack, &Code::new(files, mentions))
}

fn in_stack_of(r: &Resource, stack: &StackRef, code: &Code) -> Option<Location> {
    let (files, mentions) = (code.files, code.mentions);
    let spec = cfn_spec(r.kind);
    let names_stack = |repo: &str| {
        mentions
            .iter()
            .any(|m| m.repo == repo && m.term == stack.stack)
            || files
                .iter()
                .any(|f| f.repo == repo && f.content.contains(&stack.stack))
    };
    let env = {
        let e = environment_of(&[&stack.stack]);
        if e.is_empty() {
            environment_of_resource(r)
        } else {
            e
        }
    };
    let base = |f: &SourceFile, tool: &str| Location {
        repo: f.repo.clone(),
        file: f.path.clone(),
        how: stack.how.into(),
        tool: tool.to_string(),
        stack: stack.stack.clone(),
        environment: env.clone(),
        ..Default::default()
    };
    if stack.logical_id.is_empty() {
        return None;
    }
    // A template that has the logical id as a key, with the right type.
    let mut found: Vec<Location> = Vec::new();
    for (f, tool) in code.tooled() {
        if !matches!(tool, "cloudformation" | "sam" | "serverless") {
            continue;
        }
        let lines: Vec<&str> = f.content.lines().collect();
        let Some((at, body)) = template_resource(&lines, &stack.logical_id, spec.types) else {
            continue;
        };
        let mut l = base(f, tool);
        l.address = stack.logical_id.clone();
        l.line = (at + 1) as u32;
        l.edit = template_edit(&f.path, &lines, &body, spec.version);
        l.evidence.push(format!(
            "stack `{}` made it as `{}` ({}), and this template defines a resource with that logical id",
            stack.stack,
            stack.logical_id,
            if stack.how == "cloudformation_tag" {
                "its CloudFormation tags"
            } else {
                "CloudFormation's answer for its physical id"
            }
        ));
        found.push(l);
    }
    if !found.is_empty() {
        let named: Vec<usize> = found
            .iter()
            .enumerate()
            .filter(|(_, l)| names_stack(&l.repo))
            .map(|(i, _)| i)
            .collect();
        let pick = match (found.len(), named.len()) {
            (1, _) => 0,
            (_, 1) => named[0],
            _ => {
                let mut l = found[0].clone();
                l.confidence = Confidence::Low;
                l.evidence.push(format!(
                    "{} templates define a resource with that logical id; CoderHelm cannot tell which one the stack was deployed from",
                    found.len()
                ));
                l.candidates = found
                    .iter()
                    .skip(1)
                    .take(5)
                    .map(|c| format!("{}:{} {}", c.repo, c.file, c.address))
                    .collect();
                return Some(l);
            }
        };
        let mut l = found.swap_remove(pick);
        if names_stack(&l.repo) {
            l.confidence = Confidence::High;
            l.evidence
                .push(format!("the repository names the stack `{}`", stack.stack));
        } else {
            l.confidence = Confidence::Medium;
            l.evidence.push(format!(
                "the stack name `{}` is not written in the repository: the template is the only one that fits",
                stack.stack
            ));
        }
        if l.edit.is_none() {
            l.evidence.push(format!(
                "the template does not set `{}` as a plain value CoderHelm can read",
                spec.version
            ));
        }
        return Some(l);
    }
    // CDK: the logical id is the construct path's ids and a hash.
    let readable = cdk_readable(&stack.logical_id)?;
    let want = readable.to_ascii_lowercase();
    let mut best: Option<(i32, &SourceFile, Construct)> = None;
    let mut ties = 0;
    for (f, tool) in code.tooled() {
        if tool != "cdk" {
            continue;
        }
        for c in constructs(&f.content) {
            let id = alnum(&c.id).to_ascii_lowercase();
            if id.len() < 3 || !want.contains(&id) {
                continue;
            }
            let class = c
                .class
                .rsplit('.')
                .next()
                .unwrap_or("")
                .to_ascii_lowercase();
            let right_class = spec.classes.iter().any(|k| class.contains(k));
            let score = (if want == id {
                60
            } else if want.ends_with(&id) || want.starts_with(&id) {
                40
            } else {
                20
            }) + if right_class { 40 } else { 0 }
                + id.len().min(20) as i32;
            match &best {
                Some((s, _, _)) if *s > score => {}
                Some((s, _, _)) if *s == score => ties += 1,
                _ => {
                    ties = 0;
                    best = Some((score, f, c));
                }
            }
        }
    }
    let (score, f, c) = best?;
    let mut l = base(f, "cdk");
    l.how = "cdk_logical_id".into();
    l.address = c.id.clone();
    l.line = (c.line + 1) as u32;
    l.evidence.push(format!(
        "stack `{}` made it as `{}`; CDK builds that id from construct ids and a hash, and `{}` creates the construct `{}` here",
        stack.stack, stack.logical_id, c.class, c.id
    ));
    // The version the construct is given: within the call.
    let lines: Vec<&str> = f.content.lines().collect();
    let version = re(
        r#"((?:[A-Za-z_]\w*\.)*[A-Za-z]*(?:EngineVersion|KubernetesVersion|Runtime)\.[A-Za-z0-9_]+(?:\([^)]*\))?)"#,
    );
    l.edit = lines
        .iter()
        .enumerate()
        .skip(c.line)
        .take(60)
        .find_map(|(n, x)| version.captures(x).map(|m| (n, m[1].to_string())))
        .map(|(n, v)| Edit {
            file: f.path.clone(),
            line: (n + 1) as u32,
            attribute: "version".into(),
            value: v,
            via: "construct".into(),
            shared: String::new(),
        });
    let stack_named = names_stack(&f.repo);
    l.confidence = if ties > 0 || score < 60 {
        l.evidence.push(
            "more than one construct fits that id about as well, or the construct is not of the expected kind".into(),
        );
        Confidence::Low
    } else if stack_named && score >= 80 {
        l.evidence
            .push(format!("the repository names the stack `{}`", stack.stack));
        Confidence::High
    } else {
        l.evidence.push(format!(
            "the stack name `{}` is not written in the repository",
            stack.stack
        ));
        Confidence::Medium
    };
    Some(l)
}

/// What to search a repository for, for one resource, strongest first.
pub fn search_terms(r: &Resource) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |s: &str| {
        let s = s.trim();
        if s.len() >= 4 && !out.iter().any(|x| x == s) {
            out.push(s.to_string());
        }
    };
    if let Some(l) = r.tags.get("aws:cloudformation:logical-id") {
        push(l);
        if let Some(readable) = cdk_readable(l) {
            push(readable);
        }
    }
    if let Some(s) = r.tags.get("aws:cloudformation:stack-name") {
        push(s);
    }
    if !r.versions.ami_id.is_empty() {
        push(&r.versions.ami_id);
    }
    match terraform::generated_prefix(&r.name) {
        // A generated name is written nowhere; its prefix may be.
        Some(p) if p != "terraform-" => push(p.trim_end_matches('-')),
        Some(_) => {}
        None => push(&r.name),
    }
    // A group that is an ARN ends in an id nobody writes in code.
    if !r.group.starts_with("arn:") && terraform::generated_prefix(&r.group).is_none() {
        push(&r.group);
    }
    if let Some(lt) = r.tags.get("coderhelm:launch-template") {
        push(lt);
    }
    for k in ["subnet_group", "parameter_group", "cluster"] {
        if let Some(v) = r.facts.get(k) {
            if !v.starts_with("default") {
                push(v);
            }
        }
    }
    out
}

/// The names to search for across a team's resources, at most `cap`:
/// resources with something pending first, and every resource's strongest
/// name before any resource's second.
pub fn ranked_terms(resources: &[Resource], cap: usize) -> Vec<String> {
    let mut order: Vec<&Resource> = resources.iter().collect();
    order.sort_by_key(|r| {
        (
            r.pending.is_empty(),
            !matches!(
                r.kind,
                Kind::AutoScalingGroup | Kind::Ec2Instance | Kind::EksNodegroup
            ),
            r.id.clone(),
        )
    });
    let per: Vec<Vec<String>> = order.iter().map(|r| search_terms(r)).collect();
    let mut out: Vec<String> = Vec::new();
    let deepest = per.iter().map(Vec::len).max().unwrap_or(0);
    'all: for rank in 0..deepest {
        for terms in &per {
            if let Some(t) = terms.get(rank) {
                if !out.contains(t) {
                    if out.len() >= cap {
                        break 'all;
                    }
                    out.push(t.clone());
                }
            }
        }
    }
    out
}

/// The repository a tag names (`org/repo` or a GitHub URL), when it is
/// one of the team's.
fn tagged_repo(r: &Resource, team_repos: &[String]) -> Option<String> {
    super::inventory::repo_from_tags(&r.tags, team_repos)
}

/// The name written in an infrastructure file: the last resort.
fn by_name(r: &Resource, code: &Code) -> Option<Location> {
    let spec = cfn_spec(r.kind);
    let tf = terraform::spec(r.kind);
    let mut hits: Vec<Location> = Vec::new();
    for term in search_terms(r) {
        for (f, tool) in code.tooled() {
            let Some(line) = super::inventory::line_of(&f.content, &term) else {
                continue;
            };
            let mut l = Location {
                repo: f.repo.clone(),
                file: f.path.clone(),
                line: line as u32,
                how: "search".into(),
                tool: tool.into(),
                environment: environment_of(&[&f.path]),
                ..Default::default()
            };
            let mut fits = false;
            if matches!(tool, "terraform" | "opentofu") && f.path.ends_with(".tfvars") {
                continue;
            }
            if matches!(tool, "terraform" | "opentofu") {
                if let Some((address, at)) = super::inventory::terraform_address(&f.content, line) {
                    fits = tf.as_ref().is_some_and(|s| {
                        s.types
                            .iter()
                            .any(|t| address.starts_with(&format!("{t}.")))
                    });
                    l.address = address;
                    l.line = at;
                }
            } else if matches!(tool, "cloudformation" | "sam" | "serverless") {
                // The resource the line is in: the nearest key above it
                // whose body names one of the kind's types.
                let lines: Vec<&str> = f.content.lines().collect();
                let key = re(r#"^\s*["']?([A-Za-z0-9]+)["']?\s*:\s*\{?\s*$"#);
                for i in (0..line.min(lines.len())).rev() {
                    let Some(c) = key.captures(lines[i]) else {
                        continue;
                    };
                    if let Some((at, body)) = template_resource(&lines, &c[1], spec.types) {
                        if at == i && !spec.types.is_empty() {
                            fits = true;
                            l.address = c[1].to_string();
                            l.line = (at + 1) as u32;
                            l.edit = template_edit(&f.path, &lines, &body, spec.version);
                            break;
                        }
                    }
                }
            }
            l.evidence.push(format!("`{term}` is written in this file"));
            l.confidence = if fits {
                Confidence::Medium
            } else {
                l.evidence.push(
                    "CoderHelm could not tell that the code around it defines this resource".into(),
                );
                Confidence::Low
            };
            hits.push(l);
        }
        if !hits.is_empty() {
            break;
        }
    }
    hits.sort_by_key(|h| std::cmp::Reverse(h.confidence));
    let mut best = hits.first()?.clone();
    let same: Vec<&Location> = hits
        .iter()
        .skip(1)
        .filter(|h| h.confidence == best.confidence)
        .collect();
    if !same.is_empty() {
        best.confidence = Confidence::Low;
        best.evidence.push(format!(
            "the name is written in {} files; CoderHelm cannot tell which defines it",
            same.len() + 1
        ));
        best.candidates = same
            .iter()
            .take(5)
            .map(|c| {
                format!("{}:{} {}", c.repo, c.file, c.address)
                    .trim()
                    .to_string()
            })
            .collect();
    }
    Some(best)
}

/// Where a resource is defined, from everything read: the files of the
/// team's repositories and the places its names are mentioned.
pub fn locate(
    r: &Resource,
    team_repos: &[String],
    files: &[SourceFile],
    mentions: &[Mention],
) -> Option<Location> {
    locate_in(r, team_repos, &Code::new(files, mentions))
}

/// [`locate`] against code prepared once ([`Code::new`]) for a whole read.
pub fn locate_in(r: &Resource, team_repos: &[String], code: &Code) -> Option<Location> {
    let tagged = tagged_repo(r, team_repos);
    // A repository its tag names: only that repository's files count.
    let scoped: Vec<SourceFile>;
    let narrowed: Code;
    let code: &Code = match &tagged {
        Some(t) => {
            scoped = code
                .files
                .iter()
                .filter(|f| &f.repo == t)
                .cloned()
                .collect();
            narrowed = Code::new(&scoped, code.mentions);
            &narrowed
        }
        None => code,
    };
    let stack = stack_ref(r);
    let mut found: Vec<Location> = Vec::new();
    if let Some(s) = &stack {
        found.extend(in_stack_of(r, s, code));
    }
    found.extend(terraform::locate_in(
        &terraform::Live::of(r),
        &code.terraform,
    ));
    if found.iter().all(|l| l.confidence == Confidence::Low) {
        found.extend(by_name(r, code));
    }
    found.sort_by_key(|l| std::cmp::Reverse(l.confidence));
    if let Some(mut l) = found.into_iter().next() {
        if tagged.is_some() {
            l.evidence
                .push(format!("its repository tag names `{}`", l.repo));
        }
        if l.environment.is_empty() {
            l.environment = environment_of_resource(r);
        }
        if let Some(s) = &stack {
            l.stack = s.stack.clone();
        }
        return Some(l);
    }
    if let Some(repo) = tagged {
        return Some(Location {
            repo,
            how: "repo_tag".into(),
            confidence: Confidence::Low,
            evidence: vec![
                "its repository tag names this repository; CoderHelm did not find the file that defines it there".into(),
            ],
            environment: environment_of_resource(r),
            ..Default::default()
        });
    }
    stack.map(|s| Location {
        how: s.how.into(),
        tool: "cloudformation".into(),
        address: s.logical_id.clone(),
        confidence: Confidence::Low,
        evidence: vec![format!(
            "CloudFormation stack `{}` made it{}; no connected repository has a template or CDK construct for it",
            s.stack,
            if s.logical_id.is_empty() {
                String::new()
            } else {
                format!(" as `{}`", s.logical_id)
            }
        )],
        environment: environment_of_resource(r),
        stack: s.stack,
        ..Default::default()
    })
}

/// A sentence's first letter in upper case.
pub fn sentence(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

/// What a location says in one line, for a card or a ticket.
pub fn describe(l: &Location) -> String {
    let how = match l.how.as_str() {
        "cloudformation_tag" => "its CloudFormation stack tags",
        "cloudformation_lookup" => "CloudFormation's record of the stack that owns it",
        "cdk_logical_id" => "its CloudFormation logical id, matched to a CDK construct",
        "terraform_name" => "its identifier in Terraform",
        "terraform_generated_name" => "its Terraform-generated name's prefix and its settings",
        "terraform_attributes" => "its type and settings in Terraform",
        "terraform_module" => "a Terraform module call and its inputs",
        "repo_tag" => "its repository tag",
        _ => "its name in the code",
    };
    let sure = match l.confidence {
        Confidence::High => "high",
        Confidence::Medium => "medium",
        Confidence::Low => "low",
    };
    format!("Found by {how} (confidence: {sure}).")
}

/// Facts about a set of resources that say which are the same thing in
/// different environments: the name with its environment word removed.
pub fn sibling_key(r: &Resource) -> String {
    let name = match terraform::generated_prefix(&r.name) {
        Some(p) => p,
        None => &r.name,
    };
    let words: Vec<String> = name
        .to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| !t.is_empty() && !ENVS.iter().any(|(_, names)| names.contains(t)))
        .map(String::from)
        .collect();
    format!("{:?}:{}:{}", r.kind, r.versions.engine, words.join("-"))
}

/// Tags a resource carries, as a map, for tests and callers that build one.
pub fn tags(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra_vuln::inventory::Versions;

    fn file(repo: &str, path: &str, content: &str) -> SourceFile {
        SourceFile {
            repo: repo.into(),
            path: path.into(),
            content: content.into(),
        }
    }

    fn db(name: &str, t: &[(&str, &str)]) -> Resource {
        Resource {
            id: format!("arn:aws:rds:us-east-1:111122223333:db:{name}"),
            kind: Kind::RdsInstance,
            name: name.into(),
            group: format!("arn:aws:rds:us-east-1:111122223333:db:{name}"),
            versions: Versions {
                engine: "postgres".into(),
                engine_version: "16.4".into(),
                ..Default::default()
            },
            tags: tags(t),
            ..Default::default()
        }
    }

    const CFN: &str = r#"AWSTemplateFormatVersion: "2010-09-09"
Parameters:
  DbVersion:
    Type: String
    Default: "16.4"
Resources:
  Queue:
    Type: AWS::SQS::Queue
  Database:
    Type: AWS::RDS::DBInstance
    DeletionPolicy: Snapshot
    Properties:
      Engine: postgres
      EngineVersion: "16.4"
      DBInstanceClass: db.t4g.medium
  Replica:
    Type: AWS::RDS::DBInstance
    Properties:
      EngineVersion: !Ref DbVersion
"#;

    #[test]
    fn files_are_recognised_by_what_is_in_them() {
        // CloudFormation at a neutral path.
        assert_eq!(tool_of("deploy/api.yaml", CFN), Some("cloudformation"));
        assert_eq!(
            tool_of("app.yml", "Transform: AWS::Serverless-2016-10-31\nResources:\n  Fn:\n    Type: AWS::Serverless::Function\n"),
            Some("sam")
        );
        assert_eq!(
            tool_of("services/api/serverless.yml", "service: api\nprovider:\n  name: aws\n  runtime: nodejs22.x\nfunctions:\n  a:\n    handler: a.h\n"),
            Some("serverless")
        );
        // CDK next to application code, not named a stack.
        assert_eq!(
            tool_of(
                "src/database.ts",
                "import * as rds from 'aws-cdk-lib/aws-rds';\n"
            ),
            Some("cdk")
        );
        assert_eq!(
            tool_of("app/storage.py", "from aws_cdk import aws_rds as rds\n"),
            Some("cdk")
        );
        assert_eq!(
            tool_of("index.ts", "import * as aws from '@pulumi/aws';\n"),
            Some("pulumi")
        );
        assert_eq!(
            tool_of("__main__.py", "import pulumi_aws as aws\n"),
            Some("pulumi")
        );
        assert_eq!(tool_of("main.tofu", ""), Some("opentofu"));
        assert_eq!(tool_of("envs/prod.tfvars", ""), Some("terraform"));
        // Not infrastructure code, whatever it is called.
        assert_eq!(
            tool_of("docker-compose.yml", "services:\n  web:\n    image: x\n"),
            None
        );
        assert_eq!(
            tool_of("src/app/page.tsx", "export default function Page() {}"),
            None
        );
        assert_eq!(tool_of("openapi.yaml", "openapi: 3.0.0\npaths: {}\n"), None);
        assert_eq!(
            tool_of("node_modules/aws-cdk-lib/x.ts", "aws-cdk-lib"),
            None
        );
        assert!(maybe_iac_path("src/database.ts") && !maybe_iac_path("src/database.test.ts"));
        assert!(iac_by_name("serverless.yml") && iac_by_name("Pulumi.prod.yaml"));
        assert!(iac_by_name("samconfig.toml") && iac_by_name("infra/app.py"));
        assert!(!iac_by_name("src/app/page.tsx") && !iac_by_name("docker-compose.yml"));
    }

    #[test]
    fn a_stack_tag_and_logical_id_lead_to_the_template_and_the_line() {
        let r = db(
            "api-prod-database-abc123",
            &[
                ("aws:cloudformation:stack-name", "api-prod"),
                ("aws:cloudformation:logical-id", "Database"),
            ],
        );
        let files = vec![
            file("acme/api", "deploy/api.yaml", CFN),
            file(
                "acme/api",
                ".github/deploy.sh",
                "aws cloudformation deploy --stack-name api-prod",
            ),
        ];
        let l = locate(&r, &["acme/api".into()], &files, &[]).unwrap();
        assert_eq!(
            (l.how.as_str(), l.tool.as_str()),
            ("cloudformation_tag", "cloudformation")
        );
        assert_eq!(l.confidence, Confidence::High);
        assert_eq!(
            (l.file.as_str(), l.address.as_str(), l.line),
            ("deploy/api.yaml", "Database", 9)
        );
        assert_eq!(l.stack, "api-prod");
        assert_eq!(l.environment, "production");
        let e = l.edit.unwrap();
        assert_eq!(
            (
                e.attribute.as_str(),
                e.value.as_str(),
                e.line,
                e.via.as_str()
            ),
            ("EngineVersion", "16.4", 14, "template")
        );
        // The physical name is nowhere in the template.
        assert!(!CFN.contains("api-prod-database-abc123"));
    }

    #[test]
    fn a_lookup_by_physical_id_works_where_tags_are_missing_and_a_parameter_is_followed() {
        let mut r = db("api-prod-replica-zz9", &[]);
        assert!(stack_ref(&r).is_none());
        // What the worker adds after asking CloudFormation.
        r.tags
            .insert("aws:cloudformation:stack-name".into(), "api-prod".into());
        r.tags
            .insert("aws:cloudformation:logical-id".into(), "Replica".into());
        r.tags.insert(LOOKUP_TAG.into(), "true".into());
        let files = vec![file("acme/api", "deploy/api.yaml", CFN)];
        let l = locate(&r, &["acme/api".into()], &files, &[]).unwrap();
        assert_eq!(l.how, "cloudformation_lookup");
        assert_eq!(
            l.confidence,
            Confidence::Medium,
            "the stack name is not in the repository"
        );
        let e = l.edit.unwrap();
        assert_eq!(
            (
                e.via.as_str(),
                e.attribute.as_str(),
                e.line,
                e.value.as_str()
            ),
            ("parameter", "DbVersion", 5, "16.4")
        );
        assert!(e.shared.contains("outside the template"));
        // A mention of the stack name in a workflow makes it sure.
        let m = vec![Mention {
            repo: "acme/api".into(),
            path: ".github/workflows/deploy.yml".into(),
            term: "api-prod".into(),
        }];
        assert_eq!(
            locate(&r, &["acme/api".into()], &files, &m)
                .unwrap()
                .confidence,
            Confidence::High
        );
    }

    #[test]
    fn two_templates_with_the_same_logical_id_are_low_and_listed() {
        let r = db(
            "x",
            &[
                ("aws:cloudformation:stack-name", "billing"),
                ("aws:cloudformation:logical-id", "Database"),
            ],
        );
        let files = vec![
            file("acme/api", "deploy/api.yaml", CFN),
            file("acme/billing", "template.yaml", CFN),
        ];
        let l = locate(&r, &[], &files, &[]).unwrap();
        assert_eq!(l.confidence, Confidence::Low);
        assert_eq!(l.candidates.len(), 1);
        assert!(!l.sure());
        // No template anywhere: the stack is named, with no file.
        let none = locate(&r, &[], &[], &[]).unwrap();
        assert_eq!((none.stack.as_str(), none.file.as_str()), ("billing", ""));
        assert!(!none.sure());
        assert!(none.evidence[0].contains("no connected repository"));
    }

    const CDK: &str = r#"import * as cdk from 'aws-cdk-lib';
import * as rds from 'aws-cdk-lib/aws-rds';

export class DataTier extends cdk.Stack {
  constructor(scope: cdk.App, id: string) {
    super(scope, id);
    const vpc = new ec2.Vpc(this, 'Network');
    new rds.DatabaseCluster(this, 'Ledger', {
      engine: rds.DatabaseClusterEngine.auroraPostgres({
        version: rds.AuroraPostgresEngineVersion.VER_16_4,
      }),
      vpc,
    });
  }
}
"#;

    #[test]
    fn a_cdk_logical_id_leads_to_the_construct_in_a_file_not_named_a_stack() {
        assert_eq!(cdk_readable("LedgerB7C4D2E1"), Some("Ledger"));
        assert_eq!(cdk_readable("Database"), None);
        let r = Resource {
            kind: Kind::RdsCluster,
            name: "datatier-ledgerb7c4d2e1-k3j2h1".into(),
            tags: tags(&[
                ("aws:cloudformation:stack-name", "DataTierProd"),
                ("aws:cloudformation:logical-id", "LedgerB7C4D2E1"),
            ]),
            ..Default::default()
        };
        let files = vec![
            file("acme/app", "src/data-tier.ts", CDK),
            file(
                "acme/app",
                "bin/app.ts",
                "import 'aws-cdk-lib';\nnew DataTier(app, 'DataTierProd');\n",
            ),
        ];
        let l = locate(&r, &[], &files, &[]).unwrap();
        assert_eq!((l.how.as_str(), l.tool.as_str()), ("cdk_logical_id", "cdk"));
        assert_eq!(
            (l.file.as_str(), l.address.as_str(), l.line),
            ("src/data-tier.ts", "Ledger", 8)
        );
        assert_eq!(l.confidence, Confidence::High);
        let e = l.edit.unwrap();
        assert_eq!((e.line, e.via.as_str()), (10, "construct"));
        assert_eq!(e.value, "rds.AuroraPostgresEngineVersion.VER_16_4");
    }

    #[test]
    fn terms_are_ranked_so_no_resource_is_starved() {
        let mut many: Vec<Resource> = (0..40)
            .map(|i| {
                let mut r = db(&format!("aaa-instance-{i:02}"), &[]);
                r.kind = Kind::Ec2Instance;
                r.group = format!("aaa-group-{i:02}");
                r.versions.ami_id = format!("ami-0000000000000{i:04}");
                r
            })
            .collect();
        // Sorted alphabetically, this one's names came after the cap.
        let mut late = db(
            "zeta-orders",
            &[("aws:cloudformation:logical-id", "ZetaOrders")],
        );
        late.pending.push(Default::default());
        many.push(late);
        let t = ranked_terms(&many, 30);
        assert_eq!(t.len(), 30);
        assert!(t.contains(&"ZetaOrders".to_string()), "{t:?}");
        // A Terraform-generated name is not searched for; its prefix is.
        let gen = db("orders-20240311094512345600000001", &[]);
        assert_eq!(search_terms(&gen), vec!["orders"]);
        assert!(search_terms(&db("terraform-20240311094512345600000001", &[])).is_empty());
    }

    #[test]
    fn a_name_in_a_file_is_the_last_resort_and_graded() {
        // Pulumi: recognised, searched by name, never sure enough alone.
        let r = db("reports-prod", &[]);
        let files = vec![file(
            "acme/app",
            "infra/index.ts",
            "import * as aws from '@pulumi/aws';\nnew aws.rds.Instance('reports', { identifier: 'reports-prod', engineVersion: '16.4' });\n",
        )];
        let l = locate(&r, &[], &files, &[]).unwrap();
        assert_eq!((l.how.as_str(), l.tool.as_str()), ("search", "pulumi"));
        assert_eq!(l.confidence, Confidence::Low);
        assert!(!l.sure());
        // A template that writes the identifier, with no stack records.
        let t = "Resources:\n  Reports:\n    Type: AWS::RDS::DBInstance\n    Properties:\n      DBInstanceIdentifier: reports-prod\n      EngineVersion: \"16.4\"\n";
        let l = locate(&r, &[], &[file("acme/app", "db.yaml", t)], &[]).unwrap();
        assert_eq!(
            (l.address.as_str(), l.confidence),
            ("Reports", Confidence::Medium)
        );
        assert_eq!(l.edit.unwrap().line, 6);
        assert!(locate(&r, &[], &[], &[]).is_none());
    }

    #[test]
    fn environments_and_siblings_are_read_from_names_and_tags() {
        assert_eq!(environment_of(&["envs/prod/main.tf"]), "production");
        assert_eq!(environment_of(&["orders-staging"]), "staging");
        assert_eq!(
            environment_of(&["product-catalog"]),
            "",
            "a word that only starts like one"
        );
        let p = db("orders-prod", &[]);
        let s = db("orders-staging", &[("Environment", "Staging")]);
        assert_eq!(environment_of_resource(&p), "production");
        assert_eq!(environment_of_resource(&s), "staging");
        assert_eq!(sibling_key(&p), sibling_key(&s));
        assert_ne!(sibling_key(&p), sibling_key(&db("billing-prod", &[])));
    }
}
