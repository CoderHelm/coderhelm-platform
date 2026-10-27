//! The real [`Env`]: read-only AWS lookups through the team's connection,
//! the team's code through its GitHub App installation, the cache in the
//! settings table, and a model call the service makes (metered there).
//!
//! Every AWS call here only describes, lists or gets. A call the role isn't
//! allowed is recorded by its IAM action in [`AwsFacts::denied`], so the
//! dashboard can say which permission would help.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use futures::StreamExt;

use super::{
    ecr_repo_name, image_ref, Anchor, AwsFacts, Cached, EcsFact, Env, ImageFact, LambdaFact,
    PastFix, StackFact, Target, CACHE_SECS,
};
use crate::live::aws::Session;
use crate::log_findings::{self as lf, Finding};
use crate::repo_read::{CodeReader, RepoHost, RepoInfo, SearchHit};

/// How long the AWS lookups may take in all.
const AWS_BUDGET: Duration = Duration::from_secs(20);
/// Most clusters, services and task definitions looked at.
const MAX_CLUSTERS: usize = 20;
const MAX_SERVICES: usize = 300;
const MAX_TASK_DEFS: usize = 80;
/// CloudFormation's own tag on every resource a stack made.
const STACK_TAG: &str = "aws:cloudformation:stack-name";
/// Most pages of a stack's resource list read (100 resources each).
const MAX_STACK_PAGES: usize = 5;
/// Most functions from the log group's stack looked up.
const MAX_STACK_FUNCTIONS: usize = 10;

/// A described ECS service: (cluster, service name, task definition ARN,
/// service tags, named directly by the target).
type Described = (String, String, String, BTreeMap<String, String>, bool);

/// A model call the service makes, metered by its own helper.
#[async_trait::async_trait]
pub trait Judge: Send + Sync {
    async fn judge(&self, system: &str, user: &str) -> Result<String, String>;
}

/// The real environment.
pub struct Live<'a> {
    /// A session on the team's log reader role for the target's account.
    pub session: Option<Session>,
    /// The team's code, through its GitHub App installation.
    pub host: Option<&'a dyn RepoHost>,
    pub dynamo: &'a DynamoClient,
    pub settings_table: &'a str,
    pub team_id: &'a str,
    pub judge: &'a dyn Judge,
}

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

/// Pure: the AWS error means the role isn't allowed the call.
pub fn is_refusal(code: Option<&str>) -> bool {
    matches!(
        code,
        Some(
            "AccessDenied"
                | "AccessDeniedException"
                | "UnauthorizedOperation"
                | "UnauthorizedException"
                | "AuthorizationError"
        )
    )
}

fn refused(e: &impl ProvideErrorMetadata) -> bool {
    let code = e.code();
    // ECS says ClientException for many things; only a permission one counts.
    if code == Some("ClientException") {
        return e
            .message()
            .is_some_and(|m| m.contains("not authorized") || m.contains("AccessDenied"));
    }
    is_refusal(code)
}

fn tag_map<'t>(
    pairs: impl IntoIterator<Item = (Option<&'t str>, Option<&'t str>)>,
) -> BTreeMap<String, String> {
    pairs
        .into_iter()
        .filter_map(|(k, v)| Some((k?.to_string(), v.unwrap_or("").to_string())))
        .collect()
}

impl Live<'_> {
    async fn gather(&self, s: &Session, target: &Target, facts: &mut AwsFacts) {
        let started = Instant::now();
        let left = || AWS_BUDGET.saturating_sub(started.elapsed());
        let in_time = |d: Duration| !d.is_zero();

        // The log group's own tags.
        if !target.log_group.is_empty() && !target.account.is_empty() {
            let logs = crate::live::aws::logs_client(s);
            let arn = format!(
                "arn:aws:logs:{}:{}:log-group:{}",
                s.region, target.account, target.log_group
            );
            match logs.list_tags_for_resource().resource_arn(arn).send().await {
                Ok(o) => {
                    facts.log_group_tags = o
                        .tags()
                        .map(|t| t.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                        .unwrap_or_default()
                }
                Err(e) if refused(&e) => facts.deny("logs:ListTagsForResource"),
                Err(_) => {}
            }
        }

        // Functions the log group's own stack made (one with its own log
        // group setting has no `/aws/lambda/` name to go by).
        let stack_functions = if in_time(left()) {
            self.stack_functions(s, facts).await
        } else {
            Vec::new()
        };
        if in_time(left()) {
            self.ecs(s, target, facts, started).await;
        }
        if in_time(left()) {
            self.lambda(s, target, &stack_functions, facts).await;
        }
        if in_time(left()) {
            self.images(s, facts).await;
        }
        stacks(facts);
    }

    /// The ECS services whose task definitions log to the group (read from
    /// each task definition's log configuration), and those named directly.
    async fn ecs(&self, s: &Session, target: &Target, facts: &mut AwsFacts, started: Instant) {
        let ecs = client!(aws_sdk_ecs, s);
        // (cluster, service ARN or name)
        let mut services: Vec<(String, String)> = target.ecs_services.clone();
        let named = services.len();
        let groups = target.log_groups();
        if !groups.is_empty() {
            let mut clusters = Vec::new();
            let mut next: Option<String> = None;
            loop {
                match ecs.list_clusters().set_next_token(next.take()).send().await {
                    Ok(o) => {
                        clusters.extend(o.cluster_arns().iter().cloned());
                        next = o.next_token().map(str::to_string);
                    }
                    Err(e) => {
                        if refused(&e) {
                            facts.deny("ecs:ListClusters");
                        }
                        break;
                    }
                }
                if next.is_none() || clusters.len() >= MAX_CLUSTERS {
                    break;
                }
            }
            clusters.truncate(MAX_CLUSTERS);
            'clusters: for c in &clusters {
                let mut next: Option<String> = None;
                loop {
                    if started.elapsed() > AWS_BUDGET / 2 {
                        break 'clusters;
                    }
                    match ecs
                        .list_services()
                        .cluster(c)
                        .max_results(100)
                        .set_next_token(next.take())
                        .send()
                        .await
                    {
                        Ok(o) => {
                            services
                                .extend(o.service_arns().iter().map(|a| (c.clone(), a.clone())));
                            next = o.next_token().map(str::to_string);
                        }
                        Err(e) => {
                            if refused(&e) {
                                facts.deny("ecs:ListServices");
                            }
                            break;
                        }
                    }
                    if next.is_none() || services.len() >= MAX_SERVICES {
                        break;
                    }
                }
                if services.len() >= MAX_SERVICES {
                    break;
                }
            }
        }
        if services.is_empty() {
            return;
        }
        // (cluster, service name, task definition ARN, service tags)
        let mut described: Vec<Described> = Vec::new();
        let mut by_cluster: BTreeMap<String, Vec<(String, bool)>> = BTreeMap::new();
        for (i, (c, svc)) in services.iter().enumerate() {
            by_cluster
                .entry(c.clone())
                .or_default()
                .push((svc.clone(), i < named));
        }
        for (cluster, svcs) in by_cluster {
            for chunk in svcs.chunks(10) {
                let names: Vec<String> = chunk.iter().map(|(n, _)| n.clone()).collect();
                match ecs
                    .describe_services()
                    .cluster(&cluster)
                    .set_services(Some(names))
                    .include(aws_sdk_ecs::types::ServiceField::Tags)
                    .send()
                    .await
                {
                    Ok(o) => {
                        for sv in o.services() {
                            let name = sv.service_name().unwrap_or_default().to_string();
                            let direct = chunk.iter().any(|(n, d)| {
                                *d && (n == &name || sv.service_arn() == Some(n.as_str()))
                            });
                            described.push((
                                cluster.clone(),
                                name,
                                sv.task_definition().unwrap_or_default().to_string(),
                                tag_map(sv.tags().iter().map(|t| (t.key(), t.value()))),
                                direct,
                            ));
                        }
                    }
                    Err(e) => {
                        if refused(&e) {
                            facts.deny("ecs:DescribeServices");
                        }
                    }
                }
            }
        }
        let mut arns: Vec<String> = described.iter().map(|d| d.2.clone()).collect();
        arns.sort();
        arns.dedup();
        arns.retain(|a| !a.is_empty());
        arns.truncate(MAX_TASK_DEFS);
        let ecs_ref = &ecs;
        let defs: Vec<(
            String,
            Result<
                aws_sdk_ecs::operation::describe_task_definition::DescribeTaskDefinitionOutput,
                bool,
            >,
        )> = futures::stream::iter(arns.into_iter().map(|arn| async move {
            let r = ecs_ref
                .describe_task_definition()
                .task_definition(&arn)
                .include(aws_sdk_ecs::types::TaskDefinitionField::Tags)
                .send()
                .await
                .map_err(|e| refused(&e));
            (arn, r)
        }))
        .buffer_unordered(8)
        .collect()
        .await;
        let mut by_arn = HashMap::new();
        for (arn, r) in defs {
            match r {
                Ok(o) => {
                    by_arn.insert(arn, o);
                }
                Err(true) => facts.deny("ecs:DescribeTaskDefinition"),
                Err(false) => {}
            }
        }
        for (cluster, service, arn, svc_tags, direct) in described {
            let Some(o) = by_arn.get(&arn) else {
                if direct {
                    facts.ecs.push(EcsFact {
                        cluster,
                        service,
                        task_definition: arn,
                        tags: svc_tags,
                        ..Default::default()
                    });
                }
                continue;
            };
            let Some(td) = o.task_definition() else {
                continue;
            };
            let logs_here = td.container_definitions().iter().any(|c| {
                c.log_configuration()
                    .and_then(|l| l.options())
                    .and_then(|o| o.get("awslogs-group"))
                    .is_some_and(|g| groups.contains(g))
            });
            if !logs_here && !direct {
                continue;
            }
            let mut labels = BTreeMap::new();
            let mut images = Vec::new();
            for c in td.container_definitions() {
                if let Some(i) = c.image() {
                    images.push(i.to_string());
                }
                if let Some(l) = c.docker_labels() {
                    labels.extend(l.iter().map(|(k, v)| (k.clone(), v.clone())));
                }
            }
            let mut tags = svc_tags;
            tags.extend(tag_map(o.tags().iter().map(|t| (t.key(), t.value()))));
            facts.ecs.push(EcsFact {
                cluster,
                service,
                task_definition: arn,
                family: td.family().unwrap_or_default().to_string(),
                images,
                labels,
                tags,
            });
        }
    }

    /// The Lambda functions behind the log group: the function whose default
    /// log group it is, those the log group's stack made, and those named
    /// directly. Only their tags are read (`lambda:ListTags`): a function's
    /// configuration carries its environment variables and `GetFunction` a
    /// link to its code, so neither is asked for.
    async fn lambda(
        &self,
        s: &Session,
        target: &Target,
        from_stack: &[String],
        facts: &mut AwsFacts,
    ) {
        let lambda = client!(aws_sdk_lambda, s);
        let names = function_names(target, from_stack);
        for name in names {
            let mut fact = LambdaFact {
                name: name.clone(),
                ..Default::default()
            };
            if let Some(arn) = function_arn(&s.region, &target.account, &name) {
                match lambda.list_tags().resource(arn).send().await {
                    Ok(o) => {
                        fact.tags = o
                            .tags()
                            .map(|t| t.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                            .unwrap_or_default()
                    }
                    Err(e) if refused(&e) => facts.deny("lambda:ListTags"),
                    // No such function (a log group outliving it): nothing to say.
                    Err(e) if e.code() == Some("ResourceNotFoundException") => continue,
                    Err(_) => {}
                }
            }
            facts.lambdas.push(fact);
        }
    }

    /// The Lambda functions the log group's CloudFormation stack made (the
    /// stack-name tag CloudFormation puts on the group), read from the
    /// stack's resource list: names and types only, never its parameters or
    /// outputs.
    async fn stack_functions(&self, s: &Session, facts: &mut AwsFacts) -> Vec<String> {
        let Some(stack) = facts.log_group_tags.get(STACK_TAG).cloned() else {
            return Vec::new();
        };
        let cfn = client!(aws_sdk_cloudformation, s);
        let mut resources: Vec<(String, String)> = Vec::new();
        let mut next: Option<String> = None;
        for _ in 0..MAX_STACK_PAGES {
            match cfn
                .list_stack_resources()
                .stack_name(&stack)
                .set_next_token(next.take())
                .send()
                .await
            {
                Ok(o) => {
                    resources.extend(o.stack_resource_summaries().iter().map(|r| {
                        (
                            r.resource_type().unwrap_or_default().to_string(),
                            r.physical_resource_id().unwrap_or_default().to_string(),
                        )
                    }));
                    next = o.next_token().map(str::to_string);
                }
                Err(e) => {
                    if refused(&e) {
                        facts.deny("cloudformation:ListStackResources");
                    }
                    break;
                }
            }
            if next.is_none() {
                break;
            }
        }
        stack_lambdas(&resources)
    }

    /// The ECR images the containers and functions run: the manifest's
    /// annotations (no layers are read) and the repository's tags.
    async fn images(&self, s: &Session, facts: &mut AwsFacts) {
        let mut images: Vec<String> = facts
            .ecs
            .iter()
            .flat_map(|e| e.images.iter().cloned())
            .chain(facts.lambdas.iter().map(|l| l.image.clone()))
            .filter(|i| !i.is_empty())
            .collect();
        images.sort();
        images.dedup();
        if images.is_empty() {
            return;
        }
        let ecr = client!(aws_sdk_ecr, s);
        for image in images.into_iter().take(6) {
            let Some(repo) = ecr_repo_name(&image) else {
                continue;
            };
            let acct = image.split('.').next().unwrap_or_default().to_string();
            // Only images in the session's region can be read with it.
            if !image.contains(&format!(".dkr.ecr.{}.", s.region)) {
                facts.images.push(ImageFact {
                    image,
                    ecr_repo: repo,
                    ..Default::default()
                });
                continue;
            }
            let mut fact = ImageFact {
                image: image.clone(),
                ecr_repo: repo.clone(),
                ..Default::default()
            };
            if let Some((r, is_digest)) = image_ref(&image) {
                let id = if is_digest {
                    aws_sdk_ecr::types::ImageIdentifier::builder().image_digest(r)
                } else {
                    aws_sdk_ecr::types::ImageIdentifier::builder().image_tag(r)
                };
                match ecr
                    .batch_get_image()
                    .registry_id(&acct)
                    .repository_name(&repo)
                    .image_ids(id.build())
                    .accepted_media_types("application/vnd.oci.image.index.v1+json")
                    .accepted_media_types("application/vnd.oci.image.manifest.v1+json")
                    .accepted_media_types("application/vnd.docker.distribution.manifest.v2+json")
                    .accepted_media_types(
                        "application/vnd.docker.distribution.manifest.list.v2+json",
                    )
                    .send()
                    .await
                {
                    Ok(o) => {
                        if let Some(m) = o.images().first().and_then(|i| i.image_manifest()) {
                            fact.annotations = manifest_annotations(m);
                        }
                    }
                    Err(e) => {
                        if refused(&e) {
                            facts.deny("ecr:BatchGetImage");
                        }
                    }
                }
            }
            let arn = format!("arn:aws:ecr:{}:{acct}:repository/{repo}", s.region);
            match ecr.list_tags_for_resource().resource_arn(arn).send().await {
                Ok(o) => {
                    fact.tags = tag_map(o.tags().iter().map(|t| (Some(t.key()), Some(t.value()))))
                }
                Err(e) => {
                    if refused(&e) {
                        facts.deny("ecr:ListTagsForResource");
                    }
                }
            }
            facts.images.push(fact);
        }
    }
}

/// The CloudFormation stacks that made the resources (CloudFormation's own
/// stack-name tag on them). Only their names: a stack's description comes
/// with its parameters, which aren't read. The stack's own tags are on the
/// resources it made, which are already read.
fn stacks(facts: &mut AwsFacts) {
    let mut names: Vec<String> = facts
        .ecs
        .iter()
        .map(|e| &e.tags)
        .chain(facts.lambdas.iter().map(|l| &l.tags))
        .chain(std::iter::once(&facts.log_group_tags))
        .filter_map(|t| t.get(STACK_TAG).cloned())
        .collect();
    names.sort();
    names.dedup();
    for name in names.into_iter().take(4) {
        facts.stacks.push(StackFact {
            name,
            ..Default::default()
        });
    }
}

/// Pure: the functions to look up: those named directly, the one whose
/// default log group (`/aws/lambda/<function>`) each group is, then those
/// the group's stack made; once each.
pub fn function_names(target: &Target, from_stack: &[String]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let mut add = |n: &str| {
        if !n.is_empty() && !n.contains('/') && !names.iter().any(|x| x == n) {
            names.push(n.to_string());
        }
    };
    for f in &target.functions {
        add(f);
    }
    for g in target.log_groups() {
        if let Some(n) = g.strip_prefix("/aws/lambda/") {
            add(n);
        }
    }
    for f in from_stack.iter().take(MAX_STACK_FUNCTIONS) {
        add(f);
    }
    names
}

/// Pure: a function's ARN, for its tags. `None` without the account.
pub fn function_arn(region: &str, account: &str, name: &str) -> Option<String> {
    (crate::log_reader::is_account(account) && !region.is_empty() && !name.is_empty())
        .then(|| format!("arn:aws:lambda:{region}:{account}:function:{name}"))
}

/// Pure: the Lambda functions in a stack's resource list (type, physical id).
pub fn stack_lambdas(resources: &[(String, String)]) -> Vec<String> {
    resources
        .iter()
        .filter(|(t, id)| t == "AWS::Lambda::Function" && !id.is_empty())
        .map(|(_, id)| id.clone())
        .collect()
}

/// Pure: an image manifest's (or index's) annotations, and those of its
/// first manifest when it is an index.
pub fn manifest_annotations(manifest: &str) -> BTreeMap<String, String> {
    let v: serde_json::Value = serde_json::from_str(manifest).unwrap_or_default();
    let mut out = BTreeMap::new();
    let mut take = |a: &serde_json::Value| {
        if let Some(m) = a.as_object() {
            for (k, v) in m {
                if let Some(s) = v.as_str() {
                    out.entry(k.clone()).or_insert_with(|| s.to_string());
                }
            }
        }
    };
    take(&v["annotations"]);
    if let Some(ms) = v["manifests"].as_array() {
        for m in ms.iter().take(4) {
            take(&m["annotations"]);
        }
    }
    out
}

#[async_trait::async_trait]
impl Env for Live<'_> {
    async fn aws_facts(&self, target: &Target) -> AwsFacts {
        let Some(s) = &self.session else {
            return AwsFacts::default();
        };
        let mut facts = AwsFacts {
            connected: true,
            ..Default::default()
        };
        self.gather(s, target, &mut facts).await;
        facts
    }

    async fn anchor_changed(&self, _target: &Target, anchor: &Anchor) -> bool {
        let Some(s) = &self.session else {
            return false;
        };
        if !anchor.ecs.is_empty() {
            let ecs = client!(aws_sdk_ecs, s);
            for (cluster, service, td) in &anchor.ecs {
                if let Ok(o) = ecs
                    .describe_services()
                    .cluster(cluster)
                    .services(service)
                    .send()
                    .await
                {
                    let now = o.services().first().and_then(|s| s.task_definition());
                    if now.is_some_and(|n| n != td) {
                        return true;
                    }
                }
            }
        }
        false
    }

    async fn search(&self, repos: &[String], query: &str) -> Result<Vec<SearchHit>, String> {
        let Some(host) = self.host else {
            return Ok(vec![]);
        };
        // GitHub code search wants the phrase quoted; a few repos per query.
        let q = format!("\"{}\"", query.replace('"', ""));
        let mut out = Vec::new();
        for chunk in repos.chunks(10).take(6) {
            match host.search(chunk, &q).await {
                Ok(h) => out.extend(h),
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(out)
    }

    async fn profile(&self, repo: &RepoInfo) -> Option<String> {
        let host = self.host?;
        let reader = CodeReader::new(host, vec![repo.clone()]);
        let o = reader.overview(&repo.name).await.ok()?;
        let mut text = String::new();
        for f in o.files.iter().take(4) {
            text.push_str(&format!(
                "[{}] {}\n",
                f.path,
                crate::truncate_str(&f.text, 600)
            ));
        }
        (!text.is_empty()).then_some(text)
    }

    async fn judge(&self, system: &str, user: &str) -> Result<String, String> {
        self.judge.judge(system, user).await
    }

    async fn load(&self, key: &str) -> Option<Cached> {
        let out = self
            .dynamo
            .get_item()
            .table_name(self.settings_table)
            .key("pk", AttributeValue::S(self.team_id.to_string()))
            .key("sk", AttributeValue::S(key.to_string()))
            .send()
            .await
            .ok()?;
        let data = out.item()?.get("data")?.as_s().ok()?;
        serde_json::from_str(data).ok()
    }

    async fn save(&self, key: &str, cached: &Cached) {
        let Ok(data) = serde_json::to_string(cached) else {
            return;
        };
        let ttl = chrono::Utc::now().timestamp().max(0) + CACHE_SECS + 86_400;
        if let Err(e) = self
            .dynamo
            .put_item()
            .table_name(self.settings_table)
            .item("pk", AttributeValue::S(self.team_id.to_string()))
            .item("sk", AttributeValue::S(key.to_string()))
            .item("data", AttributeValue::S(data))
            .item("ttl", AttributeValue::N(ttl.to_string()))
            .send()
            .await
        {
            tracing::warn!(error = %e, "Could not cache the resolved repo");
        }
    }
}

// ─── What the team already told CoderHelm ───────────────────────────────────

/// The team's enabled repos with their roles and descriptions.
pub async fn load_repos(dynamo: &DynamoClient, repos_table: &str, team_id: &str) -> Vec<RepoInfo> {
    let mut out = Vec::new();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let r = dynamo
            .query()
            .table_name(repos_table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
            .expression_attribute_values(":p", AttributeValue::S("REPO#".into()))
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(r) = r else {
            tracing::warn!(team_id, "Couldn't read the team's repositories");
            break;
        };
        for item in r.items() {
            let s = |k: &str| {
                item.get(k)
                    .and_then(|v| v.as_s().ok())
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty())
            };
            let enabled = item
                .get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
            let Some(name) = s("repo_name").filter(|n| n.contains('/')) else {
                continue;
            };
            if enabled {
                out.push(RepoInfo {
                    branch: s("default_branch").unwrap_or_else(|| "main".into()),
                    role: s("role"),
                    description: s("description"),
                    name,
                });
            }
        }
        match r.last_evaluated_key() {
            Some(k) if !k.is_empty() && out.len() < 500 => start = Some(k.clone()),
            _ => break,
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Pure: the repo saved for `log_group` and every other saved mapping.
pub fn saved_mappings(
    cfg: &lf::AutoFix,
    log_group: &str,
) -> (Option<String>, Vec<(String, String)>) {
    let saved = cfg.policy(log_group).repo;
    let related = cfg
        .overrides
        .iter()
        .filter(|o| o.log_group != log_group && !o.repo.is_empty())
        .map(|o| (o.log_group.clone(), o.repo.clone()))
        .collect();
    (saved, related)
}

/// Past fix runs for `log_group`: the findings' fix repos and whether
/// their pull requests merged (a few tickets read).
pub async fn past_fixes(
    dynamo: &DynamoClient,
    runs_table: &str,
    team_id: &str,
    findings: &[Finding],
    log_group: &str,
) -> Vec<PastFix> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for f in findings
        .iter()
        .filter(|f| f.source_log_group == log_group && !f.fix_repo.is_empty())
    {
        if !seen.insert(f.fix_ticket_id.clone()) || seen.len() > 5 {
            continue;
        }
        let runs = lf::runs_for_ticket(dynamo, runs_table, team_id, &f.fix_ticket_id)
            .await
            .unwrap_or_default();
        let (state, run) = lf::fix_state(&runs, &f.fix_started_at, chrono::Utc::now());
        // The repo the run worked in: its triage may have chosen another.
        let ran_in = run
            .map(|r| r.repo)
            .or_else(|| runs.iter().map(|r| r.repo.clone()).find(|r| !r.is_empty()))
            .unwrap_or_default();
        out.push(PastFix {
            repo: f.fix_repo.clone(),
            merged: state == lf::FixState::Merged,
            ran_in,
        });
    }
    out
}

/// The resources CloudWatch alarms watch, from their metric dimensions
/// (`LogGroupName`, `ServiceName` with `ClusterName`, `FunctionName`), as a
/// target. Read-only (`cloudwatch:DescribeAlarms`).
pub async fn alarm_target(
    session: &Session,
    account: &str,
    alarms: &[String],
) -> (Target, Vec<String>) {
    let mut t = Target {
        account: account.to_string(),
        alarms: alarms.to_vec(),
        ..Default::default()
    };
    let mut denied = Vec::new();
    if alarms.is_empty() {
        return (t, denied);
    }
    let cw = client!(aws_sdk_cloudwatch, session);
    match cw
        .describe_alarms()
        .set_alarm_names(Some(alarms.iter().take(100).cloned().collect()))
        .send()
        .await
    {
        Ok(o) => {
            for a in o.metric_alarms() {
                let dims: Vec<(&str, &str)> = a
                    .dimensions()
                    .iter()
                    .chain(
                        a.metrics()
                            .iter()
                            .filter_map(|m| m.metric_stat())
                            .filter_map(|s| s.metric())
                            .flat_map(|m| m.dimensions()),
                    )
                    .filter_map(|d| Some((d.name()?, d.value()?)))
                    .collect();
                apply_dimensions(&mut t, &dims);
                if let Some(d) = a.alarm_description() {
                    if t.about.len() < 1_000 {
                        t.about
                            .push_str(&format!("{}: {d}\n", a.alarm_name().unwrap_or_default()));
                    }
                }
            }
        }
        Err(e) => {
            if refused(&e) {
                denied.push("cloudwatch:DescribeAlarms".to_string());
            }
        }
    }
    (t, denied)
}

/// Pure: a target's resources from metric dimensions (AWS's own names for
/// them in CloudWatch metrics).
pub fn apply_dimensions(t: &mut Target, dims: &[(&str, &str)]) {
    let get = |k: &str| {
        dims.iter()
            .find(|(n, _)| *n == k)
            .map(|(_, v)| v.to_string())
    };
    if let Some(g) = get("LogGroupName") {
        if t.log_group.is_empty() {
            t.log_group = g;
        }
    }
    if let Some(svc) = get("ServiceName") {
        let cluster = get("ClusterName").unwrap_or_else(|| "default".into());
        if !t
            .ecs_services
            .iter()
            .any(|(c, s)| c == &cluster && s == &svc)
        {
            t.ecs_services.push((cluster, svc));
        }
    }
    if let Some(f) = get("FunctionName") {
        if !t.functions.contains(&f) {
            t.functions.push(f);
        }
    }
}

// ─── One call for a finding or alarms ───────────────────────────────────────

/// Where a team's records live, and its AWS access.
pub struct Team<'a> {
    pub dynamo: &'a DynamoClient,
    pub sts: &'a aws_sdk_sts::Client,
    pub settings_table: &'a str,
    pub repos_table: &'a str,
    pub runs_table: &'a str,
    pub insights_table: &'a str,
    pub team_id: &'a str,
}

/// Pure: a finding as a target, with its own sample lines.
pub fn finding_target(f: &Finding, samples: &[lf::Sample]) -> Target {
    let mut about = f.title.clone();
    for (label, v) in [
        ("Summary", &f.summary),
        ("Error pattern", &f.error_pattern),
        ("Suspected location", &f.suspected_location),
    ] {
        if !v.is_empty() {
            about.push_str(&format!("\n{label}: {v}"));
        }
    }
    Target {
        account: f.source_account_id.clone(),
        log_group: f.source_log_group.clone(),
        about,
        samples: samples
            .iter()
            .map(|s| super::LogLine {
                log_group: if s.log_group.is_empty() {
                    f.source_log_group.clone()
                } else {
                    s.log_group.clone()
                },
                message: s.message.clone(),
            })
            .collect(),
        ..Default::default()
    }
}

/// The repo for a finding: its saved mapping, or worked out from its own
/// log lines, AWS, the team's code and records (cached per log group).
pub async fn for_finding(
    team: &Team<'_>,
    host: Option<&dyn RepoHost>,
    judge: &dyn Judge,
    f: &Finding,
    cfg: &lf::AutoFix,
    findings: &[Finding],
    refresh: bool,
) -> super::Resolution {
    let session = lf::session_for(
        team.dynamo,
        team.sts,
        team.insights_table,
        team.team_id,
        &f.source_account_id,
    )
    .await
    .unwrap_or_else(|e| {
        tracing::warn!(error = %e, "Repo resolution: no AWS session");
        None
    });
    let samples = match &session {
        Some(s) => lf::fetch_samples(s, f, Duration::from_secs(15))
            .await
            .unwrap_or_default(),
        None => vec![],
    };
    let (saved, related) = saved_mappings(cfg, &f.source_log_group);
    let input = super::Input {
        target: finding_target(f, &samples),
        repos: load_repos(team.dynamo, team.repos_table, team.team_id).await,
        saved,
        related,
        past: past_fixes(
            team.dynamo,
            team.runs_table,
            team.team_id,
            findings,
            &f.source_log_group,
        )
        .await,
        refresh,
    };
    let env = Live {
        session,
        host,
        dynamo: team.dynamo,
        settings_table: team.settings_table,
        team_id: team.team_id,
        judge,
    };
    super::resolve(&env, &input, chrono::Utc::now()).await
}

/// The repo for the resources CloudWatch alarms watch, in `account`.
pub async fn for_alarms(
    team: &Team<'_>,
    host: Option<&dyn RepoHost>,
    judge: &dyn Judge,
    account: &str,
    alarms: &[String],
    cfg: &lf::AutoFix,
) -> super::Resolution {
    let session = lf::session_for(
        team.dynamo,
        team.sts,
        team.insights_table,
        team.team_id,
        account,
    )
    .await
    .ok()
    .flatten();
    let (target, denied) = match &session {
        Some(s) => alarm_target(s, account, alarms).await,
        None => (
            Target {
                account: account.to_string(),
                alarms: alarms.to_vec(),
                ..Default::default()
            },
            vec![],
        ),
    };
    let (saved, related) = saved_mappings(cfg, &target.log_group);
    let input = super::Input {
        // An alarm's rule has its own repo: a log group's saved repo is
        // only evidence here.
        saved: None,
        related: saved
            .map(|r| (target.log_group.clone(), r))
            .into_iter()
            .chain(related)
            .collect(),
        target,
        repos: load_repos(team.dynamo, team.repos_table, team.team_id).await,
        past: vec![],
        refresh: false,
    };
    let env = Live {
        session,
        host,
        dynamo: team.dynamo,
        settings_table: team.settings_table,
        team_id: team.team_id,
        judge,
    };
    let mut r = super::resolve(&env, &input, chrono::Utc::now()).await;
    for d in denied {
        if !r.permissions.contains(&d) {
            r.permissions.push(d);
        }
    }
    r
}
