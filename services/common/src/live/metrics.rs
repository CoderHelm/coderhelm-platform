//! CloudWatch metrics in a team's AWS account, through its log reader role.
//!
//! Two calls only, both reads: ListMetrics (what exists, with its dimension
//! values) and GetMetricData (numbers over time). Any namespace may be read:
//! the team's role is the boundary (its policy allows
//! `cloudwatch:GetMetricData` and `cloudwatch:ListMetrics`), so a namespace
//! the role can't read comes back as AWS's refusal, named as the install's
//! setup item "AWS role reads", never as a refusal of ours. A run once
//! couldn't read an AWS WAF alarm's own metric (`AWS/WAFV2`) because a list
//! here left it out. A window is capped at 14 days and a query at five
//! metrics and ten dimensions, so one lookup can't pull an account's whole
//! history.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::Value;

use super::aws::{Session, TeamSession};
use super::http::window;
use super::StepError;

/// Common namespaces with what they cover: hints for the tool description,
/// never a limit (any namespace is read).
pub const NAMESPACE_HINTS: &[(&str, &str)] = &[
    ("AWS/ECS", "ECS services: CPUUtilization, MemoryUtilization (ClusterName, ServiceName)"),
    ("ECS/ContainerInsights", "ECS Container Insights: RunningTaskCount, CpuUtilized, MemoryUtilized"),
    ("AWS/RDS", "RDS and Aurora: DatabaseConnections, CPUUtilization, ReadLatency, WriteLatency, FreeableMemory (DBInstanceIdentifier or DBClusterIdentifier)"),
    ("AWS/Lambda", "Lambda: Duration, Errors, Throttles, Invocations, ConcurrentExecutions (FunctionName)"),
    ("AWS/CloudFront", "CloudFront: Requests, 4xxErrorRate, 5xxErrorRate, CacheHitRate (DistributionId, Region=Global; read in us-east-1)"),
    ("AWS/WAFV2", "AWS WAF: CountedRequests, BlockedRequests, AllowedRequests, CaptchaRequests, ChallengeRequests (WebACL, Rule, Region; a CloudFront web ACL has no Region and is read in us-east-1; Rule=ALL for the whole web ACL)"),
    ("AWS/ApplicationELB", "Load balancers: RequestCount, TargetResponseTime, HTTPCode_Target_5XX_Count, HealthyHostCount (LoadBalancer, TargetGroup)"),
    ("AWS/ELB", "Classic load balancers: RequestCount, Latency, HTTPCode_Backend_5XX"),
    ("AWS/NetworkELB", "Network load balancers: ActiveFlowCount, ProcessedBytes, HealthyHostCount, UnHealthyHostCount (LoadBalancer, TargetGroup)"),
    ("AWS/ApiGateway", "API Gateway: Count, Latency, 5XXError (ApiName)"),
    ("AWS/SQS", "SQS: ApproximateNumberOfMessagesVisible, ApproximateAgeOfOldestMessage (QueueName)"),
    ("AWS/SNS", "SNS: NumberOfMessagesPublished, NumberOfNotificationsFailed (TopicName)"),
    ("AWS/Events", "EventBridge: Invocations, FailedInvocations, TriggeredRules (RuleName)"),
    ("AWS/States", "Step Functions: ExecutionsFailed, ExecutionsTimedOut, ExecutionTime (StateMachineArn)"),
    ("AWS/Kinesis", "Kinesis streams: IncomingRecords, GetRecords.IteratorAgeMilliseconds (StreamName)"),
    ("AWS/Firehose", "Firehose: IncomingRecords, DeliveryToS3.Success, DeliveryToS3.DataFreshness (DeliveryStreamName)"),
    ("AWS/DynamoDB", "DynamoDB: ConsumedReadCapacityUnits, ThrottledRequests, SuccessfulRequestLatency (TableName)"),
    ("AWS/ElastiCache", "ElastiCache: CPUUtilization, CurrConnections, Evictions (CacheClusterId)"),
    ("AWS/EC2", "EC2 instances: CPUUtilization, NetworkIn, NetworkOut, StatusCheckFailed, CPUCreditBalance (InstanceId or AutoScalingGroupName)"),
    ("AWS/EBS", "EBS volumes: VolumeReadOps, VolumeWriteOps, VolumeQueueLength, BurstBalance (VolumeId)"),
    ("AWS/NATGateway", "NAT gateways: BytesOutToDestination, ErrorPortAllocation, PacketsDropCount (NatGatewayId)"),
    ("AWS/S3", "S3: BucketSizeBytes, NumberOfObjects (BucketName, StorageType; daily), 4xxErrors, 5xxErrors with request metrics (BucketName, FilterId)"),
    ("AWS/Logs", "CloudWatch Logs: IncomingLogEvents, IncomingBytes (LogGroupName)"),
    ("AWS/ES", "OpenSearch: CPUUtilization, JVMMemoryPressure, FreeStorageSpace, ClusterStatus.red, SearchLatency (DomainName, ClientId)"),
];
const STATS: &[&str] = &[
    "Average",
    "Sum",
    "Minimum",
    "Maximum",
    "SampleCount",
    "p50",
    "p90",
    "p95",
    "p99",
];
const MAX_QUERIES: usize = 5;
const MAX_DIMENSIONS: usize = 10;
const MAX_POINTS: i64 = 1_440;
const MAX_WINDOW: Duration = Duration::from_secs(14 * 86_400);
const DEFAULT_WINDOW: Duration = Duration::from_secs(3 * 3600);

fn refused(message: impl Into<String>) -> StepError {
    StepError::new(
        "read_only",
        message,
        "Name the namespace, metric and dimensions (metrics_list shows what exists), over 14 days at most.",
    )
}

/// Pure: a namespace as CloudWatch allows it (ASCII letters, digits,
/// `.`, `-`, `_`, `/`, `#`, `:` and spaces, at most 255), with a hint's
/// spelling when it is one of the common ones (`aws/ecs` is `AWS/ECS`).
/// Every namespace passes: the team's role decides what it may read.
pub fn check_namespace(ns: &str) -> Result<String, StepError> {
    let ns = ns.trim();
    if let Some((n, _)) = NAMESPACE_HINTS
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(ns))
    {
        return Ok((*n).to_string());
    }
    let ok = !ns.is_empty()
        && ns.len() <= 255
        && ns
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._-/#: ".contains(c));
    if ok {
        Ok(ns.to_string())
    } else {
        Err(refused(format!(
            "{ns:?} isn't a CloudWatch namespace (letters, digits, . - _ / # : and spaces)."
        )))
    }
}

/// Pure: a region an agent named (`us-east-1`).
fn check_region(r: &str) -> Result<String, StepError> {
    let r = r.trim();
    let ok = !r.is_empty()
        && r.len() <= 30
        && r.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if ok {
        Ok(r.to_string())
    } else {
        Err(refused(format!("{r:?} isn't an AWS region.")))
    }
}

/// Pure: the region a metric is published in when it isn't the team's:
/// CloudFront's, and a CloudFront web ACL's WAF metrics (no `Region`
/// dimension, or `Region=Global`), live in us-east-1; a regional web ACL's
/// WAF metrics live in the region its `Region` dimension names.
pub fn metric_region(namespace: &str, dimensions: &BTreeMap<String, String>) -> Option<String> {
    match namespace {
        "AWS/CloudFront" => Some("us-east-1".to_string()),
        "AWS/WAFV2" => match dimensions.get("Region") {
            None => Some("us-east-1".to_string()),
            Some(r) if r.eq_ignore_ascii_case("global") => Some("us-east-1".to_string()),
            Some(r) => check_region(r).ok(),
        },
        _ => None,
    }
}

fn check_name(what: &str, s: &str) -> Result<String, StepError> {
    let s = s.trim();
    let ok = !s.is_empty() && s.len() <= 255 && s.chars().all(|c| !c.is_control());
    if ok {
        Ok(s.to_string())
    } else {
        Err(refused(format!("That {what} isn't valid.")))
    }
}

fn dimensions(v: &Value) -> Result<BTreeMap<String, String>, StepError> {
    let mut out = BTreeMap::new();
    if let Some(map) = v.as_object() {
        if map.len() > MAX_DIMENSIONS {
            return Err(refused("Too many dimensions."));
        }
        for (k, val) in map {
            let val = val
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| val.to_string());
            out.insert(
                check_name("dimension name", k)?,
                check_name("dimension value", &val)?,
            );
        }
    }
    Ok(out)
}

/// One metric to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricQuery {
    pub id: String,
    pub namespace: String,
    pub metric: String,
    pub dimensions: BTreeMap<String, String>,
    pub stat: String,
}

/// A checked GetMetricData request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedQuery {
    pub queries: Vec<MetricQuery>,
    pub start: i64,
    pub end: i64,
    pub period: i32,
    /// Where the metrics are read when it isn't the team's region: the
    /// region asked for, else where they are published ([`metric_region`]).
    pub region: Option<String>,
}

impl CheckedQuery {
    pub fn describe(&self) -> String {
        self.queries
            .iter()
            .map(|q| {
                let dims: Vec<String> = q
                    .dimensions
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect();
                format!(
                    "{} {} {} [{}]",
                    q.namespace,
                    q.metric,
                    q.stat,
                    dims.join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
            + &format!(
                " every {}s from {} to {}",
                self.period, self.start, self.end
            )
    }
}

/// Checks a request: `metrics` is a list of `{namespace, metric, dimensions,
/// stat}`; `period` in seconds (a multiple of 60, default chosen to give at
/// most 120 points); `region` where to read when it isn't the team's region
/// or where the metric is published.
pub fn check_query(
    metrics: &[Value],
    period: Option<i64>,
    since: Option<&str>,
    until: Option<&str>,
    region: Option<&str>,
    now: i64,
) -> Result<CheckedQuery, StepError> {
    if metrics.is_empty() {
        return Err(refused("Say which metric to read."));
    }
    if metrics.len() > MAX_QUERIES {
        return Err(refused(format!(
            "At most {MAX_QUERIES} metrics per lookup."
        )));
    }
    let (start, end) = window(since, until, now, DEFAULT_WINDOW, MAX_WINDOW)?;
    let span = end - start;
    let period = match period {
        Some(p) if p < 60 || p % 60 != 0 => {
            return Err(refused(
                "The period is a whole number of minutes, in seconds (60, 300, 3600).",
            ))
        }
        Some(p) => p,
        None => (((span / 120) + 59) / 60 * 60).max(60),
    };
    if span / period > MAX_POINTS {
        return Err(refused(format!(
            "That asks for more than {MAX_POINTS} points. Use a longer period or a shorter window."
        )));
    }
    let asked = region
        .map(str::trim)
        .filter(|r| !r.is_empty())
        .map(check_region)
        .transpose()?;
    let mut queries = Vec::new();
    let mut regions: Vec<Option<String>> = Vec::new();
    for (i, m) in metrics.iter().enumerate() {
        let namespace = check_namespace(m["namespace"].as_str().unwrap_or(""))?;
        let dims = dimensions(&m["dimensions"])?;
        let at = metric_region(&namespace, &dims);
        if !regions.contains(&at) {
            regions.push(at);
        }
        let stat = m["stat"].as_str().unwrap_or("Average").trim().to_string();
        if !STATS.contains(&stat.as_str()) {
            return Err(refused(format!(
                "{stat} isn't a statistic CloudWatch knows ({}).",
                STATS.join(", ")
            )));
        }
        queries.push(MetricQuery {
            id: format!("m{i}"),
            namespace,
            metric: check_name("metric name", m["metric"].as_str().unwrap_or(""))?,
            dimensions: dims,
            stat,
        });
    }
    if asked.is_none() && regions.len() > 1 {
        return Err(refused(
            "Read metrics published in different regions in their own lookups (CloudFront's \
             and a CloudFront web ACL's WAF metrics live in us-east-1).",
        ));
    }
    Ok(CheckedQuery {
        queries,
        start,
        end,
        period: period as i32,
        region: asked.or_else(|| regions.into_iter().next().flatten()),
    })
}

/// A checked ListMetrics request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedList {
    pub namespace: String,
    pub metric: Option<String>,
    pub dimensions: BTreeMap<String, String>,
    /// Where to list when it isn't the team's region (see [`CheckedQuery`]).
    pub region: Option<String>,
}

pub fn check_list(
    namespace: &str,
    metric: Option<&str>,
    dims: &Value,
    region: Option<&str>,
) -> Result<CheckedList, StepError> {
    let namespace = check_namespace(namespace)?;
    let dimensions = dimensions(dims)?;
    let asked = region
        .map(str::trim)
        .filter(|r| !r.is_empty())
        .map(check_region)
        .transpose()?;
    Ok(CheckedList {
        region: asked.or_else(|| metric_region(&namespace, &dimensions)),
        namespace,
        metric: metric
            .map(str::trim)
            .filter(|m| !m.is_empty())
            .map(|m| check_name("metric name", m))
            .transpose()?,
        dimensions,
    })
}

/// One series that came back.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub label: String,
    /// (unix seconds, value), oldest first.
    pub points: Vec<(i64, f64)>,
}

/// One metric that exists, with its dimensions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricInfo {
    pub metric: String,
    pub dimensions: Vec<(String, String)>,
}

/// The two reads, behind a trait so tests can stand in for AWS.
#[async_trait::async_trait]
pub trait MetricsApi: Send + Sync {
    async fn list(&self, q: &CheckedList) -> Result<Vec<MetricInfo>, StepError>;
    async fn get(&self, q: &CheckedQuery) -> Result<Vec<Series>, StepError>;
}

/// Numbers as a short summary and a table an agent can read.
pub fn format_series(q: &CheckedQuery, series: &[Series]) -> String {
    let mut out = String::new();
    for (s, mq) in series.iter().zip(q.queries.iter()) {
        let dims: Vec<String> = mq
            .dimensions
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        out.push_str(&format!(
            "## {} {} ({}) {}\n",
            mq.namespace,
            mq.metric,
            mq.stat,
            dims.join(" ")
        ));
        if s.points.is_empty() {
            out.push_str(
                "No data points in this window (check the dimension values with metrics_list).\n\n",
            );
            continue;
        }
        let vals: Vec<f64> = s.points.iter().map(|p| p.1).collect();
        let min = vals.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let avg = vals.iter().sum::<f64>() / vals.len() as f64;
        out.push_str(&format!(
            "{} points every {}s: min {min:.3}, max {max:.3}, average {avg:.3}, latest {:.3}\n",
            vals.len(),
            q.period,
            vals[vals.len() - 1]
        ));
        let step = (s.points.len() / 60).max(1);
        for (t, v) in s.points.iter().step_by(step) {
            let at = chrono::DateTime::from_timestamp(*t, 0)
                .map(|d| d.format("%Y-%m-%dT%H:%MZ").to_string())
                .unwrap_or_default();
            out.push_str(&format!("{at} {v:.3}\n"));
        }
        out.push('\n');
    }
    out
}

pub fn format_list(list: &[MetricInfo]) -> String {
    if list.is_empty() {
        return "No metrics matched.".into();
    }
    let mut out = String::new();
    for m in list.iter().take(100) {
        let dims: Vec<String> = m
            .dimensions
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        out.push_str(&format!("- {} [{}]\n", m.metric, dims.join(", ")));
    }
    if list.len() > 100 {
        out.push_str(&format!(
            "… and {} more; narrow by metric or dimension.\n",
            list.len() - 100
        ));
    }
    out
}

/// The real reads, on a session of the team's role.
pub struct Aws<'a> {
    pub session: &'a TeamSession,
}

fn client(s: &Session, region: Option<&str>) -> aws_sdk_cloudwatch::Client {
    use aws_sdk_cloudwatch::config::{BehaviorVersion, Credentials, Region};
    let creds = Credentials::new(
        &s.access_key_id,
        &s.secret_access_key,
        Some(s.session_token.clone()),
        None,
        "coderhelm-team-role",
    );
    aws_sdk_cloudwatch::Client::from_conf(
        aws_sdk_cloudwatch::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.unwrap_or(&s.region).to_string()))
            .credentials_provider(creds)
            .build(),
    )
}

/// AWS's answer to a refused or failed call. A refusal of the team's role
/// (`role_arn`) is the install's setup item for `action`, named in one
/// line; a refusal of any other principal is CoderHelm's own failure, named
/// with the principal AWS refused.
fn aws_error<E, R>(
    e: &aws_sdk_cloudwatch::error::SdkError<E, R>,
    action: &str,
    role_arn: &str,
) -> StepError
where
    E: aws_sdk_cloudwatch::error::ProvideErrorMetadata + std::error::Error + 'static,
    R: std::fmt::Debug,
{
    use aws_sdk_cloudwatch::error::ProvideErrorMetadata as _;
    let code = e.code().unwrap_or("").to_string();
    let msg = e.message().unwrap_or("").to_string();
    if is_denied(&code) {
        if let Some(principal) = super::aws::refused_other_principal(&msg, role_arn) {
            tracing::error!(
                action,
                principal = %principal,
                "CoderHelm's own failure: a team read was made with a role that isn't the team's"
            );
            return StepError::new(
                "read",
                format!(
                    "AWS refused {action} to {principal}, which isn't the team's role, so this \
                     is CoderHelm's own failure, not the team's role."
                ),
                "",
            );
        }
        return StepError::role_refused(&[action]);
    }
    StepError::new(
        "read",
        format!(
            "CloudWatch refused the lookup ({}).",
            super::error_detail(&code, &msg)
        ),
        "Check the namespace, metric and dimension names with metrics_list.",
    )
}

/// Pure: an AWS error code that says the role may not make the call.
fn is_denied(code: &str) -> bool {
    code.contains("AccessDenied") || code.contains("UnauthorizedOperation")
}

#[async_trait::async_trait]
impl MetricsApi for Aws<'_> {
    async fn list(&self, q: &CheckedList) -> Result<Vec<MetricInfo>, StepError> {
        use aws_sdk_cloudwatch::types::DimensionFilter;
        let c = client(self.session, q.region.as_deref());
        let mut req = c.list_metrics().namespace(&q.namespace);
        if let Some(m) = &q.metric {
            req = req.metric_name(m);
        }
        for (k, v) in &q.dimensions {
            req = req.dimensions(DimensionFilter::builder().name(k).value(v).build());
        }
        let out = req
            .send()
            .await
            .map_err(|e| aws_error(&e, "cloudwatch:ListMetrics", self.session.role_arn()))?;
        Ok(out
            .metrics()
            .iter()
            .map(|m| MetricInfo {
                metric: m.metric_name().unwrap_or("").to_string(),
                dimensions: m
                    .dimensions()
                    .iter()
                    .map(|d| {
                        (
                            d.name().unwrap_or("").to_string(),
                            d.value().unwrap_or("").to_string(),
                        )
                    })
                    .collect(),
            })
            .collect())
    }

    async fn get(&self, q: &CheckedQuery) -> Result<Vec<Series>, StepError> {
        use aws_sdk_cloudwatch::primitives::DateTime;
        use aws_sdk_cloudwatch::types::{Dimension, Metric, MetricDataQuery, MetricStat};
        let c = client(self.session, q.region.as_deref());
        let mut req = c
            .get_metric_data()
            .start_time(DateTime::from_secs(q.start))
            .end_time(DateTime::from_secs(q.end))
            .max_datapoints(5_000);
        for mq in &q.queries {
            let mut metric = Metric::builder()
                .namespace(&mq.namespace)
                .metric_name(&mq.metric);
            for (k, v) in &mq.dimensions {
                metric = metric.dimensions(Dimension::builder().name(k).value(v).build());
            }
            req = req.metric_data_queries(
                MetricDataQuery::builder()
                    .id(&mq.id)
                    .metric_stat(
                        MetricStat::builder()
                            .metric(metric.build())
                            .period(q.period)
                            .stat(&mq.stat)
                            .build(),
                    )
                    .return_data(true)
                    .build(),
            );
        }
        let out = req
            .send()
            .await
            .map_err(|e| aws_error(&e, "cloudwatch:GetMetricData", self.session.role_arn()))?;
        let mut series = Vec::new();
        for mq in &q.queries {
            let r = out
                .metric_data_results()
                .iter()
                .find(|r| r.id() == Some(mq.id.as_str()));
            let mut points: Vec<(i64, f64)> = r
                .map(|r| {
                    r.timestamps()
                        .iter()
                        .zip(r.values().iter())
                        .map(|(t, v)| (t.secs(), *v))
                        .collect()
                })
                .unwrap_or_default();
            points.sort_by_key(|p| p.0);
            series.push(Series {
                label: r.and_then(|r| r.label()).unwrap_or("").to_string(),
                points,
            });
        }
        Ok(series)
    }
}

/// Whether the role can read metrics: one ListMetrics. `Ok(false)` when AWS
/// says the role isn't allowed.
pub async fn probe(session: &TeamSession) -> Result<bool, StepError> {
    let c = client(session, None);
    match c.list_metrics().namespace("AWS/Lambda").send().await {
        Ok(_) => Ok(true),
        Err(e) => {
            let err = aws_error(&e, "cloudwatch:ListMetrics", session.role_arn());
            if !err.role_refused.is_empty() {
                Ok(false)
            } else {
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    const NOW: i64 = 1_800_000_000;

    #[test]
    fn any_namespace_passes_and_common_ones_keep_their_spelling() {
        let ok = check_query(
            &[json!({"namespace":"aws/ecs","metric":"CPUUtilization","dimensions":{"ClusterName":"prod","ServiceName":"api"},"stat":"p95"})],
            None,
            Some("2h"),
            None,
            None,
            NOW,
        )
        .unwrap();
        assert_eq!(ok.queries[0].namespace, "AWS/ECS");
        assert_eq!(ok.period, 60);
        assert_eq!(ok.region, None);
        // An instance's CPU, as a reviewer checks before a resize.
        let ec2 = check_query(
            &[json!({"namespace":"AWS/EC2","metric":"CPUUtilization","dimensions":{"InstanceId":"i-0123456789abcdef0"},"stat":"Maximum"})],
            Some(86_400),
            Some("7d"),
            None,
            None,
            NOW,
        )
        .unwrap();
        assert_eq!(ec2.queries[0].namespace, "AWS/EC2");
        // The role decides what it may read, not a list here: namespaces
        // outside the hints, and custom ones, are read as asked.
        for ns in [
            "AWS/WAFV2",
            "AWS/Billing",
            "AWS/Usage",
            "Custom/Mine",
            "Glue",
            "CWAgent",
        ] {
            let q = check_query(
                &[json!({"namespace": ns, "metric":"x"})],
                None,
                None,
                None,
                None,
                NOW,
            );
            assert!(q.is_ok(), "{ns}: {q:?}");
            assert!(check_list(ns, None, &json!({}), None).is_ok(), "{ns}");
        }
        // What CloudWatch itself can't hold is refused before it is sent.
        for bad in ["", "AWS/WAFV2\nRule", &"x".repeat(256), "a\u{0}b"] {
            assert!(check_namespace(bad).is_err(), "{bad:?}");
        }
        assert!(check_query(
            &[json!({"namespace":"AWS/ECS","metric":"x","stat":"Median"})],
            None,
            None,
            None,
            None,
            NOW
        )
        .is_err());
        assert!(check_query(&[], None, None, None, None, NOW).is_err());
    }

    #[test]
    fn the_waf_namespace_is_a_hint_with_its_dimensions() {
        let (ns, what) = NAMESPACE_HINTS
            .iter()
            .find(|(n, _)| *n == "AWS/WAFV2")
            .expect("AWS/WAFV2 is a hint");
        assert_eq!(*ns, "AWS/WAFV2");
        for word in [
            "CountedRequests",
            "BlockedRequests",
            "AllowedRequests",
            "WebACL",
            "Rule",
            "Region",
        ] {
            assert!(what.contains(word), "{word}");
        }
    }

    #[test]
    fn waf_metrics_read_where_the_web_acl_publishes_them() {
        // A CloudFront web ACL: no Region dimension, published in us-east-1.
        let cf = json!({"namespace":"AWS/WAFV2","metric":"CountedRequests","dimensions":{"WebACL":"example-web-acl","Rule":"ExampleCrawlerCount"},"stat":"Sum"});
        let blocked = json!({"namespace":"AWS/WAFV2","metric":"BlockedRequests","dimensions":{"WebACL":"example-web-acl","Rule":"ExampleCrawlerBlock"},"stat":"Sum"});
        let q = check_query(
            &[cf.clone(), blocked],
            Some(3600),
            Some("1d"),
            None,
            None,
            NOW,
        )
        .unwrap();
        assert_eq!(q.region.as_deref(), Some("us-east-1"));
        assert!(q.describe().contains(
            "AWS/WAFV2 CountedRequests Sum [Rule=ExampleCrawlerCount, WebACL=example-web-acl]"
        ));
        // A regional web ACL: its Region dimension.
        let regional = json!({"namespace":"AWS/WAFV2","metric":"BlockedRequests","dimensions":{"WebACL":"api-acl","Rule":"ALL","Region":"eu-west-1"},"stat":"Sum"});
        let q = check_query(std::slice::from_ref(&regional), None, None, None, None, NOW).unwrap();
        assert_eq!(q.region.as_deref(), Some("eu-west-1"));
        // Mixed regions go in their own lookups, unless a region is named.
        assert!(check_query(&[cf.clone(), regional.clone()], None, None, None, None, NOW).is_err());
        let named = check_query(
            &[cf.clone(), regional],
            None,
            None,
            None,
            Some("eu-west-1"),
            NOW,
        )
        .unwrap();
        assert_eq!(named.region.as_deref(), Some("eu-west-1"));
        assert!(check_query(&[cf], None, None, None, Some("not a region"), NOW).is_err());
        let l = check_list(
            "AWS/WAFV2",
            Some("CountedRequests"),
            &json!({"WebACL":"example-web-acl"}),
            None,
        )
        .unwrap();
        assert_eq!(l.region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn windows_points_and_counts_are_capped() {
        let m = json!({"namespace":"AWS/Lambda","metric":"Duration"});
        assert!(check_query(std::slice::from_ref(&m), None, Some("15d"), None, None, NOW).is_err());
        assert!(check_query(std::slice::from_ref(&m), Some(30), None, None, None, NOW).is_err());
        assert!(
            check_query(
                std::slice::from_ref(&m),
                Some(60),
                Some("7d"),
                None,
                None,
                NOW
            )
            .is_err(),
            "10080 points"
        );
        assert!(check_query(
            std::slice::from_ref(&m),
            Some(3600),
            Some("7d"),
            None,
            None,
            NOW
        )
        .is_ok());
        let six: Vec<Value> = (0..6).map(|_| m.clone()).collect();
        assert!(check_query(&six, None, None, None, None, NOW).is_err());
        let dims: serde_json::Map<String, Value> =
            (0..11).map(|i| (format!("d{i}"), json!("v"))).collect();
        assert!(check_query(
            &[json!({"namespace":"AWS/Lambda","metric":"Duration","dimensions":dims})],
            None,
            None,
            None,
            None,
            NOW
        )
        .is_err());
        let auto = check_query(&[m], None, Some("7d"), None, None, NOW).unwrap();
        assert!((7 * 86_400) / i64::from(auto.period) <= 120);
    }

    #[test]
    fn cloudfront_reads_in_us_east_1_on_its_own() {
        let cf = json!({"namespace":"AWS/CloudFront","metric":"CacheHitRate","dimensions":{"DistributionId":"E1","Region":"Global"}});
        assert_eq!(
            check_query(std::slice::from_ref(&cf), None, None, None, None, NOW)
                .unwrap()
                .region
                .as_deref(),
            Some("us-east-1")
        );
        let other = json!({"namespace":"AWS/ECS","metric":"CPUUtilization"});
        assert!(check_query(&[cf, other], None, None, None, None, NOW).is_err());
    }

    #[test]
    fn a_refused_read_is_the_roles_setup_item_not_a_refusal_of_ours() {
        assert!(is_denied("AccessDenied") && is_denied("AccessDeniedException"));
        assert!(!is_denied("InvalidParameterValue"));
        let e = StepError::role_refused(&["cloudwatch:GetMetricData"]);
        assert!(!e.is_refusal());
        assert_eq!(e.role_refused, vec!["cloudwatch:GetMetricData"]);
        assert!(e.message.contains("\"AWS role reads\""), "{}", e.message);
        assert!(!e.message.contains("isn't a namespace"));
    }

    struct Fake {
        calls: Mutex<usize>,
    }

    #[async_trait::async_trait]
    impl MetricsApi for Fake {
        async fn list(&self, _q: &CheckedList) -> Result<Vec<MetricInfo>, StepError> {
            *self.calls.lock().unwrap() += 1;
            Ok(vec![MetricInfo {
                metric: "CPUUtilization".into(),
                dimensions: vec![("ServiceName".into(), "api".into())],
            }])
        }
        async fn get(&self, q: &CheckedQuery) -> Result<Vec<Series>, StepError> {
            *self.calls.lock().unwrap() += 1;
            Ok(q.queries
                .iter()
                .map(|_| Series {
                    label: "x".into(),
                    points: vec![(NOW - 120, 10.0), (NOW - 60, 30.0)],
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn numbers_read_as_a_summary() {
        let api = Fake {
            calls: Mutex::new(0),
        };
        let q = check_query(
            &[json!({"namespace":"AWS/ECS","metric":"CPUUtilization"})],
            None,
            None,
            None,
            None,
            NOW,
        )
        .unwrap();
        let s = api.get(&q).await.unwrap();
        let text = format_series(&q, &s);
        assert!(
            text.contains("min 10.000, max 30.000, average 20.000, latest 30.000"),
            "{text}"
        );
        let l = check_list(
            "AWS/ECS",
            Some("CPUUtilization"),
            &json!({"ClusterName":"prod"}),
            None,
        )
        .unwrap();
        assert!(format_list(&api.list(&l).await.unwrap()).contains("ServiceName=api"));
        assert_eq!(*api.calls.lock().unwrap(), 2);
    }
}
