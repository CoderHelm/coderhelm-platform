//! CloudWatch metrics in a team's AWS account, through its log reader role.
//!
//! Two calls only, both reads: ListMetrics (what exists, with its dimension
//! values) and GetMetricData (numbers over time). Only the AWS service
//! namespaces below can be read, a window is capped at 14 days and a query at
//! five metrics, so one lookup can't pull an account's whole history.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::Value;

use super::aws::Session;
use super::http::window;
use super::StepError;

/// Namespaces agents may read, with what they cover.
pub const NAMESPACES: &[(&str, &str)] = &[
    ("AWS/ECS", "ECS services: CPUUtilization, MemoryUtilization (ClusterName, ServiceName)"),
    ("ECS/ContainerInsights", "ECS Container Insights: RunningTaskCount, CpuUtilized, MemoryUtilized"),
    ("AWS/RDS", "RDS and Aurora: DatabaseConnections, CPUUtilization, ReadLatency, WriteLatency, FreeableMemory (DBInstanceIdentifier or DBClusterIdentifier)"),
    ("AWS/Lambda", "Lambda: Duration, Errors, Throttles, Invocations, ConcurrentExecutions (FunctionName)"),
    ("AWS/CloudFront", "CloudFront: Requests, 4xxErrorRate, 5xxErrorRate, CacheHitRate (DistributionId, Region=Global; read in us-east-1)"),
    ("AWS/ApplicationELB", "Load balancers: RequestCount, TargetResponseTime, HTTPCode_Target_5XX_Count, HealthyHostCount (LoadBalancer, TargetGroup)"),
    ("AWS/ELB", "Classic load balancers: RequestCount, Latency, HTTPCode_Backend_5XX"),
    ("AWS/SQS", "SQS: ApproximateNumberOfMessagesVisible, ApproximateAgeOfOldestMessage (QueueName)"),
    ("AWS/DynamoDB", "DynamoDB: ConsumedReadCapacityUnits, ThrottledRequests, SuccessfulRequestLatency (TableName)"),
    ("AWS/ElastiCache", "ElastiCache: CPUUtilization, CurrConnections, Evictions (CacheClusterId)"),
    ("AWS/ApiGateway", "API Gateway: Count, Latency, 5XXError (ApiName)"),
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
        "Read one of the AWS namespaces listed in the tool description, over 14 days at most.",
    )
}

fn check_namespace(ns: &str) -> Result<&'static str, StepError> {
    NAMESPACES
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(ns.trim()))
        .map(|(n, _)| *n)
        .ok_or_else(|| refused(format!("{ns} isn't a namespace Coderhelm reads.")))
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
    pub namespace: &'static str,
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
    /// CloudFront's metrics live in us-east-1 whatever the team's region.
    pub region: Option<&'static str>,
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
/// most 120 points).
pub fn check_query(
    metrics: &[Value],
    period: Option<i64>,
    since: Option<&str>,
    until: Option<&str>,
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
    let mut queries = Vec::new();
    let mut cloudfront = false;
    let mut other = false;
    for (i, m) in metrics.iter().enumerate() {
        let namespace = check_namespace(m["namespace"].as_str().unwrap_or(""))?;
        if namespace == "AWS/CloudFront" {
            cloudfront = true;
        } else {
            other = true;
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
            dimensions: dimensions(&m["dimensions"])?,
            stat,
        });
    }
    if cloudfront && other {
        return Err(refused(
            "Read CloudFront metrics in their own lookup (they live in us-east-1).",
        ));
    }
    Ok(CheckedQuery {
        queries,
        start,
        end,
        period: period as i32,
        region: cloudfront.then_some("us-east-1"),
    })
}

/// A checked ListMetrics request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedList {
    pub namespace: &'static str,
    pub metric: Option<String>,
    pub dimensions: BTreeMap<String, String>,
}

pub fn check_list(
    namespace: &str,
    metric: Option<&str>,
    dims: &Value,
) -> Result<CheckedList, StepError> {
    Ok(CheckedList {
        namespace: check_namespace(namespace)?,
        metric: metric
            .map(str::trim)
            .filter(|m| !m.is_empty())
            .map(|m| check_name("metric name", m))
            .transpose()?,
        dimensions: dimensions(dims)?,
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
    pub session: &'a Session,
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

fn aws_error<E, R>(e: &aws_sdk_cloudwatch::error::SdkError<E, R>) -> StepError
where
    E: aws_sdk_cloudwatch::error::ProvideErrorMetadata + std::error::Error + 'static,
    R: std::fmt::Debug,
{
    use aws_sdk_cloudwatch::error::ProvideErrorMetadata as _;
    let code = e.code().unwrap_or("").to_string();
    let msg = e.message().unwrap_or("").to_string();
    if code.contains("AccessDenied") || code.contains("UnauthorizedOperation") {
        return StepError::new(
            "sign_in",
            "The CoderHelm role in your AWS account can't read metrics yet.",
            "Update the role from Settings → AWS (one click adds read access to metrics).",
        );
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

#[async_trait::async_trait]
impl MetricsApi for Aws<'_> {
    async fn list(&self, q: &CheckedList) -> Result<Vec<MetricInfo>, StepError> {
        use aws_sdk_cloudwatch::types::DimensionFilter;
        let region = (q.namespace == "AWS/CloudFront").then_some("us-east-1");
        let c = client(self.session, region);
        let mut req = c.list_metrics().namespace(q.namespace);
        if let Some(m) = &q.metric {
            req = req.metric_name(m);
        }
        for (k, v) in &q.dimensions {
            req = req.dimensions(DimensionFilter::builder().name(k).value(v).build());
        }
        let out = req.send().await.map_err(|e| aws_error(&e))?;
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
        let c = client(self.session, q.region);
        let mut req = c
            .get_metric_data()
            .start_time(DateTime::from_secs(q.start))
            .end_time(DateTime::from_secs(q.end))
            .max_datapoints(5_000);
        for mq in &q.queries {
            let mut metric = Metric::builder()
                .namespace(mq.namespace)
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
        let out = req.send().await.map_err(|e| aws_error(&e))?;
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
pub async fn probe(session: &Session) -> Result<bool, StepError> {
    let c = client(session, None);
    match c.list_metrics().namespace("AWS/Lambda").send().await {
        Ok(_) => Ok(true),
        Err(e) => {
            let err = aws_error(&e);
            if err.step == "sign_in" {
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
    fn only_listed_namespaces_and_stats_pass() {
        let ok = check_query(
            &[json!({"namespace":"aws/ecs","metric":"CPUUtilization","dimensions":{"ClusterName":"prod","ServiceName":"api"},"stat":"p95"})],
            None,
            Some("2h"),
            None,
            NOW,
        )
        .unwrap();
        assert_eq!(ok.queries[0].namespace, "AWS/ECS");
        assert_eq!(ok.period, 60);
        assert_eq!(ok.region, None);
        assert!(check_query(
            &[json!({"namespace":"AWS/Billing","metric":"EstimatedCharges"})],
            None,
            None,
            None,
            NOW
        )
        .is_err());
        assert!(check_query(
            &[json!({"namespace":"Custom/Mine","metric":"x"})],
            None,
            None,
            None,
            NOW
        )
        .is_err());
        assert!(check_query(
            &[json!({"namespace":"AWS/ECS","metric":"x","stat":"Median"})],
            None,
            None,
            None,
            NOW
        )
        .is_err());
        assert!(check_query(&[], None, None, None, NOW).is_err());
    }

    #[test]
    fn windows_points_and_counts_are_capped() {
        let m = json!({"namespace":"AWS/Lambda","metric":"Duration"});
        assert!(check_query(std::slice::from_ref(&m), None, Some("15d"), None, NOW).is_err());
        assert!(check_query(std::slice::from_ref(&m), Some(30), None, None, NOW).is_err());
        assert!(
            check_query(std::slice::from_ref(&m), Some(60), Some("7d"), None, NOW).is_err(),
            "10080 points"
        );
        assert!(check_query(std::slice::from_ref(&m), Some(3600), Some("7d"), None, NOW).is_ok());
        let six: Vec<Value> = (0..6).map(|_| m.clone()).collect();
        assert!(check_query(&six, None, None, None, NOW).is_err());
        let auto = check_query(&[m], None, Some("7d"), None, NOW).unwrap();
        assert!((7 * 86_400) / i64::from(auto.period) <= 120);
    }

    #[test]
    fn cloudfront_reads_in_us_east_1_on_its_own() {
        let cf = json!({"namespace":"AWS/CloudFront","metric":"CacheHitRate","dimensions":{"DistributionId":"E1","Region":"Global"}});
        assert_eq!(
            check_query(std::slice::from_ref(&cf), None, None, None, NOW)
                .unwrap()
                .region,
            Some("us-east-1")
        );
        let other = json!({"namespace":"AWS/ECS","metric":"CPUUtilization"});
        assert!(check_query(&[cf, other], None, None, None, NOW).is_err());
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
        )
        .unwrap();
        assert!(format_list(&api.list(&l).await.unwrap()).contains("ServiceName=api"));
        assert_eq!(*api.calls.lock().unwrap(), 2);
        assert!(check_list("AWS/Billing", None, &json!({})).is_err());
    }
}
