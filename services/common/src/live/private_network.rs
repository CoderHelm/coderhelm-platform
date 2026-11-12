//! Reading a source that is reachable only inside a VPC
//! (docs/self-host/private-networks.md).
//!
//! A self-hosted install may have up to three private networks (install
//! stack parameters `PrivateNetwork1..3`, infra/lib/native/parent.ts). Each
//! is a small read-only lookup function, `coderhelm-<name>-reach-<n>`, placed
//! in subnets an admin chose, with its own security group (out on 443 and
//! 9200 only). A source that names a network ([`super::store::Source::network`])
//! is read through it: [`Reach`] is the same [`Transport`] the internet path
//! is, so every read-only rule (index allowlist, no scripts, limits,
//! redaction) runs here before a request is built, exactly as before. The
//! function only carries the request, and refuses anything that isn't HTTPS
//! to a name with private addresses (lambda/private-reach).

use serde::Serialize;
use serde_json::{json, Value};

use super::api::Check;
use super::http::{Reply, Request, Transport};
use super::source_errors::{aws_opensearch_host, Failure};
use super::StepError;

/// The networks an install may have.
pub const NETWORKS: [&str; 3] = ["1", "2", "3"];
/// Where the gateway and worker learn the functions' name prefix (`coderhelm-<name>-reach-`).
pub const PREFIX_ENV: &str = "REACH_FUNCTION_PREFIX";

/// "" (the internet) or one of [`NETWORKS`].
pub fn valid(network: &str) -> bool {
    network.is_empty() || NETWORKS.contains(&network)
}

/// The install stack parameter of network `n`.
pub fn parameter(network: &str) -> String {
    format!("PrivateNetwork{network}")
}

/// The function of network `n` on this install, or None when this install has none.
pub fn function_name(network: &str) -> Option<String> {
    let prefix = std::env::var(PREFIX_ENV).ok().filter(|p| !p.is_empty())?;
    NETWORKS
        .contains(&network)
        .then(|| format!("{prefix}{network}"))
}

fn not_here(network: &str) -> StepError {
    StepError::new(
        "reach",
        format!("This source reads through private network {network}, which this install doesn't have."),
        "Add it again on the source's card (Data sources), or switch the source back to the internet.",
    )
}

/// The transport through network `n`'s function.
pub struct Reach {
    pub network: String,
    pub function: String,
    pub lambda: aws_sdk_lambda::Client,
}

impl Reach {
    pub async fn new(network: &str) -> Result<Reach, StepError> {
        let function = function_name(network).ok_or_else(|| not_here(network))?;
        let conf = aws_config::load_from_env().await;
        Ok(Reach {
            network: network.to_string(),
            function,
            lambda: aws_sdk_lambda::Client::new(&conf),
        })
    }

    async fn invoke(&self, event: Value) -> Result<Value, StepError> {
        let out = self
            .lambda
            .invoke()
            .function_name(&self.function)
            .payload(aws_sdk_lambda::primitives::Blob::new(event.to_string()))
            .send()
            .await
            .map_err(|e| {
                let code = e.as_service_error().map(|s| s.to_string()).unwrap_or_default();
                if code.contains("ResourceNotFound") {
                    not_here(&self.network)
                } else {
                    StepError::new(
                        "reach",
                        format!(
                            "Private network {} didn't run the lookup ({}).",
                            self.network,
                            super::error_detail("", &aws_sdk_lambda::error::DisplayErrorContext(&e).to_string())
                        ),
                        "Try again in a minute. If it keeps failing, check the install's update history (Settings, System): the network's function may still be deploying.",
                    )
                }
            })?;
        if out.function_error().is_some() {
            return Err(StepError::new(
                "reach",
                format!("Private network {}'s function failed.", self.network),
                "Try again in a minute; its log is /aws/lambda/<install>-reach-<n> in CloudWatch.",
            ));
        }
        let bytes = out
            .payload()
            .map(|b| b.as_ref().to_vec())
            .unwrap_or_default();
        serde_json::from_slice(&bytes).map_err(|_| {
            StepError::new(
                "reach",
                format!(
                    "Private network {}'s function answered with something unreadable.",
                    self.network
                ),
                "Try again in a minute.",
            )
        })
    }

    /// Each hop to `url` from inside the network: the address, name lookup, TCP, TLS.
    pub async fn hops(&self, url: &str) -> Vec<Check> {
        match self.invoke(json!({ "action": "check", "url": url })).await {
            Ok(v) => hop_checks(&self.network, &v),
            Err(e) => vec![Check {
                id: "network",
                label: L_NETWORK,
                ok: false,
                detail: e.message,
                fix: Some(e.fix),
            }],
        }
    }
}

#[async_trait::async_trait]
impl Transport for Reach {
    async fn send(&self, req: Request) -> Result<Reply, StepError> {
        let v = self
            .invoke(json!({
                "action": "send",
                "url": req.url,
                "method": req.method.as_str(),
                "headers": req.headers.iter().map(|(k, v)| (k.clone(), Value::String(v.clone()))).collect::<serde_json::Map<String, Value>>(),
                "body": req.body,
            }))
            .await?;
        answer(&self.network, &host_of(&req.url), &v)
    }
}

fn host_of(url: &str) -> String {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_default()
}

/// The function's answer: the reply, or the hop that failed, marked so
/// each kind of source words it ([`super::source_errors`]).
pub fn answer(network: &str, host: &str, v: &Value) -> Result<Reply, StepError> {
    if let Some(e) = v.get("error") {
        let hop = e["hop"].as_str().unwrap_or("");
        let detail = super::error_detail("", e["detail"].as_str().unwrap_or(""));
        let (label, fix) = hop_words(hop);
        let msg = format!("Inside private network {network}, {label}: {detail}");
        return Err(match hop {
            "dns" => StepError::marked(Failure::NotFound, host, msg, fix),
            "tcp" => StepError::marked(Failure::Timeout, host, msg, fix),
            "tls" => StepError::marked(Failure::Tls, host, msg, fix),
            _ => StepError::new("reach", msg, fix),
        });
    }
    match (v["status"].as_u64(), v["body"].as_str()) {
        (Some(status), Some(body)) => Ok(Reply {
            status: status as u16,
            body: body.to_string(),
        }),
        _ => Err(StepError::new(
            "reach",
            format!("Private network {network}'s function answered with something unreadable."),
            "Try again in a minute.",
        )),
    }
}

const L_NETWORK: &str = "Run the private network's function";
const L_ADDRESS: &str = "An https address";
const L_DNS: &str = "Find the name inside the network";
const L_TCP: &str = "Connect on the port (security groups)";
const L_TLS: &str = "Secure connection (TLS)";

/// (what the hop is, what to do when it fails)
fn hop_words(hop: &str) -> (&'static str, String) {
    match hop {
        "address" => ("the address", "Use the service's https address with its host name (no user, password or IP address), on port 443 or 9200.".to_string()),
        "dns" => ("finding the name", "Check the address is the service's current endpoint (an AWS domain deleted or replaced has a new one), and that the VPC resolves names (DNS resolution on).".to_string()),
        "tcp" => ("connecting", "The service's security group must allow tcp 443 (or 9200) from the private network's security group: the source's card shows the rule. Also check the subnets' network ACLs.".to_string()),
        "tls" => ("the secure connection", "Use the service's own endpoint name, which its certificate is for.".to_string()),
        _ => ("reading", "Try again in a minute.".to_string()),
    }
}

/// The function's hop report as checks, in order, each named.
pub fn hop_checks(network: &str, v: &Value) -> Vec<Check> {
    if let Some(e) = v.get("error") {
        let (_, fix) = hop_words(e["hop"].as_str().unwrap_or(""));
        return vec![Check {
            id: "network",
            label: L_NETWORK,
            ok: false,
            detail: format!(
                "Private network {network}: {}",
                super::error_detail("", e["detail"].as_str().unwrap_or(""))
            ),
            fix: Some(fix),
        }];
    }
    v["hops"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|h| {
            let hop = h["hop"].as_str().unwrap_or("");
            let (id, label) = match hop {
                "address" => ("address", L_ADDRESS),
                "dns" => ("dns", L_DNS),
                "tcp" => ("tcp", L_TCP),
                "tls" => ("tls", L_TLS),
                _ => ("network", L_NETWORK),
            };
            let ok = h["ok"].as_bool().unwrap_or(false);
            Check {
                id,
                label,
                ok,
                detail: super::error_detail("", h["detail"].as_str().unwrap_or("")),
                fix: (!ok).then(|| hop_words(hop).1),
            }
        })
        .collect()
}

/// One network as the install has it now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Placement {
    pub network: String,
    pub vpc_id: String,
    pub subnet_ids: Vec<String>,
    pub security_group_ids: Vec<String>,
    /// Lambda's state: Active, Pending, Failed.
    pub state: String,
}

/// The install's networks that exist (their functions), read from Lambda.
pub async fn placements(lambda: &aws_sdk_lambda::Client) -> Vec<Placement> {
    let mut out = Vec::new();
    for n in NETWORKS {
        let Some(name) = function_name(n) else {
            return out;
        };
        let Ok(c) = lambda
            .get_function_configuration()
            .function_name(&name)
            .send()
            .await
        else {
            continue;
        };
        let vpc = c.vpc_config();
        out.push(Placement {
            network: n.to_string(),
            vpc_id: vpc.and_then(|v| v.vpc_id()).unwrap_or("").to_string(),
            subnet_ids: vpc.map(|v| v.subnet_ids().to_vec()).unwrap_or_default(),
            security_group_ids: vpc
                .map(|v| v.security_group_ids().to_vec())
                .unwrap_or_default(),
            state: c
                .state()
                .map(|s| s.as_str().to_string())
                .unwrap_or_default(),
        });
    }
    out
}

/// An AWS OpenSearch domain, as DescribeDomain has it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DomainPlace {
    pub domain: String,
    pub region: String,
    /// Its current endpoint, as an https address.
    pub endpoint: String,
    /// Reachable only inside its VPC.
    pub vpc_only: bool,
    pub vpc_id: String,
    pub subnet_ids: Vec<String>,
    pub security_group_ids: Vec<String>,
}

/// The domain behind an AWS OpenSearch address, read with the install's own
/// role (es:DescribeDomain). Ok(None): not an AWS OpenSearch address.
pub async fn describe_domain(host: &str) -> Result<Option<DomainPlace>, String> {
    let Some((_, domain, region)) = aws_opensearch_host(host) else {
        return Ok(None);
    };
    describe_named(domain, region).await
}

/// DescribeDomain of `domain` in `region`.
pub async fn describe_named(domain: &str, region: &str) -> Result<Option<DomainPlace>, String> {
    let conf = aws_config::from_env()
        .region(aws_config::Region::new(region.to_string()))
        .load()
        .await;
    let es = aws_sdk_opensearch::Client::new(&conf);
    let d = match es.describe_domain().domain_name(domain).send().await {
        Ok(d) => d,
        Err(e) => {
            let text = aws_sdk_opensearch::error::DisplayErrorContext(&e).to_string();
            if text.contains("ResourceNotFound") {
                return Err(format!("No OpenSearch domain named {domain} in {region} in this AWS account: it was deleted, or it is in another account."));
            }
            return Err(format!(
                "Coderhelm couldn't read the domain {domain}: {}",
                super::error_detail("", &text)
            ));
        }
    };
    let Some(s) = d.domain_status() else {
        return Ok(None);
    };
    let vpc = s.vpc_options();
    let endpoint = s
        .endpoints()
        .and_then(|m| m.get("vpc").cloned())
        .or_else(|| s.endpoint().map(str::to_string))
        .unwrap_or_default();
    Ok(Some(DomainPlace {
        domain: domain.to_string(),
        region: region.to_string(),
        endpoint: if endpoint.is_empty() {
            String::new()
        } else {
            format!("https://{endpoint}")
        },
        vpc_only: vpc.and_then(|v| v.vpc_id()).is_some(),
        vpc_id: vpc.and_then(|v| v.vpc_id()).unwrap_or("").to_string(),
        subnet_ids: vpc.map(|v| v.subnet_ids().to_vec()).unwrap_or_default(),
        security_group_ids: vpc
            .map(|v| v.security_group_ids().to_vec())
            .unwrap_or_default(),
    }))
}

/// The domain behind an address, or, when no domain of that name exists any
/// more, the one with a similar name that replaced it (`replaced` says so).
pub async fn find_domain(host: &str) -> Result<Option<(DomainPlace, bool)>, String> {
    match describe_domain(host).await {
        Ok(Some(d)) => Ok(Some((d, false))),
        Ok(None) => Ok(None),
        Err(e) => {
            if let Some((_, domain, region)) = aws_opensearch_host(host) {
                for n in similar_domain_names(domain, region).await.iter().take(3) {
                    if let Ok(Some(d)) = describe_named(n, region).await {
                        return Ok(Some((d, true)));
                    }
                }
            }
            Err(e)
        }
    }
}

/// The account's domain names in `region` like `domain` ([`similar_names`]).
async fn similar_domain_names(domain: &str, region: &str) -> Vec<String> {
    let conf = aws_config::from_env()
        .region(aws_config::Region::new(region.to_string()))
        .load()
        .await;
    let es = aws_sdk_opensearch::Client::new(&conf);
    match es.list_domain_names().send().await {
        Ok(r) => r
            .domain_names()
            .iter()
            .filter_map(|d| d.domain_name().map(str::to_string))
            .filter(|n| similar_names(n, domain))
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// The install's transport for a source: the internet, or its network's function.
pub async fn transport(network: &str) -> Result<Box<dyn Transport>, StepError> {
    if network.is_empty() {
        Ok(Box::new(super::http::Https))
    } else {
        Ok(Box::new(Reach::new(network).await?))
    }
}

/// Whether `a` and `b` name the same domain but for a trailing number or
/// suffix (orders-os and orders-os2, orders and orders-new).
pub fn similar_names(a: &str, b: &str) -> bool {
    let core = |s: &str| {
        s.trim_end_matches(|c: char| c.is_ascii_digit() || c == '-')
            .to_string()
    };
    let (x, y) = (core(a), core(b));
    a != b
        && x.len() >= 4
        && y.len() >= 4
        && (x == y || x.starts_with(&format!("{y}-")) || y.starts_with(&format!("{x}-")))
}

/// What the install's own AWS account says about an OpenSearch address that
/// failed: the domain's current endpoint when it moved, or a domain with a
/// similar name that replaced a deleted one, and whether it is VPC-only.
/// None when it isn't an AWS address, or nothing useful was found (another
/// account's domain, or no permission to look).
pub async fn domain_hint(host: &str) -> Option<String> {
    let (_, domain, region) = aws_opensearch_host(host)?;
    let place = |d: &DomainPlace| {
        if d.vpc_only {
            format!(
                "{}, reachable only inside {} (add a private network for it on this source's card)",
                d.endpoint, d.vpc_id
            )
        } else {
            d.endpoint.clone()
        }
    };
    match describe_domain(host).await {
        Ok(Some(d)) if !d.endpoint.ends_with(host) && !d.endpoint.is_empty() => {
            return Some(format!(
                "This AWS account's domain {} is now at {}.",
                d.domain,
                place(&d)
            ));
        }
        Ok(Some(d)) if d.vpc_only => {
            return Some(format!(
                "The domain {} is inside {}: add a private network for it on this source's card.",
                d.domain, d.vpc_id
            ));
        }
        Ok(_) => return None,
        Err(_) => {}
    }
    let names = similar_domain_names(domain, region).await;
    let mut found = Vec::new();
    for n in names.iter().take(3) {
        if let Ok(Some(d)) = describe_named(n, region).await {
            found.push(format!("{} ({})", d.domain, place(&d)));
        }
    }
    (!found.is_empty()).then(|| {
        format!(
            "No domain named {domain} exists in this AWS account any more. A domain with a similar name does: {}. If it replaced the old one, put its endpoint in this source's address.",
            found.join("; ")
        )
    })
}

/// What the card offers for a VPC domain: use a network already in its VPC,
/// or add one in the domain's own subnets in a free slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Suggestion {
    /// A network already in the domain's VPC (use it), or the free one to add.
    pub network: Option<String>,
    /// True when `network` already exists.
    pub exists: bool,
    /// The stack parameter's value to add it: `vpc-…:subnet-…,subnet-…`.
    pub option_value: String,
    pub parameter: String,
}

pub fn suggest(domain: &DomainPlace, have: &[Placement]) -> Suggestion {
    if let Some(p) = have
        .iter()
        .find(|p| p.vpc_id == domain.vpc_id && !p.vpc_id.is_empty())
    {
        return Suggestion {
            network: Some(p.network.clone()),
            exists: true,
            option_value: String::new(),
            parameter: parameter(&p.network),
        };
    }
    let free = NETWORKS
        .iter()
        .find(|n| !have.iter().any(|p| p.network == **n));
    let value = format!(
        "{}:{}",
        domain.vpc_id,
        domain
            .subnet_ids
            .iter()
            .take(6)
            .cloned()
            .collect::<Vec<_>>()
            .join(",")
    );
    Suggestion {
        network: free.map(|n| n.to_string()),
        exists: false,
        option_value: if free.is_some() { value } else { String::new() },
        parameter: free.map(|n| parameter(n)).unwrap_or_default(),
    }
}

/// The inbound rule a service needs for a network: what to add, a console
/// link, and the same as one command an AWS admin runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Rule {
    pub target_group: String,
    pub source_group: String,
    pub console_url: String,
    pub command: String,
}

pub fn rule(region: &str, target_group: &str, source_group: &str) -> Rule {
    Rule {
        target_group: target_group.to_string(),
        source_group: source_group.to_string(),
        console_url: format!("https://{region}.console.aws.amazon.com/ec2/home?region={region}#ModifyInboundSecurityGroupRules:securityGroupId={target_group}"),
        command: format!(
            "aws ec2 authorize-security-group-ingress --region {region} --group-id {target_group} --ip-permissions 'IpProtocol=tcp,FromPort=443,ToPort=443,UserIdGroupPairs=[{{GroupId={source_group},Description=Coderhelm private network read-only lookups}}]'"
        ),
    }
}

/// Whether the service's security groups already let the network in on
/// 443: from its security group, or from CIDR ranges that hold every one of
/// its subnets. Read with ec2:DescribeSecurityGroups and DescribeSubnets.
pub async fn already_allowed(
    region: &str,
    target_groups: &[String],
    net: &Placement,
) -> Option<bool> {
    let conf = aws_config::from_env()
        .region(aws_config::Region::new(region.to_string()))
        .load()
        .await;
    let ec2 = aws_sdk_ec2::Client::new(&conf);
    let groups = ec2
        .describe_security_groups()
        .set_group_ids(Some(target_groups.to_vec()))
        .send()
        .await
        .ok()?;
    let subnets = ec2
        .describe_subnets()
        .set_subnet_ids(Some(net.subnet_ids.clone()))
        .send()
        .await
        .ok()?;
    let cidrs: Vec<String> = subnets
        .subnets()
        .iter()
        .filter_map(|s| s.cidr_block().map(str::to_string))
        .collect();
    let mut rules = Vec::new();
    for g in groups.security_groups() {
        for p in g.ip_permissions() {
            rules.push(InboundRule {
                protocol: p.ip_protocol().unwrap_or("").to_string(),
                from: p.from_port(),
                to: p.to_port(),
                cidrs: p
                    .ip_ranges()
                    .iter()
                    .filter_map(|r| r.cidr_ip().map(str::to_string))
                    .collect(),
                groups: p
                    .user_id_group_pairs()
                    .iter()
                    .filter_map(|r| r.group_id().map(str::to_string))
                    .collect(),
            });
        }
    }
    Some(allows(&rules, &net.security_group_ids, &cidrs))
}

/// One inbound rule of a security group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundRule {
    pub protocol: String,
    pub from: Option<i32>,
    pub to: Option<i32>,
    pub cidrs: Vec<String>,
    pub groups: Vec<String>,
}

/// Whether `rules` let tcp 443 in from `groups` or from ranges covering every one of `subnet_cidrs`.
pub fn allows(rules: &[InboundRule], groups: &[String], subnet_cidrs: &[String]) -> bool {
    let on_443 = |r: &&InboundRule| {
        (r.protocol == "-1")
            || (r.protocol == "tcp" && r.from.unwrap_or(0) <= 443 && r.to.unwrap_or(65535) >= 443)
    };
    let rules: Vec<&InboundRule> = rules.iter().filter(on_443).collect();
    if rules
        .iter()
        .any(|r| r.groups.iter().any(|g| groups.contains(g)))
    {
        return true;
    }
    !subnet_cidrs.is_empty()
        && subnet_cidrs.iter().all(|s| {
            rules
                .iter()
                .any(|r| r.cidrs.iter().any(|c| cidr_holds(c, s)))
        })
}

/// Whether range `outer` holds range `inner` (IPv4).
pub fn cidr_holds(outer: &str, inner: &str) -> bool {
    let parse = |c: &str| -> Option<(u32, u32)> {
        let (ip, bits) = c.split_once('/')?;
        let ip: std::net::Ipv4Addr = ip.parse().ok()?;
        let bits: u32 = bits.parse().ok().filter(|b| *b <= 32)?;
        Some((u32::from(ip), bits))
    };
    let (Some((o, ob)), Some((i, ib))) = (parse(outer), parse(inner)) else {
        return false;
    };
    if ib < ob {
        return false;
    }
    let mask = if ob == 0 { 0 } else { u32::MAX << (32 - ob) };
    (o & mask) == (i & mask)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn networks_are_one_to_three_and_named_by_the_install() {
        assert!(valid("") && valid("1") && valid("3"));
        assert!(!valid("4") && !valid("0") && !valid("x"));
        assert_eq!(parameter("2"), "PrivateNetwork2");
        std::env::set_var(PREFIX_ENV, "coderhelm-prod-reach-");
        assert_eq!(
            function_name("2").as_deref(),
            Some("coderhelm-prod-reach-2")
        );
        assert_eq!(function_name("4"), None);
        assert_eq!(function_name(""), None);
    }

    #[test]
    fn the_functions_answer_becomes_a_reply_or_the_failing_hop() {
        let r = answer("1", "h", &json!({"status": 200, "body": "{}"})).unwrap();
        assert_eq!((r.status, r.body.as_str()), (200, "{}"));
        let host = "vpc-orders-os-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com";
        let dns = answer(
            "1",
            host,
            &json!({"error": {"hop": "dns", "detail": "doesn't resolve"}}),
        )
        .unwrap_err();
        assert!(
            dns.message
                .starts_with("Inside private network 1, finding the name"),
            "{}",
            dns.message
        );
        assert_eq!(dns.cause.as_ref().unwrap().failure, Failure::NotFound);
        let tcp = answer(
            "2",
            host,
            &json!({"error": {"hop": "tcp", "detail": "no answer on 10.0.1.5:443"}}),
        )
        .unwrap_err();
        assert!(tcp.fix.contains("security group"), "{}", tcp.fix);
        assert_eq!(tcp.cause.as_ref().unwrap().failure, Failure::Timeout);
        assert!(answer("1", host, &json!({"nothing": 1})).is_err());
    }

    #[test]
    fn hop_checks_name_each_hop_and_the_one_that_failed() {
        let v = json!({"hops": [
            {"hop": "address", "ok": true, "detail": "an https address"},
            {"hop": "dns", "ok": true, "detail": "h is 10.2.101.7"},
            {"hop": "tcp", "ok": false, "detail": "no answer on 10.2.101.7:443 within 25 seconds"},
        ]});
        let c = hop_checks("1", &v);
        assert_eq!(
            c.iter().map(|c| (c.id, c.ok)).collect::<Vec<_>>(),
            [("address", true), ("dns", true), ("tcp", false)]
        );
        assert_eq!(c[2].label, "Connect on the port (security groups)");
        assert!(c[2].fix.as_deref().unwrap().contains("allow tcp 443"));
        assert!(c[0].fix.is_none());
        let refused = hop_checks(
            "3",
            &json!({"error": {"hop": "address", "detail": "unknown action"}}),
        );
        assert_eq!(refused[0].id, "network");
    }

    fn domain(vpc: &str) -> DomainPlace {
        DomainPlace {
            domain: "orders-os".into(),
            region: "us-east-1".into(),
            endpoint: "https://vpc-orders-os-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com"
                .into(),
            vpc_only: true,
            vpc_id: vpc.into(),
            subnet_ids: vec![
                "subnet-0a1b2c3d4e5f60711".into(),
                "subnet-0a1b2c3d4e5f60712".into(),
            ],
            security_group_ids: vec!["sg-0a1b2c3d4e5f60713".into()],
        }
    }

    fn placed(n: &str, vpc: &str) -> Placement {
        Placement {
            network: n.into(),
            vpc_id: vpc.into(),
            subnet_ids: vec![],
            security_group_ids: vec!["sg-0a1b2c3d4e5f60799".into()],
            state: "Active".into(),
        }
    }

    #[test]
    fn a_domain_uses_the_network_in_its_vpc_or_gets_the_next_free_one() {
        let d = domain("vpc-0a1b2c3d4e5f60710");
        let s = suggest(&d, &[]);
        assert_eq!((s.network.as_deref(), s.exists), (Some("1"), false));
        assert_eq!(s.parameter, "PrivateNetwork1");
        assert_eq!(
            s.option_value,
            "vpc-0a1b2c3d4e5f60710:subnet-0a1b2c3d4e5f60711,subnet-0a1b2c3d4e5f60712"
        );
        let s = suggest(
            &d,
            &[
                placed("1", "vpc-other"),
                placed("2", "vpc-0a1b2c3d4e5f60710"),
            ],
        );
        assert_eq!(
            (s.network.as_deref(), s.exists, s.option_value.as_str()),
            (Some("2"), true, "")
        );
        let s = suggest(&d, &[placed("1", "a"), placed("2", "b"), placed("3", "c")]);
        assert_eq!((s.network, s.option_value.as_str()), (None, ""));
    }

    #[test]
    fn a_replacement_domain_is_one_with_the_same_name_but_a_trailing_number_or_suffix() {
        assert!(similar_names("orders-os", "orders-os2"));
        assert!(similar_names("orders-os2", "orders-os"));
        assert!(similar_names("orders-os", "orders-os-new"));
        assert!(!similar_names("orders-os", "orders-os"));
        assert!(!similar_names("staging-orders-os", "prod-orders-os"));
        assert!(!similar_names("ab1", "ab2"), "too short to say");
    }

    #[test]
    fn the_rule_is_one_inbound_443_from_the_networks_group() {
        let r = rule("us-east-1", "sg-0a1b2c3d4e5f60713", "sg-0a1b2c3d4e5f60799");
        assert!(r.command.starts_with("aws ec2 authorize-security-group-ingress --region us-east-1 --group-id sg-0a1b2c3d4e5f60713"));
        assert!(
            r.command.contains("FromPort=443,ToPort=443")
                && r.command.contains("GroupId=sg-0a1b2c3d4e5f60799")
        );
        assert!(r
            .console_url
            .contains("securityGroupId=sg-0a1b2c3d4e5f60713"));
    }

    #[test]
    fn a_service_already_lets_the_network_in_by_group_or_by_its_subnets_ranges() {
        let r = |p: &str, f, t, cidrs: &[&str], groups: &[&str]| InboundRule {
            protocol: p.into(),
            from: Some(f),
            to: Some(t),
            cidrs: cidrs.iter().map(|s| s.to_string()).collect(),
            groups: groups.iter().map(|s| s.to_string()).collect(),
        };
        let mine = vec!["sg-mine".to_string()];
        let subnets = vec!["10.2.101.0/24".to_string(), "10.2.102.0/24".to_string()];
        assert!(allows(
            &[r("tcp", 443, 443, &[], &["sg-mine"])],
            &mine,
            &subnets
        ));
        assert!(allows(
            &[r("tcp", 443, 443, &["10.2.101.0/24", "10.2.102.0/24"], &[])],
            &mine,
            &subnets
        ));
        assert!(allows(
            &[r("tcp", 0, 65535, &["10.2.0.0/16"], &[])],
            &mine,
            &subnets
        ));
        assert!(
            !allows(
                &[r("tcp", 443, 443, &["10.2.101.0/24"], &[])],
                &mine,
                &subnets
            ),
            "one subnet left out"
        );
        assert!(
            !allows(
                &[r("tcp", 9200, 9200, &["10.2.0.0/16"], &["sg-mine"])],
                &mine,
                &subnets
            ),
            "another port"
        );
        assert!(allows(&[r("-1", 0, 0, &[], &["sg-mine"])], &mine, &subnets));
        assert!(
            cidr_holds("10.0.0.0/8", "10.2.101.0/24")
                && !cidr_holds("10.2.101.0/24", "10.2.0.0/16")
        );
        assert!(!cidr_holds("bad", "10.0.0.0/8"));
    }
}
