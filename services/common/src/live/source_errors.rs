//! What a failed lookup says, by the kind of source it was.
//!
//! The network layer ([`super::net`], [`super::http`]) and each client
//! ([`super::opensearch`], [`super::rest`], [`super::api`]) mark a failure
//! with a [`Failure`]. [`for_source`] turns a marked failure into words for
//! that kind of source: what likely happened, and what to do, with an
//! address example of the right shape. So an OpenSearch cluster that no
//! longer resolves never gets a GraphQL example, and an AWS domain that was
//! deleted or moved into a VPC says so.

use super::StepError;

/// The kinds of source a lookup reads, as people know them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceType {
    GraphQl,
    Rest,
    OpenSearch,
    Postgres,
    Redshift,
    Logs,
}

impl SourceType {
    pub const ALL: [SourceType; 6] = [
        SourceType::GraphQl,
        SourceType::Rest,
        SourceType::OpenSearch,
        SourceType::Postgres,
        SourceType::Redshift,
        SourceType::Logs,
    ];

    /// "the cluster", "the API": how a sentence names it.
    pub fn noun(self) -> &'static str {
        match self {
            SourceType::GraphQl | SourceType::Rest => "the API",
            SourceType::OpenSearch => "the cluster",
            SourceType::Postgres | SourceType::Redshift => "the database",
            SourceType::Logs => "CloudWatch Logs",
        }
    }

    /// What its address looks like, for a person fixing one.
    pub fn address_example(self) -> &'static str {
        match self {
            SourceType::GraphQl => "It looks like https://api.yourcompany.com/graphql.",
            SourceType::Rest => "It is the API's base address, like https://api.yourcompany.com (paths are listed separately).",
            SourceType::OpenSearch => OPENSEARCH_EXAMPLE,
            SourceType::Postgres => "A PostgreSQL source has no address: it is read through the RDS Data API with the cluster's ARN and its secret.",
            SourceType::Redshift => "A Redshift source has no address: it is read through the Redshift Data API with the cluster or workgroup name.",
            SourceType::Logs => "A logs source has no address: it reads the log groups you listed through your read-only role.",
        }
    }

    /// Where its sign-in is changed.
    fn sign_in_fix(self) -> &'static str {
        match self {
            SourceType::GraphQl | SourceType::Rest => "Paste a current key: Data sources, the source, Replace key.",
            SourceType::OpenSearch => "Check the user and its password (OpenSearch fine-grained access control, internal users), then Data sources, the source, Replace key.",
            SourceType::Postgres | SourceType::Redshift | SourceType::Logs => "Run the source's setup script again so the read-only role and its secret match.",
        }
    }
}

const OPENSEARCH_EXAMPLE: &str = "An AWS OpenSearch domain's address looks like https://search-<domain>-<id>.<region>.es.amazonaws.com, or https://vpc-<domain>-<id>.<region>.es.amazonaws.com for a domain inside a VPC; your own cluster's is its https address and port, like https://search.yourcompany.com:9200.";

/// Why a lookup failed, as the network or the service said.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    /// The name doesn't resolve.
    NotFound,
    /// The name resolves to a private address (inside a network).
    PrivateAddress,
    /// The connection was refused or reset.
    Refused,
    /// Nothing answered in time.
    Timeout,
    /// The secure connection failed (certificate, protocol).
    Tls,
    /// 401: the key or password wasn't accepted.
    Unauthorized,
    /// 403: signed in, but not allowed to read this.
    Forbidden,
    /// 404: no such index, path or table.
    Missing,
    /// 429: too many requests.
    RateLimited,
    /// 5xx: the service failed.
    ServerError,
}

impl Failure {
    pub const ALL: [Failure; 10] = [
        Failure::NotFound,
        Failure::PrivateAddress,
        Failure::Refused,
        Failure::Timeout,
        Failure::Tls,
        Failure::Unauthorized,
        Failure::Forbidden,
        Failure::Missing,
        Failure::RateLimited,
        Failure::ServerError,
    ];

    /// The failure an HTTP status means, if it is one of these.
    pub fn from_status(status: u16) -> Option<Failure> {
        match status {
            401 => Some(Failure::Unauthorized),
            403 => Some(Failure::Forbidden),
            404 => Some(Failure::Missing),
            429 => Some(Failure::RateLimited),
            500..=599 => Some(Failure::ServerError),
            _ => None,
        }
    }
}

/// A failure's mark on a [`StepError`]: which failure, and the host it hit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cause {
    pub failure: Failure,
    pub host: String,
}

/// The parts of an AWS OpenSearch Service address: (`search` or `vpc`,
/// domain name, region). None for any other host.
pub fn aws_opensearch_host(host: &str) -> Option<(&str, &str, &str)> {
    let rest = host.strip_suffix(".es.amazonaws.com")?;
    let (label, region) = rest.split_once('.')?;
    if region.contains('.') || region.is_empty() {
        return None;
    }
    let (kind, after) = label.split_once('-')?;
    if kind != "search" && kind != "vpc" {
        return None;
    }
    // <domain>-<26-character id>; the domain may itself hold hyphens.
    let (domain, id) = after.rsplit_once('-')?;
    let ok = id.len() == 26
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && (3..=28).contains(&domain.len())
        && domain.starts_with(|c: char| c.is_ascii_lowercase());
    ok.then_some((kind, domain, region))
}

/// The words for `failure` on a source of type `ty` at `host`: (step,
/// message, fix).
pub fn words(ty: SourceType, failure: Failure, host: &str) -> (&'static str, String, String) {
    let noun = ty.noun();
    let example = ty.address_example();
    match failure {
        Failure::NotFound => match (ty, aws_opensearch_host(host)) {
            (SourceType::OpenSearch, Some((_, domain, region))) => (
                "reach",
                format!("{host} no longer exists. AWS removes a domain's address when the domain is deleted or replaced, for example when it is moved into a VPC (it then gets a new vpc-{domain}-… address)."),
                format!("Find the domain's current endpoint (OpenSearch Service console in {region}, the domain, Endpoint) and put it in this source's address. A vpc-… endpoint is reachable only inside its VPC: add a private network for it on this source's card."),
            ),
            _ => (
                "reach",
                format!("{host} doesn't resolve: the name is misspelled, or {noun} was moved or removed."),
                format!("Check the address. {example}"),
            ),
        },
        Failure::PrivateAddress => (
            "reach",
            format!("{host} is inside a private network (it resolves to a private address), so it can't be reached over the internet."),
            match ty {
                SourceType::OpenSearch | SourceType::Rest | SourceType::GraphQl => format!(
                    "On a self-hosted install, reach it through a private network: add one on this source's card and Coderhelm runs a small read-only lookup function in that VPC. On Coderhelm's hosted service, a private {} can't be reached; give it a read-only https address on the internet instead.",
                    if ty == SourceType::OpenSearch { "cluster" } else { "API" }
                ),
                _ => "This source reads through AWS's APIs, not an address; run its setup script again.".to_string(),
            },
        ),
        Failure::Refused => (
            "reach",
            format!("{host} refused the connection."),
            match ty {
                SourceType::OpenSearch => "Check the address and port (AWS domains answer on 443; your own cluster often on 9200), and that the cluster accepts connections from Coderhelm.".to_string(),
                _ => format!("Check the address and that {noun} accepts connections from Coderhelm. {example}"),
            },
        ),
        Failure::Timeout => (
            "reach",
            format!("{host} didn't answer in time."),
            match ty {
                SourceType::OpenSearch => "If the domain is inside a VPC, it can't be reached over the internet: add a private network on this source's card. Otherwise its access policy or a firewall drops Coderhelm's requests.".to_string(),
                _ => format!("Check that {noun} is up and that a firewall doesn't drop Coderhelm's requests."),
            },
        ),
        Failure::Tls => (
            "reach",
            format!("The secure connection to {host} failed (its certificate isn't valid for that name, or it doesn't speak HTTPS on that port)."),
            match ty {
                SourceType::OpenSearch => "Use the domain's own endpoint, not a custom name without a matching certificate, and https on the port the cluster serves TLS on.".to_string(),
                _ => format!("Use {noun}'s https address with a valid certificate. {example}"),
            },
        ),
        Failure::Unauthorized => (
            "sign_in",
            match ty {
                SourceType::OpenSearch => "The cluster didn't accept the user and password.".to_string(),
                _ => format!("{} didn't accept the key.", capital(noun)),
            },
            ty.sign_in_fix().to_string(),
        ),
        Failure::Forbidden => (
            "read",
            match ty {
                SourceType::OpenSearch => "Signed in, but that user can't read this (its role has no read on the index, or the domain's access policy refuses it).".to_string(),
                SourceType::Rest | SourceType::GraphQl => "The key can't read that.".to_string(),
                _ => "The read-only role can't read that.".to_string(),
            },
            match ty {
                SourceType::OpenSearch => "Give the user's role read on the index (indices:data/read/*, indices:admin/mappings/get), or read an index it can see.",
                SourceType::Rest | SourceType::GraphQl => "Read something the key has access to, or use a key that may read it.",
                _ => "Run the source's setup script again to give the role read access.",
            }
            .to_string(),
        ),
        Failure::Missing => (
            "read",
            match ty {
                SourceType::OpenSearch => "No such index (or the user can't see it).".to_string(),
                SourceType::Rest => "The API has nothing at that path.".to_string(),
                SourceType::GraphQl => "Nothing answers at that address.".to_string(),
                SourceType::Postgres | SourceType::Redshift => "No such table (or the user can't see it).".to_string(),
                SourceType::Logs => "No such log group.".to_string(),
            },
            match ty {
                SourceType::OpenSearch => "List the indexes with `search_read` action \"indexes\".",
                SourceType::Rest => "Paths can't be guessed. Find where the team's code calls this API (search the repos for its address or a path segment) and copy that exact path, with every parent id in it, before trying again.",
                SourceType::GraphQl => "GraphQL addresses usually end in /graphql. Ask an engineer for the exact address.",
                SourceType::Postgres | SourceType::Redshift => "Look the table up in information_schema.columns first.",
                SourceType::Logs => "Pick one of the source's log groups.",
            }
            .to_string(),
        ),
        Failure::RateLimited => (
            "read",
            format!("{} is limiting requests right now.", capital(noun)),
            "Wait a minute and try again; ask for fewer documents or rows at a time.".to_string(),
        ),
        Failure::ServerError => (
            "read",
            format!("{} failed while answering.", capital(noun)),
            match ty {
                SourceType::OpenSearch => "Try again in a minute. If it keeps failing, check the cluster's health (red status, full disk) in its console.",
                _ => "Try again in a minute.",
            }
            .to_string(),
        ),
    }
}

fn capital(s: &str) -> String {
    let mut c = s.chars();
    c.next()
        .map(|f| f.to_uppercase().collect::<String>() + c.as_str())
        .unwrap_or_default()
}

impl StepError {
    /// A failure marked with what happened and where, worded generically
    /// until [`for_source`] says what kind of source it was.
    pub fn marked(
        failure: Failure,
        host: &str,
        message: impl Into<String>,
        fix: impl Into<String>,
    ) -> Self {
        let (step, _, _) = words(SourceType::Rest, failure, host);
        let mut e = StepError::new(step, message, fix);
        e.cause = Some(Cause {
            failure,
            host: host.to_string(),
        });
        e
    }
}

/// `e` in the words of a `ty` source, when it was marked; otherwise as it is.
pub fn for_source(ty: SourceType, e: StepError) -> StepError {
    let Some(cause) = e.cause.clone() else {
        return e;
    };
    let (step, message, fix) = words(ty, cause.failure, &cause.host);
    let mut out = StepError::new(step, message, fix);
    out.cause = Some(cause);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOST: &str = "search-orders-os-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com";

    fn all_words(ty: SourceType) -> Vec<(Failure, String)> {
        Failure::ALL
            .iter()
            .flat_map(|f| {
                ["api.example.org", HOST].map(|h| {
                    let (_, m, x) = words(ty, *f, h);
                    (*f, format!("{m} {x}"))
                })
            })
            .collect()
    }

    #[test]
    fn no_message_for_a_source_that_isnt_graphql_carries_a_graphql_example() {
        for ty in SourceType::ALL {
            if ty == SourceType::GraphQl {
                continue;
            }
            assert!(
                !ty.address_example().to_lowercase().contains("graphql"),
                "{ty:?}"
            );
            for (f, text) in all_words(ty) {
                assert!(
                    !text.to_lowercase().contains("graphql"),
                    "{ty:?} {f:?}: {text}"
                );
            }
        }
    }

    #[test]
    fn every_failure_has_words_for_every_type_naming_the_host_where_it_matters() {
        for ty in SourceType::ALL {
            for f in Failure::ALL {
                let (step, m, x) = words(ty, f, "api.example.org");
                assert!(!m.is_empty() && !x.is_empty(), "{ty:?} {f:?}");
                assert!(["reach", "sign_in", "read"].contains(&step));
                if matches!(
                    f,
                    Failure::NotFound
                        | Failure::PrivateAddress
                        | Failure::Refused
                        | Failure::Timeout
                        | Failure::Tls
                ) {
                    assert!(m.contains("api.example.org"), "{ty:?} {f:?}: {m}");
                }
                assert!(!format!("{m}{x}").contains('→'), "no arrows: {m}{x}");
            }
        }
    }

    #[test]
    fn opensearch_words_per_failure() {
        let w = |f, h| {
            let (s, m, x) = words(SourceType::OpenSearch, f, h);
            (s, format!("{m} {x}"))
        };
        // A deleted or replaced AWS domain: says so, points at the console and the private network.
        let (s, t) = w(Failure::NotFound, HOST);
        assert_eq!(s, "reach");
        assert!(
            t.contains("no longer exists")
                && t.contains("moved into a VPC")
                && t.contains("vpc-orders-os-"),
            "{t}"
        );
        assert!(
            t.contains("us-east-1") && t.contains("private network"),
            "{t}"
        );
        // Any other cluster: the right shape of address.
        let (_, t) = w(Failure::NotFound, "search.example.org");
        assert!(t.contains("es.amazonaws.com") && t.contains(":9200"), "{t}");
        assert!(w(Failure::PrivateAddress, HOST)
            .1
            .contains("private network"));
        assert!(w(Failure::Timeout, HOST).1.contains("inside a VPC"));
        assert!(w(Failure::Refused, HOST).1.contains("9200"));
        assert!(w(Failure::Tls, HOST).1.contains("certificate"));
        assert_eq!(w(Failure::Unauthorized, HOST).0, "sign_in");
        assert!(w(Failure::Unauthorized, HOST)
            .1
            .contains("user and password"));
        assert!(w(Failure::Forbidden, HOST).1.contains("indices:data/read"));
        assert!(w(Failure::Missing, HOST).1.contains("No such index"));
        assert!(w(Failure::RateLimited, HOST)
            .1
            .contains("The cluster is limiting"));
        assert!(w(Failure::ServerError, HOST).1.contains("health"));
    }

    #[test]
    fn rest_graphql_and_aws_sources_per_failure() {
        let t = |ty, f| {
            let (_, m, x) = words(ty, f, "api.example.org");
            format!("{m} {x}")
        };
        assert!(t(SourceType::Rest, Failure::NotFound)
            .contains("base address, like https://api.yourcompany.com"));
        assert!(t(SourceType::Rest, Failure::Missing).contains("nothing at that path"));
        assert!(t(SourceType::Rest, Failure::Unauthorized).contains("didn't accept the key"));
        assert!(t(SourceType::GraphQl, Failure::NotFound).contains("/graphql"));
        assert!(t(SourceType::GraphQl, Failure::Missing).contains("/graphql"));
        for ty in [SourceType::Postgres, SourceType::Redshift, SourceType::Logs] {
            assert!(!t(ty, Failure::NotFound).contains("https://"), "{ty:?}");
            assert!(
                t(ty, Failure::Unauthorized).contains("setup script"),
                "{ty:?}"
            );
        }
        assert!(t(SourceType::Postgres, Failure::Missing).contains("table"));
        assert!(t(SourceType::Logs, Failure::Missing).contains("log group"));
    }

    #[test]
    fn aws_opensearch_hosts_are_told_apart() {
        assert_eq!(
            aws_opensearch_host(HOST),
            Some(("search", "orders-os", "us-east-1"))
        );
        assert_eq!(
            aws_opensearch_host("vpc-a-b-c-0123456789abcdefghijklmnop.eu-west-1.es.amazonaws.com"),
            Some(("vpc", "a-b-c", "eu-west-1"))
        );
        for h in [
            "search.example.org",
            "search-orders-short.us-east-1.es.amazonaws.com",
            "x-orders-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com",
            "es.amazonaws.com",
        ] {
            assert_eq!(aws_opensearch_host(h), None, "{h}");
        }
    }

    #[test]
    fn a_marked_failure_takes_the_sources_words_and_an_unmarked_one_stays() {
        let e = StepError::marked(Failure::NotFound, HOST, "generic", "generic");
        let os = for_source(SourceType::OpenSearch, e.clone());
        assert!(os.message.contains("no longer exists"));
        assert!(!os.fix.contains("graphql"));
        let plain = StepError::new("read_only", "refused", "do this");
        assert_eq!(for_source(SourceType::OpenSearch, plain.clone()), plain);
    }
}
