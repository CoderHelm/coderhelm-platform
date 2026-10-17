//! Talks to a team's GraphQL API under the rules in [`super`].

use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{json, Value};

use super::graphql::{
    check_read_only, introspection_query, schema_from_introspection, Schema, INTROSPECTION_LEVELS,
};
use super::net::{parse_endpoint, resolve_public};
use super::redact::{hidden_terms, is_hidden, redact};
use super::StepError;

const TIMEOUT: Duration = Duration::from_secs(8);
/// Total time allowed for asking an API to describe itself, across all forms.
const DESCRIBE_BUDGET: Duration = Duration::from_secs(12);
/// Most of a response we will read. A lookup that returns more is cut off.
const MAX_RESPONSE_BYTES: usize = 1_000_000;
const USER_AGENT: &str = "Coderhelm-DataSources/1.0";

/// How the key is presented to the API. Found by trying each when a source is
/// connected, so nobody has to know which one their API uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthStyle {
    XApiKey,
    Bearer,
    Authorization,
}

impl AuthStyle {
    pub const ALL: [AuthStyle; 3] = [
        AuthStyle::XApiKey,
        AuthStyle::Bearer,
        AuthStyle::Authorization,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            AuthStyle::XApiKey => "x-api-key",
            AuthStyle::Bearer => "bearer",
            AuthStyle::Authorization => "authorization",
        }
    }

    pub fn parse(s: &str) -> AuthStyle {
        match s {
            "bearer" => AuthStyle::Bearer,
            "authorization" => AuthStyle::Authorization,
            _ => AuthStyle::XApiKey,
        }
    }

    pub(crate) fn header(self, key: &str) -> (&'static str, String) {
        match self {
            AuthStyle::XApiKey => ("x-api-key", key.to_string()),
            AuthStyle::Bearer => ("authorization", format!("Bearer {key}")),
            AuthStyle::Authorization => ("authorization", key.to_string()),
        }
    }
}

/// A connected API and the key to read it.
#[derive(Clone)]
pub struct ApiSource {
    pub url: String,
    pub auth: AuthStyle,
    pub key: String,
}

struct Response {
    status: u16,
    body: Option<Value>,
    bytes: usize,
    truncated: bool,
}

impl Response {
    fn data(&self) -> Option<&Value> {
        self.body
            .as_ref()
            .map(|b| &b["data"])
            .filter(|d| !d.is_null())
    }
    fn first_error(&self) -> Option<String> {
        let msg = self.body.as_ref()?["errors"][0]["message"].as_str()?;
        Some(crate::truncate_str(msg, 300).to_string())
    }
    fn looks_like_graphql(&self) -> bool {
        self.body
            .as_ref()
            .map(|b| b.get("data").is_some() || b.get("errors").is_some())
            .unwrap_or(false)
    }
}

/// Sends one checked read to the registered address. The body is built here
/// from the checked text: no operation name and no extensions are sent, so the
/// server can only run what was checked.
async fn send(
    url: &str,
    auth: Option<(AuthStyle, &str)>,
    query: &str,
    variables: Option<&Value>,
) -> Result<Response, StepError> {
    let read = check_read_only(query)?;
    let ep = parse_endpoint(url)?;
    let addr = resolve_public(&ep).await?;

    let cant_reach = || {
        StepError::new(
            "reach",
            format!("We couldn't reach {}.", ep.host),
            "Check the address for a typo. If your API only accepts approved callers, it needs to allow Coderhelm.",
        )
    };
    let client = reqwest::Client::builder()
        .https_only(true)
        // Never route through a proxy named in the environment.
        .no_proxy()
        // A redirect could point anywhere, including inside a private network.
        .redirect(reqwest::redirect::Policy::none())
        // Connect to the address that was checked, not whatever the name
        // resolves to a moment later.
        .resolve(&ep.host, addr)
        .timeout(TIMEOUT)
        .user_agent(USER_AGENT)
        .build()
        .map_err(|_| cant_reach())?;

    let mut body = json!({ "query": read.text });
    if let Some(v) = variables.filter(|v| v.is_object()) {
        body["variables"] = v.clone();
    }
    let mut req = client.post(ep.url.clone()).json(&body);
    if let Some((style, key)) = auth {
        let (name, value) = style.header(key);
        req = req.header(name, value);
    }
    let mut resp = req.send().await.map_err(|_| cant_reach())?;
    let status = resp.status().as_u16();
    if (300..400).contains(&status) {
        return Err(StepError::new(
            "reach",
            "That address sends us somewhere else, and we don't follow redirects.",
            "Paste the final address of the API.",
        ));
    }

    let mut buf: Vec<u8> = Vec::new();
    let mut truncated = false;
    while let Some(chunk) = resp.chunk().await.map_err(|_| cant_reach())? {
        if buf.len() + chunk.len() > MAX_RESPONSE_BYTES {
            truncated = true;
            break;
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(Response {
        status,
        body: serde_json::from_slice(&buf).ok(),
        bytes: buf.len(),
        truncated,
    })
}

const TYPENAME: &str = "{ __typename }";

/// What we learned from the address alone, before any key.
#[derive(Debug, Serialize)]
pub struct Probe {
    pub ok: bool,
    pub kind: Option<&'static str>,
    pub needs_key: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<StepError>,
}

pub async fn probe(url: &str) -> Probe {
    match send(url, None, TYPENAME, None).await {
        Err(e) => Probe {
            ok: false,
            kind: None,
            needs_key: false,
            error: Some(e),
        },
        Ok(r) => {
            let open = r.status == 200 && r.data().is_some();
            // An API that wants a key usually answers 401/403 without one.
            let graphql = r.looks_like_graphql() || matches!(r.status, 401 | 403);
            if graphql {
                Probe {
                    ok: true,
                    kind: Some("graphql"),
                    needs_key: !open,
                    error: None,
                }
            } else {
                Probe {
                    ok: false,
                    kind: None,
                    needs_key: false,
                    error: Some(StepError::new(
                        "reach",
                        "Something answered at that address, but it isn't a GraphQL API.",
                        "GraphQL addresses usually end in /graphql. Ask an engineer for the exact address.",
                    )),
                }
            }
        }
    }
}

/// One named check shown to the person connecting a source.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Check {
    pub id: &'static str,
    pub label: &'static str,
    pub ok: bool,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix: Option<String>,
}

impl Check {
    fn pass(id: &'static str, label: &'static str, detail: impl Into<String>) -> Self {
        Check {
            id,
            label,
            ok: true,
            detail: detail.into(),
            fix: None,
        }
    }
    fn fail(id: &'static str, label: &'static str, e: &StepError) -> Self {
        Check {
            id,
            label,
            ok: false,
            detail: e.message.clone(),
            fix: Some(e.fix.clone()),
        }
    }
}

const L_REACH: &str = "Reach the address";
const L_SIGN_IN: &str = "The key is accepted";
const L_READ: &str = "Read the list of data";
const L_READ_ONLY: &str = "Only reads are possible";

/// The outcome of checking an address and key together.
pub struct ConnectReport {
    pub ok: bool,
    pub checks: Vec<Check>,
    pub auth: AuthStyle,
    pub schema: Option<Schema>,
}

pub async fn connect_checks(url: &str, key: &str) -> ConnectReport {
    let mut checks = Vec::new();
    let failed = |checks: Vec<Check>| ConnectReport {
        ok: false,
        checks,
        auth: AuthStyle::XApiKey,
        schema: None,
    };

    let open = match send(url, None, TYPENAME, None).await {
        Err(e) => {
            checks.push(Check::fail("reach", L_REACH, &e));
            return failed(checks);
        }
        Ok(r) => {
            checks.push(Check::pass("reach", L_REACH, "The API answered."));
            r.status == 200 && r.data().is_some()
        }
    };

    // Tried together so the whole check fits in one short request.
    let accepted = |r: Result<Response, StepError>| {
        r.map(|r| r.status == 200 && r.data().is_some())
            .unwrap_or(false)
    };
    let (a, b, c) = tokio::join!(
        send(url, Some((AuthStyle::ALL[0], key)), TYPENAME, None),
        send(url, Some((AuthStyle::ALL[1], key)), TYPENAME, None),
        send(url, Some((AuthStyle::ALL[2], key)), TYPENAME, None),
    );
    let auth = [a, b, c]
        .into_iter()
        .zip(AuthStyle::ALL)
        .find_map(|(r, style)| accepted(r).then_some(style));
    let auth = match (auth, open) {
        // The API answers with or without a key, so the key proves nothing.
        (Some(a), true) => {
            checks.push(Check::pass(
                "sign_in",
                L_SIGN_IN,
                "This API also answers without a key, so we couldn't confirm yours is needed.",
            ));
            a
        }
        (Some(a), false) => {
            checks.push(Check::pass(
                "sign_in",
                L_SIGN_IN,
                "The API accepted the key.",
            ));
            a
        }
        (None, _) => {
            let e = StepError::new(
                "sign_in",
                "That key was rejected.",
                "It may have expired, or a character was missed when copying. Paste it again, or ask for a new one.",
            );
            checks.push(Check::fail("sign_in", L_SIGN_IN, &e));
            return failed(checks);
        }
    };

    // Shallower forms lose some detail about lists but pass APIs that cap depth.
    // The whole attempt has one time budget so a slow API can't stall the check.
    let describe = async {
        for levels in (0..=INTROSPECTION_LEVELS).rev() {
            let found = send(url, Some((auth, key)), &introspection_query(levels), None)
                .await
                .ok()
                .and_then(|r| r.body.as_ref().and_then(schema_from_introspection));
            if found.is_some() {
                return found;
            }
        }
        None
    };
    let schema = tokio::time::timeout(DESCRIBE_BUDGET, describe)
        .await
        .ok()
        .flatten();
    checks.push(match &schema {
        Some(s) => Check::pass("read", L_READ, format!("It can look up {} kinds of data.", s.kinds)),
        None => Check::pass(
            "read",
            L_READ,
            "This API keeps its list of data private. Lookups still work; the reviewer just gets less guidance.",
        ),
    });

    let writes = match &schema {
        Some(s) if s.has_mutations => "This API can also change data. Coderhelm can't: anything that isn't a pure read is refused before it is sent.",
        _ => "Anything that isn't a pure read is refused before it is sent.",
    };
    checks.push(Check::pass("read_only", L_READ_ONLY, writes));

    ConnectReport {
        ok: true,
        checks,
        auth,
        schema,
    }
}

/// The result of one lookup, ready to show or to hand to a model.
#[derive(Debug, Clone)]
pub struct Lookup {
    /// The exact text that was sent.
    pub query: String,
    pub fields: Vec<String>,
    /// Pretty-printed response data with hidden fields replaced.
    pub result: String,
    pub hidden_count: usize,
    pub bytes: usize,
    pub ms: u128,
}

/// Runs one read against a connected API. `hidden` are the field names whose
/// values must never leave this function.
pub async fn run_lookup(
    src: &ApiSource,
    query: &str,
    variables: Option<&Value>,
    hidden: &[String],
    max_result_chars: usize,
) -> Result<Lookup, StepError> {
    let read = check_read_only(query)?;
    // A hidden field is refused outright, so its value never leaves the team's
    // system. Checked by real field name: renaming it with an alias (which
    // would slip past hiding by name in the response) changes nothing.
    let terms = hidden_terms(hidden);
    if let Some(field) = read.all_fields.iter().find(|f| is_hidden(f, &terms)) {
        return Err(StepError::new(
            "read_only",
            format!("That lookup asks for a hidden field ({field})."),
            "Hidden fields can't be read. Remove it from the lookup. Nothing was sent.",
        ));
    }
    let started = Instant::now();
    let r = send(&src.url, Some((src.auth, &src.key)), &read.text, variables).await?;
    let ms = started.elapsed().as_millis();

    if matches!(r.status, 401 | 403) {
        return Err(StepError::new(
            "sign_in",
            "The API no longer accepts the saved key.",
            "Replace the key in Data sources.",
        ));
    }
    if r.truncated {
        return Err(StepError::new(
            "read",
            "The answer was too large to read.",
            "Ask for fewer rows or fewer fields.",
        ));
    }
    let Some(mut data) = r.data().cloned() else {
        let why = r
            .first_error()
            .unwrap_or_else(|| format!("The API answered with status {}.", r.status));
        return Err(StepError::new(
            "read",
            format!("The API couldn't answer that lookup: {why}"),
            "Check the field names against what this API offers.",
        ));
    };

    // Second line of defence for values nested inside a field the query
    // couldn't name (a JSON-typed field, say).
    let hidden_count = redact(&mut data, &terms);
    let pretty = serde_json::to_string_pretty(&data).unwrap_or_default();
    Ok(Lookup {
        query: read.text,
        fields: read.fields,
        result: crate::head_tail_str(&pretty, max_result_chars),
        hidden_count,
        bytes: r.bytes,
        ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(url: &str) -> ApiSource {
        ApiSource {
            url: url.to_string(),
            auth: AuthStyle::XApiKey,
            key: "k".to_string(),
        }
    }

    // These hold without any network: each is refused before a connection is made.

    #[tokio::test]
    async fn a_write_is_refused_before_anything_is_sent() {
        // The address is unroutable on purpose: reaching for it would fail with
        // a `reach` error, so a `read_only` error proves nothing was attempted.
        let e = run_lookup(
            &src("https://10.0.0.1/graphql"),
            "mutation { wipe }",
            None,
            &[],
            1000,
        )
        .await
        .unwrap_err();
        assert!(e.is_refusal());
        assert_eq!(e.step, "read_only");
    }

    #[tokio::test]
    async fn a_hidden_field_is_refused_even_behind_an_alias() {
        let hidden = vec!["email".to_string()];
        let unreachable = src("https://10.0.0.1/graphql");
        for q in [
            "{ people { email } }",
            "{ people { x: email } }",
            "{ people { ...P } } fragment P on Person { contactEmail }",
        ] {
            let e = run_lookup(&unreachable, q, None, &hidden, 1000)
                .await
                .unwrap_err();
            assert!(e.is_refusal(), "{q}");
            assert!(e.message.contains("hidden field"), "{q}");
        }
        // Without it hidden, the same lookup gets as far as the address check.
        let e = run_lookup(&unreachable, "{ people { email } }", None, &[], 1000)
            .await
            .unwrap_err();
        assert_eq!(e.step, "reach");
    }

    #[tokio::test]
    async fn private_addresses_are_refused() {
        for url in [
            "https://10.0.0.1/graphql",
            "https://169.254.169.254/graphql",
            "https://localhost/graphql",
            "http://api.example.com/graphql",
        ] {
            let e = run_lookup(&src(url), "{ a }", None, &[], 1000)
                .await
                .unwrap_err();
            assert_eq!(e.step, "reach", "{url}");
            let p = probe(url).await;
            assert!(!p.ok, "{url}");
        }
    }

    // Against a real public GraphQL API. Run with `cargo test -- --ignored`.
    const PUBLIC_API: &str = "https://countries.trevorblades.com/graphql";

    #[tokio::test]
    #[ignore = "needs network"]
    async fn a_real_api_can_be_probed_checked_and_read() {
        let p = probe(PUBLIC_API).await;
        assert!(p.ok, "{:?}", p.error);
        assert_eq!(p.kind, Some("graphql"));

        let report = connect_checks(PUBLIC_API, "any-key-this-api-is-open").await;
        assert!(report.ok);
        let ids: Vec<&str> = report.checks.iter().map(|c| c.id).collect();
        assert_eq!(ids, vec!["reach", "sign_in", "read", "read_only"]);
        let schema = report.schema.expect("this API describes itself");
        assert!(schema.kinds > 0);
        assert!(schema.text.starts_with("type Query {"));

        let hidden = vec!["capital".to_string()];
        let asks_hidden = "{ country(code: \"BR\") { name capital } }";
        let e = run_lookup(&src(PUBLIC_API), asks_hidden, None, &hidden, 2000)
            .await
            .unwrap_err();
        assert!(e.is_refusal());
        let l = run_lookup(
            &src(PUBLIC_API),
            "{ country(code: \"BR\") { name } }",
            None,
            &hidden,
            2000,
        )
        .await
        .unwrap();
        assert_eq!(l.fields, vec!["country"]);
        assert!(l.result.contains("Brazil"));
        assert_eq!(l.hidden_count, 0);

        for (_, q) in &schema.samples {
            assert!(check_read_only(q).is_ok());
        }
    }

    #[tokio::test]
    #[ignore = "needs network"]
    async fn a_redirect_is_not_followed() {
        let e = run_lookup(
            &src("https://httpbin.org/redirect-to?url=https%3A%2F%2Fexample.com"),
            "{ a }",
            None,
            &[],
            1000,
        )
        .await
        .unwrap_err();
        assert_eq!(e.step, "reach");
    }

    #[test]
    fn auth_styles_round_trip() {
        for a in AuthStyle::ALL {
            assert_eq!(AuthStyle::parse(a.as_str()), a);
        }
        assert_eq!(AuthStyle::parse("unknown"), AuthStyle::XApiKey);
        assert_eq!(
            AuthStyle::Bearer.header("k"),
            ("authorization", "Bearer k".to_string())
        );
        assert_eq!(
            AuthStyle::XApiKey.header("k"),
            ("x-api-key", "k".to_string())
        );
    }
}
