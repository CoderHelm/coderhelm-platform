//! A small in-process stand-in for the AWS services sign-in calls (DynamoDB,
//! Cognito and KMS), so tests run the real handlers end to end against
//! records they set up. It speaks the services' JSON protocols on a local
//! port; the SDK clients are pointed at it with an endpoint override.
//!
//! What it understands is what the sign-in code sends: items by key,
//! queries on a key (and `begins_with` on the sort key), the update and
//! condition expressions those paths write (`SET`, `REMOVE`, `ADD`,
//! `if_not_exists`, `attribute_exists`, `attribute_not_exists`, `=`, `<>`,
//! `<`, `>`, `AND`, `OR`, brackets) and transactions that write nothing when any
//! condition fails. Anything it doesn't understand (an operation, a
//! condition, an expression) is recorded, answered with an error, and fails
//! the test when the stand-in is dropped: a test can't pass on a fake laxer
//! than DynamoDB. Reads of chosen keys can be made to fail
//! ([`Store::fail_reads`]), to check that callers refuse.

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Router,
};
use base64::Engine;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Obj = Map<String, Value>;

/// A password account.
#[derive(Clone, Debug, Default)]
pub struct User {
    pub sub: String,
    pub email: String,
    pub password: String,
    /// Cognito's own authenticator code is on (its code is `123456`).
    pub mfa: bool,
}

#[derive(Default)]
pub struct Store {
    tables: Mutex<HashMap<String, Vec<Obj>>>,
    users: Mutex<Vec<User>>,
    /// What a test asked the stand-in that it doesn't understand.
    unsupported: Mutex<Vec<String>>,
    /// `(table, pk)` whose reads answer a server error.
    failing: Mutex<Vec<(String, String)>>,
    /// Every transaction is cancelled (as when another write changed an
    /// item it checks in between).
    cancel_transactions: std::sync::atomic::AtomicBool,
}

impl Store {
    /// Cancels every transaction from now on.
    pub fn cancel_transactions(&self) {
        self.cancel_transactions
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Makes every read of `pk` in `table` fail (a server error).
    pub fn fail_reads(&self, table: &str, pk: &str) {
        self.failing
            .lock()
            .unwrap()
            .push((table.to_string(), pk.to_string()));
    }

    fn read_fails(&self, table: &str, pk: Option<&str>) -> bool {
        let pk = pk.unwrap_or("");
        self.failing
            .lock()
            .unwrap()
            .iter()
            .any(|(t, p)| t == table && p == pk)
    }

    fn unsupported(&self, what: String) -> Response {
        self.unsupported
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(what.clone());
        error("UnsupportedByStandIn", &what)
    }

    /// Puts an item (plain strings, numbers and booleans) into `table`.
    pub fn put(&self, table: &str, item: &[(&str, Value)]) {
        let obj: Obj = item.iter().map(|(k, v)| (k.to_string(), wire(v))).collect();
        let mut t = self
            .tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let rows = t.entry(table.to_string()).or_default();
        rows.retain(|r| !same_key(r, &obj));
        rows.push(obj);
    }

    /// The items of `table` whose `pk` is `pk`, as plain JSON.
    pub fn items(&self, table: &str, pk: &str) -> Vec<Value> {
        self.tables
            .lock()
            .unwrap()
            .get(table)
            .map(|rows| {
                rows.iter()
                    .filter(|r| r.get("pk").and_then(|v| v["S"].as_str()) == Some(pk))
                    .map(plain)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// One item by `pk` and `sk`, as plain JSON.
    pub fn get(&self, table: &str, pk: &str, sk: &str) -> Option<Value> {
        self.items(table, pk)
            .into_iter()
            .find(|i| i["sk"].as_str() == Some(sk))
    }

    pub fn add_user(&self, u: User) {
        self.users
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(u);
    }

    fn user_by_email(&self, email: &str) -> Option<User> {
        self.users
            .lock()
            .unwrap()
            .iter()
            .find(|u| u.email.eq_ignore_ascii_case(email))
            .cloned()
    }
}

/// A plain JSON value in DynamoDB's wire form.
fn wire(v: &Value) -> Value {
    match v {
        Value::String(s) => json!({ "S": s }),
        Value::Number(n) => json!({ "N": n.to_string() }),
        Value::Bool(b) => json!({ "BOOL": b }),
        Value::Array(a) => json!({ "L": a.iter().map(wire).collect::<Vec<_>>() }),
        Value::Object(o) => {
            json!({ "M": o.iter().map(|(k, v)| (k.clone(), wire(v))).collect::<Obj>() })
        }
        Value::Null => json!({ "NULL": true }),
    }
}

/// A wire-form item as plain JSON.
fn plain(o: &Obj) -> Value {
    fn one(v: &Value) -> Value {
        if let Some(s) = v.get("S") {
            return s.clone();
        }
        if let Some(n) = v.get("N").and_then(|n| n.as_str()) {
            return n
                .parse::<i64>()
                .map(Value::from)
                .unwrap_or_else(|_| Value::from(n));
        }
        if let Some(b) = v.get("BOOL") {
            return b.clone();
        }
        if let Some(l) = v.get("L").and_then(|l| l.as_array()) {
            return Value::Array(l.iter().map(one).collect());
        }
        if let Some(m) = v.get("M").and_then(|m| m.as_object()) {
            return Value::Object(m.iter().map(|(k, v)| (k.clone(), one(v))).collect());
        }
        Value::Null
    }
    Value::Object(o.iter().map(|(k, v)| (k.clone(), one(v))).collect())
}

fn key_names(o: &Obj) -> Vec<&'static str> {
    if o.contains_key("pk") {
        vec!["pk", "sk"]
    } else {
        vec!["team_id", "sk"]
    }
}

fn same_key(a: &Obj, b: &Obj) -> bool {
    key_names(b).iter().all(|k| a.get(*k) == b.get(*k))
}

/// The running stand-in.
pub struct Fake {
    pub url: String,
    pub store: Arc<Store>,
}

impl Drop for Fake {
    fn drop(&mut self) {
        let asked = self
            .store
            .unsupported
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if !asked.is_empty() && !std::thread::panicking() {
            panic!("the AWS stand-in was asked what it doesn't support: {asked:?}");
        }
    }
}

pub async fn start() -> Fake {
    let store = Arc::new(Store::default());
    let app = Router::new().fallback(handle).with_state(store.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Fake {
        url: format!("http://{addr}"),
        store,
    }
}

fn error(kind: &str, message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        [("content-type", "application/x-amz-json-1.0")],
        json!({ "__type": kind, "message": message }).to_string(),
    )
        .into_response()
}

fn ok(v: Value) -> Response {
    (
        StatusCode::OK,
        [("content-type", "application/x-amz-json-1.1")],
        v.to_string(),
    )
        .into_response()
}

async fn handle(State(store): State<Arc<Store>>, headers: HeaderMap, body: Bytes) -> Response {
    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let req: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let (service, op) = target.split_once('.').unwrap_or(("", ""));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match service {
        "DynamoDB_20120810" => dynamo(&store, op, &req),
        "AWSCognitoIdentityProviderService" => cognito(&store, op, &req),
        "TrentService" => kms(&store, op, &req),
        // Services without a JSON target (SES's REST API: the emails a flow
        // sends) are accepted and not checked.
        "" => ok(json!({ "MessageId": "stand-in" })),
        other => store.unsupported(format!("service {other}")),
    }));
    match result {
        Ok(r) => r,
        Err(p) => {
            let what = p
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "a panic".into());
            store.unsupported(format!("{target}: {what}"))
        }
    }
}

// ── DynamoDB ────────────────────────────────────────────────────────

struct Ctx<'a> {
    names: &'a Value,
    values: &'a Value,
}

impl Ctx<'_> {
    fn name(&self, n: &str) -> String {
        let n = n.trim();
        if n.starts_with('#') {
            self.names[n].as_str().unwrap_or(n).to_string()
        } else {
            n.to_string()
        }
    }
    fn value(&self, v: &str) -> Value {
        self.values[v.trim()].clone()
    }
}

/// Splits on `sep` outside brackets.
fn split_top(s: &str, sep: &str) -> Vec<String> {
    let mut out = Vec::new();
    let (mut depth, mut start, bytes) = (0i32, 0usize, s.as_bytes());
    let mut i = 0;
    while i < s.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        if depth == 0 && s[i..].starts_with(sep) {
            out.push(s[start..i].to_string());
            i += sep.len();
            start = i;
            continue;
        }
        i += 1;
    }
    out.push(s[start..].to_string());
    out
}

fn num(v: &Value) -> f64 {
    v["N"].as_str().and_then(|n| n.parse().ok()).unwrap_or(0.0)
}

/// A condition expression against an item (absent: an empty one).
fn holds(expr: &str, item: Option<&Obj>, c: &Ctx) -> bool {
    let e = expr.trim();
    if e.is_empty() {
        return true;
    }
    let ors = split_top(e, " OR ");
    if ors.len() > 1 {
        return ors.iter().any(|p| holds(p, item, c));
    }
    let ands = split_top(e, " AND ");
    if ands.len() > 1 {
        return ands.iter().all(|p| holds(p, item, c));
    }
    if e.starts_with('(') && e.ends_with(')') {
        return holds(&e[1..e.len() - 1], item, c);
    }
    let attr = |n: &str| item.and_then(|i| i.get(&c.name(n)).cloned());
    if let Some(n) = e
        .strip_prefix("attribute_exists(")
        .and_then(|r| r.strip_suffix(')'))
    {
        return attr(n).is_some();
    }
    if let Some(n) = e
        .strip_prefix("attribute_not_exists(")
        .and_then(|r| r.strip_suffix(')'))
    {
        return attr(n).is_none();
    }
    for op in ["<>", ">", "<", "="] {
        if let Some((l, r)) = e.split_once(op) {
            let (l, r) = (l.trim(), r.trim());
            let plain_name = |n: &str| {
                !n.is_empty()
                    && n.chars()
                        .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '#')
            };
            if !plain_name(l) || !r.starts_with(':') || c.values.get(r).is_none() {
                panic!("unsupported condition term {e:?}");
            }
            let (have, want) = (attr(l), c.value(r));
            return match op {
                "<>" => have.as_ref() != Some(&want),
                "<" => have.is_some_and(|h| num(&h) < num(&want)),
                ">" => have.is_some_and(|h| num(&h) > num(&want)),
                _ => have.as_ref() == Some(&want),
            };
        }
    }
    panic!("unsupported condition term {e:?}")
}

/// Applies an update expression to an item.
fn update(item: &mut Obj, expr: &str, c: &Ctx) {
    let mut clauses: Vec<(String, String)> = Vec::new();
    let mut rest = expr.trim().to_string();
    let kw = ["SET ", "REMOVE ", "ADD ", "DELETE "];
    while !rest.is_empty() {
        let Some(k) = kw.iter().find(|k| rest.starts_with(**k)) else {
            panic!("unsupported update expression {expr:?}");
        };
        let body = &rest[k.len()..];
        let next = kw
            .iter()
            .filter_map(|n| body.find(&format!(" {}", n)))
            .min()
            .unwrap_or(body.len());
        clauses.push((k.trim().to_string(), body[..next].trim().to_string()));
        rest = body[next..].trim().to_string();
    }
    for (k, body) in clauses {
        for part in split_top(&body, ",") {
            let part = part.trim();
            match k.as_str() {
                "SET" => {
                    let Some((l, r)) = part.split_once('=') else {
                        continue;
                    };
                    let name = c.name(l);
                    let r = r.trim();
                    let v = if let Some(args) = r
                        .strip_prefix("if_not_exists(")
                        .and_then(|a| a.strip_suffix(')'))
                    {
                        let (a, b) = args.split_once(',').unwrap_or((args, ""));
                        item.get(&c.name(a)).cloned().unwrap_or_else(|| c.value(b))
                    } else if r.starts_with(':') && c.values.get(r).is_some() {
                        c.value(r)
                    } else {
                        panic!("unsupported update value {r:?}");
                    };
                    item.insert(name, v);
                }
                "REMOVE" => {
                    item.remove(&c.name(part));
                }
                "ADD" => {
                    let (l, r) = part.split_once(' ').unwrap_or((part, ""));
                    let name = c.name(l);
                    let have = item.get(&name).map(num).unwrap_or(0.0);
                    let add = num(&c.value(r));
                    item.insert(name, json!({ "N": ((have + add) as i64).to_string() }));
                }
                other => panic!("unsupported update clause {other}"),
            }
        }
    }
}

fn find<'a>(rows: &'a mut [Obj], key: &Obj) -> Option<&'a mut Obj> {
    rows.iter_mut().find(|r| same_key(r, key))
}

/// One write of a transaction (or on its own): whether its condition holds.
fn write_holds(rows: &[Obj], w: &Value, kind: &str) -> bool {
    let key: Obj = match kind {
        "Put" => w["Item"].as_object().cloned().unwrap_or_default(),
        "Update" | "Delete" | "ConditionCheck" => w["Key"].as_object().cloned().unwrap_or_default(),
        other => panic!("unsupported write {other}"),
    };
    let c = Ctx {
        names: &w["ExpressionAttributeNames"],
        values: &w["ExpressionAttributeValues"],
    };
    let current = rows.iter().find(|r| same_key(r, &key));
    holds(w["ConditionExpression"].as_str().unwrap_or(""), current, &c)
}

fn apply(rows: &mut Vec<Obj>, w: &Value, kind: &str) -> Obj {
    let c = Ctx {
        names: &w["ExpressionAttributeNames"],
        values: &w["ExpressionAttributeValues"],
    };
    match kind {
        "Put" => {
            let item = w["Item"].as_object().cloned().unwrap_or_default();
            rows.retain(|r| !same_key(r, &item));
            rows.push(item.clone());
            item
        }
        "Delete" => {
            let key = w["Key"].as_object().cloned().unwrap_or_default();
            rows.retain(|r| !same_key(r, &key));
            Obj::new()
        }
        "Update" => {
            let key = w["Key"].as_object().cloned().unwrap_or_default();
            if find(rows, &key).is_none() {
                rows.push(key.clone());
            }
            let item = find(rows, &key).unwrap();
            update(item, w["UpdateExpression"].as_str().unwrap_or(""), &c);
            item.clone()
        }
        "ConditionCheck" => Obj::new(),
        other => panic!("unsupported write {other}"),
    }
}

/// Only the attributes a projection names (DynamoDB returns nothing else).
fn project(item: &Obj, req: &Value) -> Obj {
    let Some(p) = req["ProjectionExpression"].as_str() else {
        return item.clone();
    };
    let c = Ctx {
        names: &req["ExpressionAttributeNames"],
        values: &Value::Null,
    };
    let keep: Vec<String> = p.split(',').map(|n| c.name(n)).collect();
    item.iter()
        .filter(|(k, _)| keep.contains(k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// DynamoDB refuses an expression that uses a `#name` or `:value` it wasn't
/// given, and a request that gives one no expression uses: so does this.
fn placeholders_match(w: &Value) {
    let mut used_names = std::collections::BTreeSet::new();
    let mut used_values = std::collections::BTreeSet::new();
    for key in [
        "ConditionExpression",
        "UpdateExpression",
        "KeyConditionExpression",
        "FilterExpression",
        "ProjectionExpression",
    ] {
        let Some(e) = w[key].as_str() else { continue };
        let b = e.as_bytes();
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'#' || b[i] == b':' {
                let start = i;
                i += 1;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                    i += 1;
                }
                let tok = e[start..i].to_string();
                if b[start] == b'#' {
                    used_names.insert(tok);
                } else {
                    used_values.insert(tok);
                }
                continue;
            }
            i += 1;
        }
    }
    let given = |k: &str| -> std::collections::BTreeSet<String> {
        w[k].as_object()
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default()
    };
    let (names, values) = (
        given("ExpressionAttributeNames"),
        given("ExpressionAttributeValues"),
    );
    if names != used_names {
        panic!("expression names don't match: given {names:?}, used {used_names:?}");
    }
    if values != used_values {
        panic!("expression values don't match: given {values:?}, used {used_values:?}");
    }
}

fn dynamo(store: &Store, op: &str, req: &Value) -> Response {
    let table = req["TableName"].as_str().unwrap_or("").to_string();
    if op == "TransactWriteItems" {
        for t in req["TransactItems"].as_array().into_iter().flatten() {
            if let Some((_, w)) = t.as_object().and_then(|o| o.iter().next()) {
                placeholders_match(w);
            }
        }
    } else {
        placeholders_match(req);
    }
    let mut tables = store
        .tables
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match op {
        "GetItem" => {
            let key = req["Key"].as_object().cloned().unwrap_or_default();
            let pk = key
                .get("pk")
                .and_then(|v| v["S"].as_str())
                .map(str::to_string);
            if store.read_fails(&table, pk.as_deref()) {
                return internal_error();
            }
            let rows = tables.entry(table).or_default();
            match rows.iter().find(|r| same_key(r, &key)) {
                Some(i) => ok(json!({ "Item": project(i, req) })),
                None => ok(json!({})),
            }
        }
        "PutItem" | "DeleteItem" | "UpdateItem" => {
            let kind = &op[..op.len() - 4];
            let rows = tables.entry(table).or_default();
            if !write_holds(rows, req, kind) {
                return error(
                    "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
                    "The conditional request failed",
                );
            }
            let item = apply(rows, req, kind);
            if req["ReturnValues"].as_str().is_some_and(|v| v != "NONE") {
                ok(json!({ "Attributes": item }))
            } else {
                ok(json!({}))
            }
        }
        "TransactWriteItems" => {
            let writes: Vec<(String, String, Value)> = req["TransactItems"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|t| {
                    let (kind, w) = t.as_object()?.iter().next()?;
                    Some((
                        w["TableName"].as_str().unwrap_or("").to_string(),
                        kind.clone(),
                        w.clone(),
                    ))
                })
                .collect();
            // DynamoDB refuses a transaction that touches one item twice.
            let mut seen: Vec<(String, Obj)> = Vec::new();
            for (t, kind, w) in &writes {
                let key: Obj = if kind == "Put" {
                    let item = w["Item"].as_object().cloned().unwrap_or_default();
                    key_names(&item)
                        .iter()
                        .filter_map(|k| item.get(*k).map(|v| (k.to_string(), v.clone())))
                        .collect()
                } else {
                    w["Key"].as_object().cloned().unwrap_or_default()
                };
                if seen.iter().any(|(st, sk)| st == t && sk == &key) {
                    return error(
                        "com.amazonaws.dynamodb.v20120810#ValidationException",
                        "Transaction request cannot include multiple operations on one item",
                    );
                }
                seen.push((t.clone(), key));
            }
            let failed = store
                .cancel_transactions
                .load(std::sync::atomic::Ordering::SeqCst)
                || writes.iter().any(|(t, kind, w)| {
                    let rows = tables.get(t).cloned().unwrap_or_default();
                    !write_holds(&rows, w, kind)
                });
            if failed {
                return error(
                    "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
                    "Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]",
                );
            }
            for (t, kind, w) in &writes {
                apply(tables.entry(t.clone()).or_default(), w, kind);
            }
            ok(json!({}))
        }
        "Query" => {
            let c = Ctx {
                names: &req["ExpressionAttributeNames"],
                values: &req["ExpressionAttributeValues"],
            };
            let cond = req["KeyConditionExpression"].as_str().unwrap_or("");
            let parts = split_top(cond, " AND ");
            let first_value = parts
                .first()
                .and_then(|p| p.split_once('='))
                .map(|(_, v)| c.value(v))
                .and_then(|v| v["S"].as_str().map(str::to_string));
            if req["IndexName"].is_null() && store.read_fails(&table, first_value.as_deref()) {
                return internal_error();
            }
            let filter = req["FilterExpression"].as_str().map(str::to_string);
            let mut sort = "sk".to_string();
            let mut rows: Vec<Obj> = tables
                .entry(table)
                .or_default()
                .iter()
                .filter(|r| {
                    parts.iter().all(|p| {
                        let p = p.trim();
                        if let Some(args) = p
                            .strip_prefix("begins_with(")
                            .and_then(|a| a.strip_suffix(')'))
                        {
                            let (a, b) = args.split_once(',').unwrap_or((args, ""));
                            let have = r.get(&c.name(a)).and_then(|v| v["S"].as_str());
                            let want = c.value(b);
                            return have
                                .is_some_and(|h| h.starts_with(want["S"].as_str().unwrap_or("")));
                        }
                        holds(p, Some(r), &c)
                    }) && filter.as_deref().is_none_or(|f| holds(f, Some(r), &c))
                })
                .map(|r| project(r, req))
                .collect();
            if parts.len() > 1 {
                let second = parts[1].trim();
                let name = second
                    .strip_prefix("begins_with(")
                    .and_then(|a| a.split_once(','))
                    .map(|(a, _)| a)
                    .unwrap_or_else(|| second.split('=').next().unwrap_or("sk"));
                sort = c.name(name);
            } else if let Some(idx) = req["IndexName"].as_str() {
                sort = format!("{idx}sk");
            }
            rows.sort_by(|a, b| {
                let k = |o: &Obj| {
                    o.get(&sort)
                        .and_then(|v| v["S"].as_str().map(str::to_string))
                };
                k(a).cmp(&k(b))
            });
            if req["ScanIndexForward"] == json!(false) {
                rows.reverse();
            }
            if let Some(n) = req["Limit"].as_u64() {
                rows.truncate(n as usize);
            }
            ok(json!({ "Items": rows, "Count": rows.len(), "ScannedCount": rows.len() }))
        }
        other => {
            drop(tables);
            store.unsupported(format!("DynamoDB {other}"))
        }
    }
}

fn internal_error() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [("content-type", "application/x-amz-json-1.0")],
        json!({ "__type": "com.amazonaws.dynamodb.v20120810#InternalServerError", "message": "stand-in read failure" }).to_string(),
    )
        .into_response()
}

// ── Cognito ─────────────────────────────────────────────────────────

const MFA_CODE: &str = "123456";

fn auth_result(u: &User) -> Value {
    json!({
        "AuthenticationResult": {
            "AccessToken": format!("at-{}", u.email),
            "IdToken": "id",
            "RefreshToken": "rt",
            "ExpiresIn": 3600,
            "TokenType": "Bearer",
        },
        "ChallengeParameters": {},
    })
}

fn attrs(u: &User) -> Value {
    json!([
        { "Name": "email", "Value": u.email },
        { "Name": "sub", "Value": u.sub },
    ])
}

fn cognito(store: &Store, op: &str, req: &Value) -> Response {
    let not_found = || error("UserNotFoundException", "User does not exist.");
    match op {
        "InitiateAuth" => {
            let email = req["AuthParameters"]["USERNAME"].as_str().unwrap_or("");
            let pw = req["AuthParameters"]["PASSWORD"].as_str().unwrap_or("");
            let Some(u) = store.user_by_email(email) else {
                return error("NotAuthorizedException", "Incorrect username or password.");
            };
            if u.password != pw {
                return error("NotAuthorizedException", "Incorrect username or password.");
            }
            if u.mfa {
                return ok(json!({
                    "ChallengeName": "SOFTWARE_TOKEN_MFA",
                    "Session": format!("session-{}", u.email),
                    "ChallengeParameters": {},
                }));
            }
            ok(auth_result(&u))
        }
        "RespondToAuthChallenge" => {
            let email = req["ChallengeResponses"]["USERNAME"].as_str().unwrap_or("");
            let code = req["ChallengeResponses"]["SOFTWARE_TOKEN_MFA_CODE"]
                .as_str()
                .unwrap_or("");
            match store.user_by_email(email) {
                Some(u) if code == MFA_CODE => ok(auth_result(&u)),
                Some(_) => error("CodeMismatchException", "Invalid code."),
                None => not_found(),
            }
        }
        "GetUser" => {
            let token = req["AccessToken"].as_str().unwrap_or("");
            match token
                .strip_prefix("at-")
                .and_then(|e| store.user_by_email(e))
            {
                Some(u) => ok(json!({ "Username": u.sub, "UserAttributes": attrs(&u) })),
                None => error("NotAuthorizedException", "Invalid access token."),
            }
        }
        "AdminGetUser" => {
            let email = req["Username"].as_str().unwrap_or("");
            match store.user_by_email(email) {
                Some(u) => ok(json!({
                    "Username": u.sub,
                    "UserAttributes": attrs(&u),
                    "UserMFASettingList": if u.mfa { json!(["SOFTWARE_TOKEN_MFA"]) } else { json!([]) },
                    "Enabled": true,
                    "UserStatus": "CONFIRMED",
                })),
                None => not_found(),
            }
        }
        "ListUsers" => {
            let filter = req["Filter"].as_str().unwrap_or("");
            let email = filter.split('"').nth(1).unwrap_or("").to_string();
            let users: Vec<Value> = store
                .user_by_email(&email)
                .map(|u| json!({ "Username": u.sub, "Attributes": attrs(&u) }))
                .into_iter()
                .collect();
            ok(json!({ "Users": users }))
        }
        other => store.unsupported(format!("Cognito {other}")),
    }
}

// ── KMS ─────────────────────────────────────────────────────────────

/// Data keys and blobs pass through as they are: enough for sealing and
/// opening to round-trip in a test.
fn kms(store: &Store, op: &str, req: &Value) -> Response {
    let b64 = |b: &[u8]| base64::engine::general_purpose::STANDARD.encode(b);
    match op {
        "GenerateDataKey" => {
            let key = b64(&[7u8; 32]);
            ok(json!({ "KeyId": "fake", "Plaintext": key, "CiphertextBlob": key }))
        }
        "Decrypt" => ok(json!({ "KeyId": "fake", "Plaintext": req["CiphertextBlob"] })),
        "Encrypt" => ok(json!({ "KeyId": "fake", "CiphertextBlob": req["Plaintext"] })),
        other => store.unsupported(format!("KMS {other}")),
    }
}

/// The gateway's state with every AWS client pointed at the stand-in.
pub fn state(url: &str) -> Arc<crate::AppState> {
    use aws_sdk_dynamodb::config::{Credentials, SharedCredentialsProvider};
    let aws = aws_config::SdkConfig::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(url)
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            "AKIDTEST", "secret", None, None, "test",
        )))
        .build();
    let secrets: crate::models::Secrets = serde_json::from_value(json!({
        "jwt_secret": "test-jwt-secret-for-the-okta-rule",
        // The deployment's own Entra app (no request reaches Microsoft:
        // the tests stop at the redirect or start after the token).
        "microsoft_client_id": "ms-client",
        "microsoft_client_secret": "ms-secret",
    }))
    .unwrap();
    Arc::new(crate::AppState {
        dynamo: aws_sdk_dynamodb::Client::new(&aws),
        sqs: aws_sdk_sqs::Client::new(&aws),
        ses: aws_sdk_sesv2::Client::new(&aws),
        s3: aws_sdk_s3::Client::new(&aws),
        cognito: aws_sdk_cognitoidentityprovider::Client::new(&aws),
        lambda: aws_sdk_lambda::Client::new(&aws),
        kms: aws_sdk_kms::Client::new(&aws),
        sts: aws_sdk_sts::Client::new(&aws),
        bedrock: aws_sdk_bedrockruntime::Client::new(&aws),
        scheduler: common::plugins::schedule::aws_sdk_scheduler::Client::new(&aws),
        http: reqwest::Client::new(),
        secrets,
        config: crate::models::Config {
            table_name: "main".into(),
            teams_table_name: "teams".into(),
            users_table_name: "users".into(),
            settings_table_name: "settings".into(),
            repos_table_name: "repos".into(),
            cognito_user_pool_id: "pool".into(),
            cognito_client_id: "client".into(),
            secrets_key_id: "key".into(),
            ..Default::default()
        },
        cognito_client_secret: "cognito-secret".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_stand_in_keeps_items_and_checks_conditions() {
        let f = start().await;
        let s = state(&f.url);
        use aws_sdk_dynamodb::types::AttributeValue as A;
        s.dynamo
            .put_item()
            .table_name("users")
            .item("pk", A::S("T".into()))
            .item("sk", A::S("USER#1".into()))
            .item("role", A::S("owner".into()))
            .send()
            .await
            .unwrap();
        // A second put on the same key under attribute_not_exists fails.
        let again = s
            .dynamo
            .put_item()
            .table_name("users")
            .item("pk", A::S("T".into()))
            .item("sk", A::S("USER#1".into()))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;
        assert!(again.is_err());
        s.dynamo
            .update_item()
            .table_name("users")
            .key("pk", A::S("T".into()))
            .key("sk", A::S("USER#1".into()))
            .update_expression("SET #r = :r REMOVE gone ADD n :one")
            .condition_expression("attribute_exists(pk)")
            .expression_attribute_names("#r", "role")
            .expression_attribute_values(":r", A::S("admin".into()))
            .expression_attribute_values(":one", A::N("1".into()))
            .send()
            .await
            .unwrap();
        let got = f.store.get("users", "T", "USER#1").unwrap();
        assert_eq!(got["role"], "admin");
        assert_eq!(got["n"], 1);
        let q = s
            .dynamo
            .query()
            .table_name("users")
            .key_condition_expression("pk = :pk AND begins_with(sk, :u)")
            .expression_attribute_values(":pk", A::S("T".into()))
            .expression_attribute_values(":u", A::S("USER#".into()))
            .send()
            .await
            .unwrap();
        assert_eq!(q.count(), 1);
    }
}
