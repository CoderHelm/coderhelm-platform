//! Rule 6: end-to-end tests that read CMS content, and a schema change that
//! makes the staging content out of date.
//!
//! A repository serves pages from its CMS through a catch-all route
//! ([`is_cms_catch_all`]); its end-to-end tests open those pages on staging.
//! When the change edits the CMS schema, the content they read can go out of
//! date: a field the schema drops is still on the documents, a field it now
//! requires is missing, a new block the new test looks for is on no page.
//! [`needs`] lists that content from the diff. Then, before the pull
//! request:
//!
//! - with the repository's staging CMS write connection (Settings, per
//!   repository; staging only, see `common::staging_cms`), the changes that
//!   can be made without inventing content are made on staging: a dropped
//!   field is removed from the documents that hold it, a newly required field
//!   with a default gets the default. The description says exactly what
//!   changed;
//! - anything else (no connection, a required field with no default, a page
//!   that must show the new block) stops the run before the pull request,
//!   naming exactly which content must change;
//! - before a note tells editors to add a block, staging is looked up for
//!   blocks of that type already there ([`instance_queries`]): one in an
//!   older shape is to be replaced or updated, by its document and key, with
//!   the fields to set and to remove and the query that confirms it
//!   ([`block_line`]). Without a write connection, the lookup goes through
//!   the CMS's public read API when the repository names its project and
//!   dataset (`outside_work::content_source`); only when neither can be read
//!   does the note say it couldn't look, and why.
//! - every line that stops the run is a [`Blocked`]: what must change, why
//!   CoderHelm couldn't do it (the missing connection, by name, or why it is
//!   an editor's choice) and how to grant it so it can next time.

use std::collections::BTreeSet;

use common::staging_cms::StagingCms;
use serde_json::{json, Value};
use tracing::warn;

/// Words in a route file that mark a CMS client.
const CMS_MARKS: &[&str] = &[
    "sanity",
    "groq",
    "contentful",
    "storyblok",
    "prismic",
    "datocms",
    "hygraph",
    "payload",
    "strapi",
    "builder.io",
    "graphcms",
    "cms",
];

/// Whether a route path is a catch-all (`[...slug]`, `[[...slug]]`).
pub fn is_catch_all_path(path: &str) -> bool {
    path.contains("[...") || path.contains("[[...")
}

/// A catch-all route file whose code reads a CMS.
pub fn is_cms_catch_all(path: &str, text: &str) -> bool {
    if !is_catch_all_path(path) {
        return false;
    }
    let l = text.to_lowercase();
    CMS_MARKS.iter().any(|m| l.contains(m))
}

/// Whether a path is an end-to-end spec.
pub fn is_e2e_spec(path: &str) -> bool {
    let l = path.to_lowercase();
    (l.contains(".spec.") || l.contains(".e2e.") || l.contains(".cy."))
        && (l.contains("e2e")
            || l.contains("playwright")
            || l.contains("cypress")
            || l.contains("tests/"))
}

/// Whether a file is a CMS schema definition.
pub fn is_schema_file(path: &str, text: &str) -> bool {
    let l = path.to_lowercase();
    let pathy = l.contains("schema")
        || l.contains("studio")
        || l.contains("/cms/")
        || l.starts_with("cms/");
    pathy
        && (text.contains("defineType")
            || text.contains("defineField")
            || text.contains("createContentType")
            || (text.contains("fields:") && text.contains("type:")))
}

/// The `name: 'x'` a line declares, if any (anywhere on the line, not
/// `displayName:` or the like).
pub fn declared_name(line: &str) -> Option<String> {
    let mut from = 0;
    while let Some(i) = line[from..].find("name:") {
        let at = from + i;
        from = at + 5;
        let before = line[..at].chars().next_back();
        if before.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_') {
            continue;
        }
        let rest = line[at + 5..].trim_start();
        let Some(q) = rest
            .chars()
            .next()
            .filter(|c| matches!(c, '\'' | '"' | '`'))
        else {
            continue;
        };
        let inner = &rest[1..];
        let end = inner.find(q)?;
        let n = &inner[..end];
        if !n.is_empty() {
            return Some(n.to_string());
        }
    }
    None
}

/// The type a schema file defines: the first name it declares.
pub fn type_name(text: &str) -> String {
    text.lines().find_map(declared_name).unwrap_or_default()
}

/// What a schema diff changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaChange {
    /// A new type (a document or a block).
    NewType(String),
    /// A field taken off a type.
    RemovedField { doc_type: String, field: String },
    /// A field a type now requires, with its default when the schema gives
    /// one (`initialValue`).
    RequiredField {
        doc_type: String,
        field: String,
        default: Option<Value>,
    },
}

/// The schema changes one file's diff makes. `type_name` is the file's type
/// (its first declared name on the branch).
pub fn schema_changes(status: &str, patch: &str, type_name: &str) -> Vec<SchemaChange> {
    let mut out = Vec::new();
    if status == "added" {
        if !type_name.is_empty() {
            out.push(SchemaChange::NewType(type_name.to_string()));
        }
        return out;
    }
    let added: BTreeSet<String> = patch
        .lines()
        .filter(|l| l.starts_with('+') && !l.starts_with("+++"))
        .filter_map(declared_name)
        .collect();
    let mut last_name = String::new();
    let mut last_default: Option<Value> = None;
    for l in patch.lines() {
        if l.starts_with("+++") || l.starts_with("---") {
            continue;
        }
        if let Some(n) = declared_name(l) {
            if l.starts_with('-') && !added.contains(&n) && n != type_name {
                out.push(SchemaChange::RemovedField {
                    doc_type: type_name.to_string(),
                    field: n.clone(),
                });
            }
            if !l.starts_with('-') {
                last_name = n;
                last_default = None;
            }
        }
        let body = l.get(1..).unwrap_or("").trim();
        if !l.starts_with('-') {
            if let Some(v) = body.strip_prefix("initialValue:") {
                let v = v.trim().trim_end_matches(',');
                last_default = serde_json::from_str(&v.replace('\'', "\"")).ok();
            }
        }
        if l.starts_with('+')
            && body.contains("required()")
            && !last_name.is_empty()
            && last_name != type_name
        {
            out.push(SchemaChange::RequiredField {
                doc_type: type_name.to_string(),
                field: last_name.clone(),
                default: last_default.clone(),
            });
        }
    }
    out.dedup();
    out
}

/// Content staging must have for the tests to read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Need {
    /// Remove a dropped field from every document of a type that holds it.
    Unset { doc_type: String, field: String },
    /// Every document of a type needs a field. `default`: set it to this.
    Require {
        doc_type: String,
        field: String,
        default: Option<Value>,
    },
    /// A staging page must show a block of the new type, for a spec.
    BlockOnPage { block: String, spec: String },
}

impl Need {
    pub fn line(&self) -> String {
        match self {
            Need::Unset { doc_type, field } => {
                format!("remove `{field}` from every `{doc_type}` document that still has it")
            }
            Need::Require {
                doc_type,
                field,
                default,
            } => match default {
                Some(v) => {
                    format!("set `{field}` to `{v}` on every `{doc_type}` document without it")
                }
                None => format!(
                    "give every `{doc_type}` document a value for the now required `{field}`"
                ),
            },
            Need::BlockOnPage { block, spec } => format!(
                "add a `{block}` block to the staging page `{spec}` opens, so the new test finds it"
            ),
        }
    }

    /// Whether CoderHelm can make it without inventing content.
    pub fn automatic(&self) -> bool {
        match self {
            Need::Unset { .. } => true,
            Need::Require { default, .. } => default.is_some(),
            Need::BlockOnPage { .. } => false,
        }
    }
}

/// The staging content the change needs, given its schema changes and the
/// end-to-end specs it adds or changes as (path, text). A new type counts
/// only when a changed spec names it.
pub fn needs(changes: &[SchemaChange], specs: &[(String, String)]) -> Vec<Need> {
    let mut out = Vec::new();
    for c in changes {
        let n = match c {
            SchemaChange::RemovedField { doc_type, field } => Need::Unset {
                doc_type: doc_type.clone(),
                field: field.clone(),
            },
            SchemaChange::RequiredField {
                doc_type,
                field,
                default,
            } => Need::Require {
                doc_type: doc_type.clone(),
                field: field.clone(),
                default: default.clone(),
            },
            SchemaChange::NewType(t) => {
                let Some((spec, _)) = specs
                    .iter()
                    .find(|(_, text)| super::registration::names_item(text, t))
                else {
                    continue;
                };
                Need::BlockOnPage {
                    block: t.clone(),
                    spec: spec.clone(),
                }
            }
        };
        if !out.contains(&n) {
            out.push(n);
        }
    }
    out
}

/// What a new block's schema declares: its fields, the required ones, and
/// where documents hold it (`(document type, array field)`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BlockShape {
    pub fields: Vec<String>,
    pub required: Vec<String>,
    pub held_in: Vec<(String, String)>,
    /// The fewest items the spec needs on the page to check anything
    /// ([`min_items`]).
    pub min_items: Option<usize>,
}

/// The fewest items a spec's assertions need: `toBeGreaterThan(N)` needs
/// N + 1, `toBeGreaterThanOrEqual(N)` and `toHaveCount(N)` need N, `.nth(N)`
/// needs N + 1. The largest of them, or `None` when the spec has none.
pub fn min_items(spec: &str) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (pat, extra) in [
        ("toBeGreaterThanOrEqual(", 0),
        ("toBeGreaterThan(", 1),
        ("toHaveCount(", 0),
        (".nth(", 1),
    ] {
        let mut from = 0;
        while let Some(i) = spec[from..].find(pat) {
            let at = from + i + pat.len();
            from = at;
            let digits: String = spec[at..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            if let Ok(n) = digits.parse::<usize>() {
                let need = n + extra;
                if need > 1 {
                    best = Some(best.map_or(need, |b| b.max(need)));
                }
            }
        }
    }
    best
}

/// The fields a schema file declares after its type's own name, and which
/// of them are required.
pub fn fields_of(text: &str) -> (Vec<String>, Vec<String>) {
    let ty = type_name(text);
    let mut fields: Vec<String> = Vec::new();
    let mut required: Vec<String> = Vec::new();
    let mut last = String::new();
    for l in text.lines() {
        if let Some(n) = declared_name(l) {
            if n != ty && !fields.contains(&n) {
                fields.push(n.clone());
            }
            last = n;
        }
        if l.contains("required()") && !last.is_empty() && last != ty && !required.contains(&last) {
            required.push(last.clone());
        }
    }
    (fields, required)
}

/// Where documents hold blocks of `block`: for each schema file whose array
/// field lists `{ type: 'block' }`, that file's type and the field.
pub fn held_in(block: &str, schemas: &[(String, String)]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (_, text) in schemas {
        let lines: Vec<&str> = text.lines().collect();
        for (i, l) in lines.iter().enumerate() {
            let member = [format!("type: '{block}'"), format!("type: \"{block}\"")]
                .iter()
                .any(|m| l.contains(m.as_str()));
            if !member || declared_name(l).as_deref() == Some(block) {
                continue;
            }
            let Some(field) = lines[..i].iter().rev().find_map(|x| declared_name(x)) else {
                continue;
            };
            let doc = type_name(text);
            if !doc.is_empty() && field != doc && !out.contains(&(doc.clone(), field.clone())) {
                out.push((doc, field));
            }
        }
    }
    out
}

/// The read-only queries that find blocks of `block` already on staging:
/// inside each document field that holds it, and as documents of its own
/// type.
pub fn instance_queries(block: &str, shape: &BlockShape) -> Vec<String> {
    let Some(b) = groq_field(block) else {
        return Vec::new();
    };
    let mut out: Vec<String> = shape
        .held_in
        .iter()
        .filter_map(|(doc, field)| {
            let f = groq_field(field)?;
            Some(format!(
                "*[_type == {} && count({f}[_type == {}]) > 0][0...5]{{_id, \"field\": {}, \"items\": {f}[_type == {}]}}",
                groq_str(doc),
                groq_str(b),
                groq_str(f),
                groq_str(b)
            ))
        })
        .collect();
    out.push(format!(
        "*[_type == {}][0...5]{{_id, \"items\": [@]}}",
        groq_str(b)
    ));
    out
}

/// A block already on staging: its document, the array field and key that
/// hold it (empty for a document of its own), and the fields it has.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Existing {
    pub doc_id: String,
    pub field: String,
    pub key: String,
    pub fields: Vec<String>,
}

/// The blocks an [`instance_queries`] result lists.
pub fn parse_instances(result: &Value) -> Vec<Existing> {
    let mut out = Vec::new();
    for d in result.as_array().into_iter().flatten() {
        let doc_id = d["_id"].as_str().unwrap_or("").to_string();
        let field = d["field"].as_str().unwrap_or("").to_string();
        for item in d["items"].as_array().into_iter().flatten() {
            let mut fields: Vec<String> = item
                .as_object()
                .into_iter()
                .flatten()
                .map(|(k, _)| k.clone())
                .filter(|k| !k.starts_with('_'))
                .collect();
            fields.sort();
            out.push(Existing {
                doc_id: doc_id.clone(),
                field: field.clone(),
                key: item["_key"].as_str().unwrap_or("").to_string(),
                fields,
            });
        }
    }
    out
}

/// The note for a block a spec needs, after staging was looked up: replace
/// or update the block that is there (fields to set and to remove, and the
/// query that confirms it), or add one when none is.
pub fn block_line(block: &str, spec: &str, shape: &BlockShape, found: &[Existing]) -> String {
    let least = shape
        .min_items
        .map(|n| {
            format!(
                ", with at least {n} items in it: `{spec}` checks that many, so fewer leave it \
                 nothing to check"
            )
        })
        .unwrap_or_default();
    let Some(e) = found.first() else {
        return format!(
            "add a `{block}` block to the staging page `{spec}` opens, so the new test finds \
             it{least} (looked up on staging: no `{block}` block exists yet). Set {}",
            if shape.fields.is_empty() {
                "its required fields".to_string()
            } else {
                shape
                    .fields
                    .iter()
                    .map(|f| format!("`{f}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        );
    };
    let set: Vec<String> = shape
        .required
        .iter()
        .chain(shape.fields.iter())
        .filter(|f| !e.fields.contains(f))
        .fold(Vec::new(), |mut acc, f| {
            if !acc.contains(f) {
                acc.push(f.clone());
            }
            acc
        });
    let remove: Vec<String> = e
        .fields
        .iter()
        .filter(|f| !shape.fields.is_empty() && !shape.fields.contains(f))
        .cloned()
        .collect();
    let (target, verify) = if e.key.is_empty() || e.field.is_empty() {
        (
            format!("the `{block}` document `{}`", e.doc_id),
            format!("*[_id == {}][0]", groq_str(&e.doc_id)),
        )
    } else {
        (
            format!(
                "the `{block}` block in document `{}` (field `{}`, key `{}`)",
                e.doc_id, e.field, e.key
            ),
            format!(
                "*[_id == {}][0].{}[_key == {}][0]",
                groq_str(&e.doc_id),
                e.field,
                groq_str(&e.key)
            ),
        )
    };
    let list = |v: &[String]| {
        if v.is_empty() {
            "nothing".to_string()
        } else {
            v.iter()
                .map(|f| format!("`{f}`"))
                .collect::<Vec<_>>()
                .join(", ")
        }
    };
    let more = if found.len() > 1 {
        format!(" ({} more like it on staging)", found.len() - 1)
    } else {
        String::new()
    };
    format!(
        "staging already has {target}{more} in an older shape: replace that one's content, do \
         not add a second{least}. Set {}; remove {}. Confirm with the query `{verify}`: it returns \
         the block with the new fields",
        list(&set),
        list(&remove)
    )
}

fn groq_str(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

fn groq_field(f: &str) -> Option<&str> {
    (!f.is_empty() && f.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')).then_some(f)
}

/// The query for the ids of the documents an automatic need applies to.
pub fn affected_query(n: &Need) -> Option<String> {
    match n {
        Need::Unset { doc_type, field } => Some(format!(
            "*[_type == {} && defined({})]._id",
            groq_str(doc_type),
            groq_field(field)?
        )),
        Need::Require {
            doc_type,
            field,
            default: Some(_),
        } => Some(format!(
            "*[_type == {} && !defined({})]._id",
            groq_str(doc_type),
            groq_field(field)?
        )),
        _ => None,
    }
}

/// The mutations that make an automatic need on `ids`.
pub fn mutations(n: &Need, ids: &[String]) -> Vec<Value> {
    ids.iter()
        .filter_map(|id| match n {
            Need::Unset { field, .. } => Some(json!({"patch": {"id": id, "unset": [field]}})),
            Need::Require {
                field,
                default: Some(v),
                ..
            } => Some(json!({"patch": {"id": id, "setIfMissing": {field: v}}})),
            _ => None,
        })
        .collect()
}

/// Content CoderHelm couldn't change itself: what, why, and how to grant
/// it the access so it can next time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Blocked {
    pub what: String,
    pub why_person: String,
    pub grant: String,
}

impl Blocked {
    pub fn line(&self) -> String {
        format!(
            "{} (why CoderHelm didn't: {}; so it can next time: {})",
            self.what, self.why_person, self.grant
        )
    }
}

/// Why CoderHelm can't change staging content without a connection, and
/// how to give it one.
fn no_connection(repo: &str) -> (String, String) {
    (
        format!("there is no staging CMS write connection for {repo}"),
        format!(
            "add one in Settings, Repositories, {repo}, Staging CMS (a staging-only write token); \
             CoderHelm then removes dropped fields and sets schema defaults on staging itself"
        ),
    )
}

/// Why a need is a person's even with a connection, and what would let
/// CoderHelm do it.
fn editors_choice(n: &Need) -> (String, String) {
    match n {
        Need::Require { field, .. } => (
            format!("the schema gives `{field}` no default, so its value is an editor's choice"),
            format!(
                "give `{field}` an `initialValue` in the schema and CoderHelm sets it on staging"
            ),
        ),
        Need::BlockOnPage { spec, .. } => (
            "which page shows the block and what it says is an editor's choice".to_string(),
            format!(
                "have `{spec}` create its own content in its setup (a fixture or a seeded \
                 document), so the test needs no editor"
            ),
        ),
        Need::Unset { .. } => (String::new(), String::new()),
    }
}

/// What happened to the staging content, for the description.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Outcome {
    /// What was changed on staging, one line each.
    pub changed: Vec<String>,
    /// What must still change: stops the pull request.
    pub blocking: Vec<Blocked>,
}

impl Outcome {
    /// The blocking lines, each with why and how to grant.
    pub fn blocking_lines(&self) -> Vec<String> {
        self.blocking.iter().map(Blocked::line).collect()
    }
}

/// Runs a read-only query on staging and returns its result.
async fn query(
    http: &reqwest::Client,
    c: &StagingCms,
    token: &str,
    q: &str,
) -> Result<Value, String> {
    let r = http
        .get(c.query_url())
        .query(&[("query", q)])
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("staging query failed: {e}"))?;
    if !r.status().is_success() {
        return Err(format!("staging query returned {}", r.status()));
    }
    let v: Value = r.json().await.map_err(|e| format!("staging query: {e}"))?;
    Ok(v["result"].clone())
}

/// Reads the ids an automatic need applies to on staging.
async fn affected(
    http: &reqwest::Client,
    c: &StagingCms,
    token: &str,
    q: &str,
) -> Result<Vec<String>, String> {
    let r = http
        .get(c.query_url())
        .query(&[("query", q)])
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("staging query failed: {e}"))?;
    if !r.status().is_success() {
        return Err(format!("staging query returned {}", r.status()));
    }
    let v: Value = r.json().await.map_err(|e| format!("staging query: {e}"))?;
    Ok(v["result"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|x| x.as_str().map(String::from))
        .collect())
}

/// How staging content is read and written: the repository's write
/// connection, its CMS's public read API, or neither.
#[derive(Clone, Copy)]
pub struct Access<'a> {
    /// `owner/name`, named in what to grant.
    pub repo: &'a str,
    /// The write connection and its opened token.
    pub conn: Option<(&'a StagingCms, &'a str)>,
    /// The public read API the repository's config names.
    pub public: Option<&'a crate::passes::outside_work::ContentSource>,
}

/// Looks up blocks of `block` already there and writes the line for it.
async fn block_need(
    http: &reqwest::Client,
    access: Access<'_>,
    n: &Need,
    block: &str,
    spec: &str,
    shapes: &[(String, BlockShape)],
) -> String {
    let shape = shapes
        .iter()
        .find(|(b, _)| b == block)
        .map(|(_, s)| s.clone())
        .unwrap_or_default();
    let mut found = Vec::new();
    let mut failed = None;
    let mut looked = false;
    for q in instance_queries(block, &shape) {
        let r = match (access.conn, access.public) {
            (Some((c, token)), _) => query(http, c, token, &q).await,
            (None, Some(p)) => crate::passes::outside_work::public_query(http, p, &q).await,
            (None, None) => break,
        };
        looked = true;
        match r {
            Ok(v) => found.extend(parse_instances(&v)),
            Err(e) => failed = Some(e),
        }
    }
    let line = n.line();
    match failed {
        _ if !looked => format!(
            "{line}. I couldn't look up existing content: the repository names no CMS project id \
             and dataset in its config or codegen environment, and it has no staging CMS \
             connection. Check first: if that page already has a `{block}` block, replace that \
             one's content instead of adding a second"
        ),
        Some(e) if found.is_empty() => format!(
            "{line}. The lookup for existing `{block}` blocks failed ({e}): check first, and \
             replace one that is there instead of adding a second"
        ),
        _ => block_line(block, spec, &shape, &found),
    }
}

/// Makes what can be made on staging and lists what can't, each with why
/// CoderHelm couldn't and how to grant it. `production_in_code`: datasets
/// the repository's production settings name, refused like production.
pub async fn settle(
    http: &reqwest::Client,
    access: Access<'_>,
    production_in_code: &[String],
    needs: &[Need],
    shapes: &[(String, BlockShape)],
) -> Outcome {
    let mut out = Outcome::default();
    let Some((c, token)) = access.conn else {
        let (why, grant) = no_connection(access.repo);
        for n in needs {
            let what = match n {
                Need::BlockOnPage { block, spec } => {
                    block_need(http, access, n, block, spec, shapes).await
                }
                _ => n.line(),
            };
            let (why, grant) = match n {
                Need::Unset { .. }
                | Need::Require {
                    default: Some(_), ..
                } => (why.clone(), grant.clone()),
                _ => editors_choice(n),
            };
            out.blocking.push(Blocked {
                what,
                why_person: why,
                grant,
            });
        }
        return out;
    };
    if let Err(e) = c
        .check_writable()
        .and_then(|_| c.check_against_code(production_in_code))
    {
        for n in needs {
            out.blocking.push(Blocked {
                what: n.line(),
                why_person: e.clone(),
                grant: "point the repository's staging CMS connection at a staging dataset".into(),
            });
        }
        return out;
    }
    for n in needs {
        // A block a spec needs: look for one already on staging first, so
        // the note says which to replace instead of asking for a second.
        if let Need::BlockOnPage { block, spec } = n {
            let (why, grant) = editors_choice(n);
            out.blocking.push(Blocked {
                what: block_need(http, access, n, block, spec, shapes).await,
                why_person: why,
                grant,
            });
            continue;
        }
        let Some(q) = n.automatic().then(|| affected_query(n)).flatten() else {
            let (why, grant) = editors_choice(n);
            out.blocking.push(Blocked {
                what: n.line(),
                why_person: why,
                grant,
            });
            continue;
        };
        let ids = match affected(http, c, token, &q).await {
            Ok(ids) => ids,
            Err(e) => {
                warn!(error = %e, "Could not read staging content");
                out.blocking.push(Blocked {
                    what: n.line(),
                    why_person: format!("reading staging failed: {e}"),
                    grant: "check the staging CMS connection's token can read the dataset".into(),
                });
                continue;
            }
        };
        if ids.is_empty() {
            continue;
        }
        let body = json!({ "mutations": mutations(n, &ids) });
        let sent = http
            .post(c.mutate_url())
            .bearer_auth(token)
            .json(&body)
            .send()
            .await;
        match sent {
            Ok(r) if r.status().is_success() => out.changed.push(format!(
                "{} on {} document(s) in the `{}` dataset ({})",
                n.line(),
                ids.len(),
                c.dataset,
                ids.iter().take(5).cloned().collect::<Vec<_>>().join(", ")
            )),
            Ok(r) => out.blocking.push(Blocked {
                what: n.line(),
                why_person: format!("staging refused the change ({})", r.status()),
                grant: "give the staging CMS connection's token write access".into(),
            }),
            Err(e) => out.blocking.push(Blocked {
                what: n.line(),
                why_person: format!("the staging write failed: {e}"),
                grant: "retry; if it keeps failing, check the staging CMS connection".into(),
            }),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_a_catch_all_route_served_by_a_cms() {
        assert!(is_cms_catch_all(
            "app/[...slug]/page.tsx",
            "import { client } from '@/sanity/client'"
        ));
        assert!(is_cms_catch_all(
            "src/pages/[[...path]].tsx",
            "fetchFromCMS()"
        ));
        assert!(!is_cms_catch_all(
            "app/[...slug]/page.tsx",
            "export default function P() {}"
        ));
        assert!(!is_cms_catch_all("app/about/page.tsx", "sanity"));
        assert!(is_e2e_spec("e2e/blocks/quote.spec.ts"));
        assert!(!is_e2e_spec("src/lib/date.test.ts"));
        assert!(is_schema_file(
            "studio/schemas/quote.ts",
            "export default defineType({ name: 'quote' })"
        ));
    }

    #[test]
    fn reads_dropped_and_newly_required_fields_from_the_diff() {
        let patch = "@@\n defineType({\n   name: 'hero',\n   fields: [\n-    defineField({ name: 'subtitle', type: 'string' }),\n     defineField({\n       name: 'theme',\n+      initialValue: 'light',\n+      validation: (Rule) => Rule.required(),\n     }),\n+    defineField({ name: 'cta', type: 'string', validation: (r) => r.required() }),\n";
        let c = schema_changes("modified", patch, "hero");
        assert_eq!(
            c,
            vec![
                SchemaChange::RemovedField {
                    doc_type: "hero".into(),
                    field: "subtitle".into()
                },
                SchemaChange::RequiredField {
                    doc_type: "hero".into(),
                    field: "theme".into(),
                    default: Some(json!("light"))
                },
                SchemaChange::RequiredField {
                    doc_type: "hero".into(),
                    field: "cta".into(),
                    default: None
                },
            ]
        );
        assert_eq!(
            schema_changes("added", "+defineType({ name: 'quote' })", "quote"),
            vec![SchemaChange::NewType("quote".into())]
        );
    }

    #[test]
    fn lists_exactly_which_content_must_change() {
        let changes = vec![
            SchemaChange::RemovedField {
                doc_type: "hero".into(),
                field: "subtitle".into(),
            },
            SchemaChange::RequiredField {
                doc_type: "hero".into(),
                field: "cta".into(),
                default: None,
            },
            SchemaChange::NewType("quoteBlock".into()),
            SchemaChange::NewType("unused".into()),
        ];
        let specs = vec![(
            "e2e/quote.spec.ts".to_string(),
            "await expect(page.getByTestId('quote-block')).toBeVisible()".to_string(),
        )];
        let n = needs(&changes, &specs);
        assert_eq!(n.len(), 3);
        assert!(n[0].automatic());
        assert!(!n[1].automatic());
        assert_eq!(
            n[2],
            Need::BlockOnPage {
                block: "quoteBlock".into(),
                spec: "e2e/quote.spec.ts".into()
            }
        );
        assert_eq!(
            affected_query(&n[0]).unwrap(),
            "*[_type == \"hero\" && defined(subtitle)]._id"
        );
        assert!(affected_query(&n[1]).is_none());
        let m = mutations(&n[0], &["a".into(), "b".into()]);
        assert_eq!(m[0], json!({"patch": {"id": "a", "unset": ["subtitle"]}}));
    }

    /// The observed case: staging already had a block of the new type in an
    /// older shape; the note asked editors to add one.
    #[test]
    fn a_block_already_on_staging_is_replaced_not_added() {
        let block = "export default defineType({\n  name: 'promoBanner',\n  type: 'object',\n  fields: [\n    defineField({ name: 'heading', type: 'string', validation: (r) => r.required() }),\n    defineField({ name: 'cta', type: 'link' }),\n  ],\n})\n";
        let page = "export default defineType({\n  name: 'page',\n  type: 'document',\n  fields: [\n    defineField({\n      name: 'sections',\n      type: 'array',\n      of: [{ type: 'hero' }, { type: 'promoBanner' }],\n    }),\n  ],\n})\n";
        let (fields, required) = fields_of(block);
        assert_eq!(fields, vec!["heading", "cta"]);
        assert_eq!(required, vec!["heading"]);
        let shape = BlockShape {
            fields,
            required,
            min_items: None,
            held_in: held_in(
                "promoBanner",
                &[
                    ("schemas/promoBanner.ts".into(), block.into()),
                    ("schemas/page.ts".into(), page.into()),
                ],
            ),
        };
        assert_eq!(
            shape.held_in,
            vec![("page".to_string(), "sections".to_string())]
        );
        let qs = instance_queries("promoBanner", &shape);
        assert_eq!(
            qs[0],
            "*[_type == \"page\" && count(sections[_type == \"promoBanner\"]) > 0][0...5]{_id, \"field\": \"sections\", \"items\": sections[_type == \"promoBanner\"]}"
        );
        let found = parse_instances(&json!([
            {"_id": "page-events", "field": "sections", "items": [
                {"_key": "k1", "_type": "promoBanner", "title": "Old", "image": {}}
            ]}
        ]));
        let line = block_line("promoBanner", "e2e/promo.spec.ts", &shape, &found);
        assert!(
            line.contains("document `page-events` (field `sections`, key `k1`)"),
            "{line}"
        );
        assert!(line.contains("do not add a second"));
        assert!(
            line.contains("Set `heading`, `cta`; remove `image`, `title`"),
            "{line}"
        );
        assert!(line.contains("*[_id == \"page-events\"][0].sections[_key == \"k1\"][0]"));
        // Nothing there: add, saying it was looked up.
        assert!(block_line("promoBanner", "e2e/promo.spec.ts", &shape, &[])
            .contains("looked up on staging: no `promoBanner` block exists yet"));
        // The spec's own count sets the least content.
        let shape = BlockShape {
            min_items: min_items(
                "await expect(cards).toHaveCount(3)\nexpect(n).toBeGreaterThan(4)",
            ),
            ..shape
        };
        assert_eq!(shape.min_items, Some(5));
        assert!(
            block_line("promoBanner", "e2e/promo.spec.ts", &shape, &found)
                .contains("with at least 5 items in it")
        );
        assert_eq!(min_items("expect(x).toBeVisible()"), None);
    }

    /// Neither a connection nor a public read API: it says it couldn't look
    /// and why, and every blocked line says why CoderHelm didn't and how to
    /// grant it.
    #[tokio::test]
    async fn without_any_access_the_note_says_why_and_how_to_grant() {
        let n = vec![
            Need::BlockOnPage {
                block: "promoBanner".into(),
                spec: "e2e/promo.spec.ts".into(),
            },
            Need::Require {
                doc_type: "hero".into(),
                field: "cta".into(),
                default: None,
            },
        ];
        let access = Access {
            repo: "o/r",
            conn: None,
            public: None,
        };
        let out = settle(&reqwest::Client::new(), access, &[], &n, &[]).await;
        assert!(out.blocking[0].what.contains(
            "I couldn't look up existing content: the repository names no CMS project id"
        ));
        assert!(out.blocking[0].grant.contains("create its own content"));
        assert!(out.blocking[1].why_person.contains("no default"));
        assert!(
            out.blocking_lines()[1].contains("so it can next time: give `cta` an `initialValue`")
        );
    }

    #[tokio::test]
    async fn without_a_connection_everything_blocks_with_the_list() {
        let n = vec![Need::Unset {
            doc_type: "hero".into(),
            field: "subtitle".into(),
        }];
        let access = Access {
            repo: "o/r",
            conn: None,
            public: None,
        };
        let out = settle(&reqwest::Client::new(), access, &[], &n, &[]).await;
        assert!(out.changed.is_empty());
        assert_eq!(
            out.blocking[0].what,
            "remove `subtitle` from every `hero` document that still has it"
        );
        assert!(out.blocking[0]
            .why_person
            .contains("no staging CMS write connection for o/r"));
        assert!(out.blocking[0]
            .grant
            .contains("Settings, Repositories, o/r, Staging CMS"));
    }

    #[tokio::test]
    async fn a_production_dataset_is_refused_before_any_call() {
        let c = StagingCms {
            repo: "o/r".into(),
            provider: "sanity".into(),
            project_id: "p1".into(),
            dataset: "production".into(),
            production_dataset: String::new(),
        };
        let n = vec![Need::Unset {
            doc_type: "hero".into(),
            field: "subtitle".into(),
        }];
        let access = |c| Access {
            repo: "o/r",
            conn: Some((c, "t")),
            public: None,
        };
        let out = settle(&reqwest::Client::new(), access(&c), &[], &n, &[]).await;
        assert!(out.changed.is_empty());
        assert!(out
            .blocking_lines()
            .iter()
            .any(|b| b.contains("only to staging")));
        let c2 = StagingCms {
            dataset: "copy".into(),
            ..c.clone()
        };
        let out = settle(
            &reqwest::Client::new(),
            access(&c2),
            &["copy".into()],
            &n,
            &[],
        )
        .await;
        assert!(out
            .blocking_lines()
            .iter()
            .any(|b| b.contains("production settings")));
    }
}
