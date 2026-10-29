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
//!   ([`block_line`]). Without a connection to look with, the note says it
//!   couldn't look instead of guessing.

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
                "add a `{block}` block to the staging page `{spec}` opens, so the new test finds \
                 it. I couldn't look up existing staging content (no staging CMS connection for \
                 this repository), so check first: if that page already has a `{block}` block, \
                 update that one (set the new fields, remove the old ones) instead of adding a \
                 second"
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
    let Some(e) = found.first() else {
        return format!(
            "add a `{block}` block to the staging page `{spec}` opens, so the new test finds it \
             (looked up on staging: no `{block}` block exists yet)"
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
        "staging already has {target}{more} in an older shape: replace or update that one, do \
         not add a second. Set {}; remove {}. Confirm with the query `{verify}`: it returns the \
         block with the new fields",
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

/// What happened to the staging content, for the description.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Outcome {
    /// What was changed on staging, one line each.
    pub changed: Vec<String>,
    /// What must still change, one line each: stops the pull request.
    pub blocking: Vec<String>,
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

/// Makes what can be made on staging and lists what can't. `conn`: the
/// repository's connection and its opened token, `None` without one.
/// `production_in_code`: datasets the repository's production settings
/// name, refused like production.
pub async fn settle(
    http: &reqwest::Client,
    conn: Option<(&StagingCms, &str)>,
    production_in_code: &[String],
    needs: &[Need],
    shapes: &[(String, BlockShape)],
) -> Outcome {
    let mut out = Outcome::default();
    let Some((c, token)) = conn else {
        out.blocking = needs.iter().map(Need::line).collect();
        return out;
    };
    if let Err(e) = c
        .check_writable()
        .and_then(|_| c.check_against_code(production_in_code))
    {
        out.blocking = needs.iter().map(Need::line).collect();
        out.blocking.push(e);
        return out;
    }
    for n in needs {
        // A block a spec needs: look for one already on staging first, so
        // the note says which to replace instead of asking for a second.
        if let Need::BlockOnPage { block, spec } = n {
            let shape = shapes
                .iter()
                .find(|(b, _)| b == block)
                .map(|(_, s)| s.clone())
                .unwrap_or_default();
            let mut found = Vec::new();
            let mut failed = None;
            for q in instance_queries(block, &shape) {
                match query(http, c, token, &q).await {
                    Ok(v) => found.extend(parse_instances(&v)),
                    Err(e) => failed = Some(e),
                }
            }
            out.blocking.push(match failed {
                Some(e) if found.is_empty() => format!(
                    "{} (the staging lookup failed: {e})",
                    n.line().replace(
                        "no staging CMS connection for this repository",
                        "the lookup failed"
                    )
                ),
                _ => block_line(block, spec, &shape, &found),
            });
            continue;
        }
        let Some(q) = n.automatic().then(|| affected_query(n)).flatten() else {
            out.blocking.push(n.line());
            continue;
        };
        let ids = match affected(http, c, token, &q).await {
            Ok(ids) => ids,
            Err(e) => {
                warn!(error = %e, "Could not read staging content");
                out.blocking.push(format!("{} ({e})", n.line()));
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
            Ok(r) => out
                .blocking
                .push(format!("{} (staging returned {})", n.line(), r.status())),
            Err(e) => out.blocking.push(format!("{} ({e})", n.line())),
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
        // No connection: it says it couldn't look.
        let n = Need::BlockOnPage {
            block: "promoBanner".into(),
            spec: "e2e/promo.spec.ts".into(),
        };
        assert!(n
            .line()
            .contains("couldn't look up existing staging content"));
    }

    #[tokio::test]
    async fn without_a_connection_everything_blocks_with_the_list() {
        let n = vec![Need::Unset {
            doc_type: "hero".into(),
            field: "subtitle".into(),
        }];
        let out = settle(&reqwest::Client::new(), None, &[], &n, &[]).await;
        assert!(out.changed.is_empty());
        assert_eq!(
            out.blocking,
            vec!["remove `subtitle` from every `hero` document that still has it"]
        );
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
        let out = settle(&reqwest::Client::new(), Some((&c, "t")), &[], &n, &[]).await;
        assert!(out.changed.is_empty());
        assert!(out.blocking.iter().any(|b| b.contains("only to staging")));
        let c2 = StagingCms {
            dataset: "copy".into(),
            ..c
        };
        let out = settle(
            &reqwest::Client::new(),
            Some((&c2, "t")),
            &["copy".into()],
            &n,
            &[],
        )
        .await;
        assert!(out
            .blocking
            .iter()
            .any(|b| b.contains("production settings")));
    }
}
