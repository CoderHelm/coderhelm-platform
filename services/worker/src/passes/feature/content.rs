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
//!   naming exactly which content must change.

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
fn declared_name(line: &str) -> Option<String> {
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

    #[tokio::test]
    async fn without_a_connection_everything_blocks_with_the_list() {
        let n = vec![Need::Unset {
            doc_type: "hero".into(),
            field: "subtitle".into(),
        }];
        let out = settle(&reqwest::Client::new(), None, &[], &n).await;
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
        let out = settle(&reqwest::Client::new(), Some((&c, "t")), &[], &n).await;
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
        )
        .await;
        assert!(out
            .blocking
            .iter()
            .any(|b| b.contains("production settings")));
    }
}
