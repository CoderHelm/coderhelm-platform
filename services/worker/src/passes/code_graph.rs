//! Persistent per-repo code graph — so every agent knows the STRUCTURE of a
//! codebase (what defines what, who calls what, which files a change reaches),
//! not just its text.
//!
//! Design (deliberately durable, no extra infrastructure):
//!   - EXTRACTION: tree-sitter, in-process. Symbol definitions, name references,
//!     and import edges per file. Syntax-level and name-based — the honest
//!     ceiling for a multi-language graph without per-language build toolchains
//!     (compiler-precise indexers exist only for a few languages and require
//!     full builds; dynamic languages defeat them anyway). References resolve to
//!     definitions BY NAME at query time, so incremental updates never require
//!     re-resolving the world.
//!   - STORAGE: an adjacency list in the settings table (the canonical DynamoDB
//!     graph pattern). One partition per (team, repo): node items, name-index
//!     items, reference items, import edges (both directions), and one manifest
//!     item per file listing everything the file contributed — which makes an
//!     incremental update "delete the manifest's items, re-extract the file".
//!   - FRESHNESS: full index when a repo is turned on, set up for review, or
//!     first worked on; push webhooks to the default branch re-index ONLY the
//!     changed files (one pass per push; >300 files → one full pass). No
//!     scheduled rebuilds.
//!   - RANKING: PageRank over the file-level reference/import graph, recomputed
//!     on every index pass (the whole partition is one paginated Query). Ranks
//!     tell the agent which files are load-bearing — the "read these first"
//!     signal agents otherwise lack.
//!
//! The graph is exposed to every agent that reads code (planner, plan chat,
//! coder, CI fixes, review replies, reviewer, and the team agents they lend
//! tools to) as the same typed lookup tools ([`RepoGraphs`]), and injected
//! into the review prompt as an exact impacted-file set. It is never an open
//! query endpoint. A repo without a graph gets its first build queued once,
//! the first time work needs it; the tools say so instead of answering
//! "no callers".

use crate::agent::llm::ToolDefinition;
use crate::clients::github::GitHubClient;
use crate::models::GraphIndexMessage;
use crate::WorkerState;
use aws_sdk_dynamodb::error::{DisplayErrorContext, ProvideErrorMetadata};
use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, PutRequest, WriteRequest};
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

use super::{attr_n, attr_s};

/// Max symbols/references extracted per file — a generated 20k-line file must
/// not dominate the graph or the write budget.
const MAX_SYMBOLS_PER_FILE: usize = 400;
const MAX_REFS_PER_FILE: usize = 800;
/// Partition key for a repo's graph.
fn graph_pk(team_id: &str, owner: &str, repo: &str) -> String {
    format!("CG#{team_id}#{owner}/{repo}")
}

// ─── Extraction ─────────────────────────────────────────────────────────────

/// A symbol definition extracted from one file.
#[derive(Debug, Clone)]
pub struct Def {
    pub name: String,
    /// "fn" | "struct" | "class" | "enum" | "trait" | "type" | "const" | "mod"
    pub kind: &'static str,
    pub line: usize,
}

/// Everything one file contributes to the graph.
#[derive(Debug, Default)]
pub struct FileFacts {
    pub defs: Vec<Def>,
    /// Names this file references (deduped), excluding its own definitions.
    pub refs: Vec<String>,
    /// Import specifiers as written (`./foo`, `crate::bar`, `pkg.mod`).
    pub imports: Vec<String>,
}

fn language_for(path: &str) -> Option<(&'static str, tree_sitter::Language)> {
    let ext = path.rsplit_once('.').map(|(_, e)| e)?;
    match ext {
        "ts" | "mts" | "cts" => Some(("ts", tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into())),
        "tsx" => Some(("tsx", tree_sitter_typescript::LANGUAGE_TSX.into())),
        "js" | "mjs" | "cjs" | "jsx" => Some(("js", tree_sitter_javascript::LANGUAGE.into())),
        "py" => Some(("py", tree_sitter_python::LANGUAGE.into())),
        "rs" => Some(("rs", tree_sitter_rust::LANGUAGE.into())),
        _ => None,
    }
}

/// True for definition-bearing AST node kinds, mapped to our symbol kind.
fn def_kind(node_kind: &str) -> Option<&'static str> {
    match node_kind {
        // TS/JS
        "function_declaration" | "method_definition" | "generator_function_declaration" => {
            Some("fn")
        }
        "class_declaration" | "abstract_class_declaration" => Some("class"),
        "interface_declaration" => Some("type"),
        "type_alias_declaration" => Some("type"),
        "enum_declaration" => Some("enum"),
        // Python
        "function_definition" => Some("fn"),
        "class_definition" => Some("class"),
        // Rust
        "function_item" => Some("fn"),
        "struct_item" => Some("struct"),
        "enum_item" => Some("enum"),
        "trait_item" => Some("trait"),
        "type_item" => Some("type"),
        "mod_item" => Some("mod"),
        "const_item" | "static_item" => Some("const"),
        _ => None,
    }
}

/// Extract defs/refs/imports from one file. Returns None for unsupported
/// languages (the file still exists in the repo tree; it just has no symbols in
/// the graph). Parse failures are tolerated — tree-sitter produces a partial
/// tree with ERROR nodes and we harvest what parsed.
pub fn extract(path: &str, source: &str) -> Option<FileFacts> {
    let (_, lang) = language_for(path)?;
    let mut parser = tree_sitter::Parser::new();
    parser.set_language(&lang).ok()?;
    let tree = parser.parse(source, None)?;

    let mut facts = FileFacts::default();
    let mut ref_names: HashSet<String> = HashSet::new();
    let mut cursor = tree.walk();
    let mut stack = vec![cursor.node()];
    while let Some(node) = stack.pop() {
        let kind = node.kind();
        if let Some(dk) = def_kind(kind) {
            if facts.defs.len() < MAX_SYMBOLS_PER_FILE {
                if let Some(name_node) = node.child_by_field_name("name") {
                    if let Ok(name) = name_node.utf8_text(source.as_bytes()) {
                        if is_identifier(name) {
                            facts.defs.push(Def {
                                name: name.to_string(),
                                kind: dk,
                                line: node.start_position().row + 1,
                            });
                        }
                    }
                }
            }
        } else if kind == "call_expression" || kind == "call" || kind == "macro_invocation" {
            // The called name: direct identifier or the property/field of a
            // member expression (`obj.method()` → `method`).
            if ref_names.len() < MAX_REFS_PER_FILE {
                if let Some(f) = node
                    .child_by_field_name("function")
                    .or_else(|| node.child_by_field_name("macro"))
                {
                    let name_node = f
                        .child_by_field_name("property")
                        .or_else(|| f.child_by_field_name("field"))
                        .or_else(|| f.child_by_field_name("attribute"))
                        .or_else(|| f.child_by_field_name("name"))
                        .unwrap_or(f);
                    if let Ok(name) = name_node.utf8_text(source.as_bytes()) {
                        if is_identifier(name) {
                            ref_names.insert(name.to_string());
                        }
                    }
                }
            }
        } else if kind == "import_statement"
            || kind == "import_from_statement"
            || kind == "use_declaration"
        {
            if let Ok(text) = node.utf8_text(source.as_bytes()) {
                if let Some(spec) = import_specifier(text) {
                    facts.imports.push(spec);
                }
                // Imported NAMES are references too — `import {foo} from "x"`
                // links this file to foo's definition wherever it lives.
                for cap in text
                    .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .filter(|w| is_identifier(w) && w.len() >= 3)
                    .take(20)
                {
                    ref_names.insert(cap.to_string());
                }
            }
        }
        for child in node.children(&mut cursor) {
            stack.push(child);
        }
    }

    // One definition per name per file: overloads, `impl` blocks and
    // re-declarations share a name, and each name maps to ONE `NM#{name}#{path}`
    // key — duplicates made the manifest string set and the batch write
    // invalid. Keep the first (lowest line) occurrence.
    dedupe_defs(&mut facts.defs);

    // A file referencing its own definitions is noise for caller queries.
    let own: HashSet<&str> = facts.defs.iter().map(|d| d.name.as_str()).collect();
    facts.refs = ref_names
        .into_iter()
        .filter(|r| !own.contains(r.as_str()) && !is_stopword(r))
        .collect();
    facts.refs.sort();
    facts.imports.sort();
    facts.imports.dedup();
    Some(facts)
}

/// Keep the first (lowest-line) definition of each name.
fn dedupe_defs(defs: &mut Vec<Def>) {
    defs.sort_by_key(|d| d.line);
    let mut seen: HashSet<String> = HashSet::new();
    defs.retain(|d| seen.insert(d.name.clone()));
}

fn is_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 100
        && s.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '$')
        && !s.chars().next().unwrap_or('0').is_numeric()
}

/// Common language keywords/builtins that would otherwise create edges to
/// everything ("import", "from", "use", tiny names).
fn is_stopword(s: &str) -> bool {
    s.len() < 3
        || matches!(
            s,
            "import"
                | "from"
                | "use"
                | "crate"
                | "self"
                | "super"
                | "type"
                | "const"
                | "await"
                | "async"
                | "return"
                | "export"
                | "default"
                | "require"
                | "println"
                | "format"
                | "String"
                | "Vec"
                | "Option"
                | "Result"
                | "Some"
                | "None"
                | "true"
                | "false"
        )
}

/// Pull the module path out of an import statement's text.
fn import_specifier(text: &str) -> Option<String> {
    if let Some(q) = text.find(['"', '\'']) {
        let rest = &text[q + 1..];
        let end = rest.find(['"', '\''])?;
        let spec = &rest[..end];
        if !spec.is_empty() && spec.len() <= 200 {
            return Some(spec.to_string());
        }
        return None;
    }
    // Rust `use a::b::c;` / Python `import a.b`
    let body = text
        .trim_start_matches("use ")
        .trim_start_matches("import ")
        .trim_start_matches("from ");
    let first = body
        .split([' ', ';', ':', '{', '\n'])
        .next()
        .unwrap_or("")
        .trim();
    if !first.is_empty() && first.len() <= 200 {
        Some(first.to_string())
    } else {
        None
    }
}

/// Resolve an import specifier to a repo file path, best-effort name matching
/// against the known file set (`./util` → `src/util.ts`, `a.b.c` → `a/b/c.py`).
fn resolve_import(spec: &str, from_file: &str, files: &HashSet<String>) -> Option<String> {
    let dir = from_file.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
    let mut candidates: Vec<String> = Vec::new();
    if let Some(rel) = spec.strip_prefix("./") {
        candidates.push(if dir.is_empty() {
            rel.to_string()
        } else {
            format!("{dir}/{rel}")
        });
    } else if spec.starts_with("../") {
        let mut d = dir;
        let mut s = spec;
        while let Some(up) = s.strip_prefix("../") {
            d = d.rsplit_once('/').map(|(p, _)| p).unwrap_or("");
            s = up;
        }
        candidates.push(if d.is_empty() {
            s.to_string()
        } else {
            format!("{d}/{s}")
        });
    } else {
        // Dotted (Python) or path-ish module: try as a path.
        candidates.push(spec.replace('.', "/").replace("::", "/"));
    }
    for cand in candidates {
        for ext in [
            "",
            ".ts",
            ".tsx",
            ".js",
            ".py",
            ".rs",
            "/index.ts",
            "/index.js",
            "/mod.rs",
            "/__init__.py",
        ] {
            let p = format!("{cand}{ext}");
            if files.contains(&p) {
                return Some(p);
            }
        }
    }
    None
}

// ─── PageRank ───────────────────────────────────────────────────────────────

/// PageRank over a file-level directed graph (edges: file → file it depends
/// on). Standard damping 0.85, fixed 20 iterations — plenty for repo-sized
/// graphs. Pure, unit-tested.
pub fn pagerank(nodes: &[String], edges: &[(usize, usize)]) -> Vec<f64> {
    let n = nodes.len();
    if n == 0 {
        return vec![];
    }
    let mut out_deg = vec![0usize; n];
    let mut incoming: Vec<Vec<usize>> = vec![Vec::new(); n];
    for &(src, dst) in edges {
        if src < n && dst < n && src != dst {
            out_deg[src] += 1;
            incoming[dst].push(src);
        }
    }
    let d = 0.85;
    let base = (1.0 - d) / n as f64;
    let mut rank = vec![1.0 / n as f64; n];
    for _ in 0..20 {
        // Dangling mass (files with no outgoing deps) is spread uniformly.
        let dangling: f64 = (0..n)
            .filter(|&i| out_deg[i] == 0)
            .map(|i| rank[i])
            .sum::<f64>()
            / n as f64;
        let mut next = vec![base + d * dangling; n];
        for (v, inc) in incoming.iter().enumerate() {
            for &u in inc {
                next[v] += d * rank[u] / out_deg[u] as f64;
            }
        }
        rank = next;
    }
    rank
}

// ─── Store ──────────────────────────────────────────────────────────────────

/// Batch-write with automatic chunking (25/request) and one retry pass for
/// unprocessed items. Best-effort: a dropped item degrades one lookup, never
/// the pipeline.
async fn batch_write(state: &WorkerState, requests: Vec<WriteRequest>) {
    // A batch may not name the same key twice — DynamoDB rejects the WHOLE
    // request. Drop duplicates before chunking.
    let requests = dedupe_requests(requests);
    for chunk in requests.chunks(25) {
        let mut pending = chunk.to_vec();
        for attempt in 0..MAX_WRITE_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(backoff(attempt)).await;
            }
            match state
                .dynamo
                .batch_write_item()
                .request_items(&state.config.settings_table_name, pending.clone())
                .send()
                .await
            {
                Ok(out) => {
                    pending = out
                        .unprocessed_items()
                        .and_then(|m| m.get(&state.config.settings_table_name))
                        .cloned()
                        .unwrap_or_default();
                    if pending.is_empty() {
                        break;
                    }
                }
                Err(e) if is_throttle_code(e.code()) && attempt + 1 < MAX_WRITE_ATTEMPTS => {
                    warn!(attempt, "code graph: batch write throttled, backing off");
                }
                Err(e) => {
                    warn!(error = %DisplayErrorContext(&e), "code graph: batch write failed");
                    break;
                }
            }
        }
        if !pending.is_empty() {
            warn!(
                dropped = pending.len(),
                "code graph: items still unprocessed after retries"
            );
        }
    }
}

const MAX_WRITE_ATTEMPTS: u32 = 6;

/// Exponential backoff for throttled/unprocessed writes: 100ms, 200ms, … 3.2s.
fn backoff(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis(100u64 << attempt.min(5))
}

/// DynamoDB error codes that mean "slow down", not "this request is wrong".
fn is_throttle_code(code: Option<&str>) -> bool {
    matches!(
        code,
        Some(
            "ProvisionedThroughputExceededException"
                | "ThrottlingException"
                | "RequestLimitExceeded"
                | "Throttling"
        )
    )
}

/// The (pk, sk) a write request targets.
fn request_key(r: &WriteRequest) -> Option<(String, String)> {
    let get = |m: &HashMap<String, AttributeValue>| {
        Some((
            m.get("pk")?.as_s().ok()?.clone(),
            m.get("sk")?.as_s().ok()?.clone(),
        ))
    };
    if let Some(p) = r.put_request() {
        return get(p.item());
    }
    r.delete_request().and_then(|d| get(d.key()))
}

/// Drop requests whose key already appeared (first one wins).
fn dedupe_requests(requests: Vec<WriteRequest>) -> Vec<WriteRequest> {
    let mut seen: HashSet<(String, String)> = HashSet::new();
    requests
        .into_iter()
        .filter(|r| match request_key(r) {
            Some(k) => seen.insert(k),
            None => true,
        })
        .collect()
}

fn put_req(pk: &str, sk: String, extra: Vec<(&str, AttributeValue)>) -> WriteRequest {
    let mut b = PutRequest::builder()
        .item("pk", attr_s(pk))
        .item("sk", attr_s(&sk));
    for (k, v) in extra {
        b = b.item(k, v);
    }
    WriteRequest::builder()
        .set_put_request(b.build().ok())
        .build()
}

fn del_req(pk: &str, sk: &str) -> WriteRequest {
    WriteRequest::builder()
        .set_delete_request(
            DeleteRequest::builder()
                .key("pk", attr_s(pk))
                .key("sk", attr_s(sk))
                .build()
                .ok(),
        )
        .build()
}

/// Query every item in the partition with an sk prefix (paginated).
async fn query_prefix(
    state: &WorkerState,
    pk: &str,
    prefix: &str,
) -> Vec<HashMap<String, AttributeValue>> {
    let mut items = Vec::new();
    let mut start_key = None;
    let mut throttled = 0u32;
    loop {
        let mut q = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", attr_s(pk))
            .expression_attribute_values(":p", attr_s(prefix));
        if let Some(k) = start_key.clone() {
            q = q.set_exclusive_start_key(Some(k));
        }
        match q.send().await {
            Ok(out) => {
                throttled = 0;
                items.extend(out.items().to_vec());
                match out.last_evaluated_key() {
                    Some(k) if !k.is_empty() => start_key = Some(k.clone()),
                    _ => break,
                }
            }
            Err(e) if is_throttle_code(e.code()) && throttled + 1 < MAX_WRITE_ATTEMPTS => {
                // Same page again after a pause.
                throttled += 1;
                tokio::time::sleep(backoff(throttled)).await;
            }
            Err(e) => {
                warn!(error = %DisplayErrorContext(&e), "code graph: query failed");
                break;
            }
        }
    }
    items
}

fn item_s(item: &HashMap<String, AttributeValue>, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

/// Emit the write requests for one file's facts. The `F#{path}` manifest item
/// records every sk the file contributed so an incremental update can delete
/// them precisely.
fn file_write_requests(
    pk: &str,
    path: &str,
    facts: &FileFacts,
    resolved_imports: &[String],
    rank: f64,
) -> Vec<WriteRequest> {
    let mut reqs = Vec::new();
    let mut contributed: Vec<String> = Vec::new();
    // The manifest is a DynamoDB string SET — duplicates are rejected — and a
    // batch may not write the same key twice. Every sk is emitted once.
    let mut seen: HashSet<String> = HashSet::new();
    for d in &facts.defs {
        let sk = format!("NM#{}#{}", d.name, path);
        if !seen.insert(sk.clone()) {
            continue;
        }
        contributed.push(sk.clone());
        reqs.push(put_req(
            pk,
            sk,
            vec![
                ("kind", attr_s(d.kind)),
                ("line", attr_n(d.line as u64)),
                ("rank", AttributeValue::N(format!("{rank:.6}"))),
            ],
        ));
    }
    for name in &facts.refs {
        let sk = format!("R#{name}#{path}");
        if !seen.insert(sk.clone()) {
            continue;
        }
        contributed.push(sk.clone());
        reqs.push(put_req(pk, sk, vec![]));
    }
    for imp in resolved_imports {
        let fwd = format!("I#{path}#{imp}");
        let rev = format!("RI#{imp}#{path}");
        if !seen.insert(fwd.clone()) {
            continue;
        }
        seen.insert(rev.clone());
        contributed.push(fwd.clone());
        contributed.push(rev.clone());
        reqs.push(put_req(pk, fwd, vec![]));
        reqs.push(put_req(pk, rev, vec![]));
    }
    // Manifest last: list of contributed sks (bounded by the per-file caps).
    reqs.push(put_req(
        pk,
        format!("F#{path}"),
        vec![(
            "sks",
            AttributeValue::Ss(if contributed.is_empty() {
                vec!["-".to_string()] // SS can't be empty
            } else {
                contributed
            }),
        )],
    ));
    reqs
}

// ─── Indexing ───────────────────────────────────────────────────────────────

/// Full or incremental index pass. Full: snapshot the repo at the branch head,
/// extract every supported file, recompute ranks, write everything (existing
/// items are overwritten in place — same keys — so a full pass is also a
/// repair). Incremental: delete the changed files' contributed items via their
/// manifests, re-extract just those files, then recompute ranks from the
/// updated reference items.
///
/// Records the build's progress on the repo's STATUS item (indexing, then
/// ready or failed with a plain reason), and does nothing for a repo whose
/// graph was turned off after the pass was queued.
pub async fn run(
    state: &WorkerState,
    msg: GraphIndexMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use common::code_graph::{self as cg, BuildState};
    let table = &state.config.settings_table_name;
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    if !cg::is_enabled(&state.dynamo, table, &msg.team_id, &repo).await {
        info!(repo = %repo, "Code graph: turned off for this repo, pass skipped");
        return Ok(());
    }
    // An incremental pass needs a built graph to update: without one, build
    // it in full (once) instead of indexing a handful of files.
    if msg.changed_files.is_some()
        && !cg::has_graph(&state.dynamo, table, &msg.team_id, &repo).await
    {
        queue_full_index(state, &msg.team_id, &repo, msg.installation_id, false).await;
        return Ok(());
    }
    let team_id = msg.team_id.clone();
    cg::set_status(
        &state.dynamo,
        table,
        &team_id,
        &repo,
        BuildState::Indexing,
        "",
    )
    .await;
    match index_pass(state, msg).await {
        Ok(()) => {
            cg::set_status(&state.dynamo, table, &team_id, &repo, BuildState::Ready, "").await;
            Ok(())
        }
        Err(e) => {
            cg::set_status(
                &state.dynamo,
                table,
                &team_id,
                &repo,
                BuildState::Failed,
                &cg::failure_reason(&e.to_string()),
            )
            .await;
            Err(e)
        }
    }
}

async fn index_pass(
    state: &WorkerState,
    msg: GraphIndexMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    let pk = graph_pk(&msg.team_id, &msg.repo_owner, &msg.repo_name);
    let owner = &msg.repo_owner;
    let repo = &msg.repo_name;

    // The enqueuer may not know the default branch — resolve it here.
    let branch = if msg.branch.is_empty() {
        github.get_default_branch(owner, repo).await?
    } else {
        msg.branch.clone()
    };
    let head = github.get_ref(owner, repo, &branch).await?;
    let bytes = github.download_tarball(owner, repo, &head).await?;
    let snapshot = crate::clients::repo_snapshot::RepoSnapshot::from_tarball(&bytes)?;
    let all_paths: Vec<String> = snapshot.tree().await;
    let file_set: HashSet<String> = all_paths.iter().cloned().collect();

    // Which files to (re)extract this pass.
    let targets: Vec<String> = match &msg.changed_files {
        Some(changed) => changed
            .iter()
            .filter(|p| language_for(p).is_some())
            .cloned()
            .collect(),
        None => all_paths
            .iter()
            .filter(|p| language_for(p).is_some())
            .cloned()
            .collect(),
    };
    info!(
        repo = %format!("{owner}/{repo}"),
        files = targets.len(),
        incremental = msg.changed_files.is_some(),
        "Code graph: indexing"
    );

    // Incremental: remove what the changed files previously contributed
    // (covers deletes and renames — a deleted file's manifest is removed too).
    if let Some(changed) = &msg.changed_files {
        let mut dels = Vec::new();
        for path in changed {
            let manifest = query_prefix(state, &pk, &format!("F#{path}")).await;
            for m in manifest {
                if let Some(AttributeValue::Ss(sks)) = m.get("sks") {
                    for sk in sks {
                        if sk != "-" {
                            dels.push(del_req(&pk, sk));
                        }
                    }
                }
                dels.push(del_req(&pk, &item_s(&m, "sk")));
            }
        }
        batch_write(state, dels).await;
    }

    // Extract.
    let mut extracted: Vec<(String, FileFacts, Vec<String>)> = Vec::new();
    for path in &targets {
        let Some(source) = snapshot.read_file(path).await else {
            continue; // deleted or binary
        };
        let Some(facts) = extract(path, &source) else {
            continue;
        };
        // Two specifiers (`./a` and `./a.ts`) can resolve to the same file.
        let mut resolved: Vec<String> = facts
            .imports
            .iter()
            .filter_map(|s| resolve_import(s, path, &file_set))
            .collect();
        resolved.sort();
        resolved.dedup();
        extracted.push((path.clone(), facts, resolved));
    }

    // Rank over the WHOLE graph: current partition state + this pass's files.
    // Edges: file →(imports)→ file, and file →(references name)→ defining file.
    let (ranks, def_counts, ref_counts, file_defs) = {
        let mut def_site: HashMap<String, Vec<String>> = HashMap::new(); // name → def files
        let mut file_refs: HashMap<String, Vec<String>> = HashMap::new(); // file → names
        let mut file_imports: HashMap<String, Vec<String>> = HashMap::new();
        // Existing items (skip files re-extracted this pass — fresher in memory).
        // A FULL pass rewrites everything, so loading the old partition would be
        // pure waste — only incremental passes merge in the untouched files.
        let incremental = msg.changed_files.is_some();
        let target_set: HashSet<&String> = targets.iter().collect();
        for it in if incremental {
            query_prefix(state, &pk, "NM#").await
        } else {
            Vec::new()
        } {
            let sk = item_s(&it, "sk");
            let mut parts = sk.splitn(3, '#');
            let (_, name, path) = (parts.next(), parts.next(), parts.next());
            if let (Some(name), Some(path)) = (name, path) {
                if !target_set.contains(&path.to_string()) {
                    def_site
                        .entry(name.to_string())
                        .or_default()
                        .push(path.to_string());
                }
            }
        }
        for it in query_prefix(state, &pk, "R#").await {
            let sk = item_s(&it, "sk");
            let mut parts = sk.splitn(3, '#');
            let (_, name, path) = (parts.next(), parts.next(), parts.next());
            if let (Some(name), Some(path)) = (name, path) {
                if !target_set.contains(&path.to_string()) {
                    file_refs
                        .entry(path.to_string())
                        .or_default()
                        .push(name.to_string());
                }
            }
        }
        for it in query_prefix(state, &pk, "I#").await {
            let sk = item_s(&it, "sk");
            let mut parts = sk.splitn(3, '#');
            let (_, path, dst) = (parts.next(), parts.next(), parts.next());
            if let (Some(path), Some(dst)) = (path, dst) {
                if !target_set.contains(&path.to_string()) {
                    file_imports
                        .entry(path.to_string())
                        .or_default()
                        .push(dst.to_string());
                }
            }
        }
        for (path, facts, resolved) in &extracted {
            for d in &facts.defs {
                def_site
                    .entry(d.name.clone())
                    .or_default()
                    .push(path.clone());
            }
            file_refs.insert(path.clone(), facts.refs.clone());
            file_imports.insert(path.clone(), resolved.clone());
        }

        let mut nodes: Vec<String> = file_set.iter().cloned().collect();
        nodes.sort();
        let idx: HashMap<&String, usize> = nodes.iter().enumerate().map(|(i, p)| (p, i)).collect();
        let mut edges: Vec<(usize, usize)> = Vec::new();
        for (src, imports) in &file_imports {
            if let Some(&si) = idx.get(src) {
                for dst in imports {
                    if let Some(&di) = idx.get(dst) {
                        edges.push((si, di));
                    }
                }
            }
        }
        for (src, names) in &file_refs {
            if let Some(&si) = idx.get(src) {
                for name in names {
                    if let Some(defs) = def_site.get(name) {
                        // Ambiguous names (defined in many files) are weak
                        // signals — cap the fan-out.
                        for dst in defs.iter().take(3) {
                            if let Some(&di) = idx.get(dst) {
                                edges.push((si, di));
                            }
                        }
                    }
                }
            }
        }
        let pr = pagerank(&nodes, &edges);
        let ranks: HashMap<String, f64> = nodes.into_iter().zip(pr).collect();
        // Caller counts per name (for the meta summary).
        let mut ref_counts: HashMap<String, usize> = HashMap::new();
        for names in file_refs.values() {
            for n in names {
                *ref_counts.entry(n.clone()).or_default() += 1;
            }
        }
        // Per-file definition counts — feeds the precomputed browse summary.
        let mut file_defs: HashMap<String, usize> = HashMap::new();
        for files in def_site.values() {
            for f in files {
                *file_defs.entry(f.clone()).or_default() += 1;
            }
        }
        (ranks, def_site.len(), ref_counts.len(), file_defs)
    };

    // Write the extracted files' items with their fresh ranks.
    let mut reqs = Vec::new();
    for (path, facts, resolved) in &extracted {
        let rank = ranks.get(path).copied().unwrap_or(0.0);
        reqs.extend(file_write_requests(&pk, path, facts, resolved, rank));
    }
    let wrote = reqs.len();
    batch_write(state, reqs).await;

    // Meta: presence marker + stats. The reviewer checks this to decide
    // whether graph tools are available for the repo.
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&pk))
        .item("sk", attr_s("META"))
        .item("indexed_sha", attr_s(&head))
        .item("branch", attr_s(&branch))
        .item("files", attr_n(extracted.len() as u64))
        .item("symbols", attr_n(def_counts as u64))
        .item("names_referenced", attr_n(ref_counts as u64))
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await;

    // Precomputed browse summary for the dashboard's Code Graph page: the top
    // load-bearing files by rank, with their definition counts. One item read
    // instead of a partition scan.
    let mut top: Vec<(&String, f64)> = ranks
        .iter()
        .filter(|(p, _)| language_for(p).is_some())
        .map(|(p, r)| (p, *r))
        .collect();
    top.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let summary: Vec<serde_json::Value> = top
        .iter()
        .take(60)
        .map(|(p, r)| {
            serde_json::json!({
                "path": p,
                "rank": (r * 1_000_000.0).round() / 1_000_000.0,
                "defs": file_defs.get(p.as_str()).copied().unwrap_or(0),
            })
        })
        .collect();
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&pk))
        .item("sk", attr_s("SUMMARY"))
        .item(
            "top_files",
            attr_s(&serde_json::to_string(&summary).unwrap_or_default()),
        )
        .send()
        .await;

    info!(
        repo = %format!("{owner}/{repo}"),
        files = extracted.len(),
        items = wrote,
        "Code graph: index pass complete"
    );
    Ok(())
}

// ─── Query API (what the agent's tools call) ────────────────────────────────

/// A handle to one repo's built graph (see [`availability`]).
#[derive(Clone, Debug)]
pub struct Graph {
    pk: String,
}

impl Graph {
    /// Where is `name` defined? → (path, kind, line, rank), highest rank first.
    pub async fn definitions(
        &self,
        state: &WorkerState,
        name: &str,
    ) -> Vec<(String, String, u64, f64)> {
        let mut out: Vec<(String, String, u64, f64)> =
            query_prefix(state, &self.pk, &format!("NM#{name}#"))
                .await
                .into_iter()
                .filter_map(|it| {
                    let sk = item_s(&it, "sk");
                    let path = sk.splitn(3, '#').nth(2)?.to_string();
                    let kind = item_s(&it, "kind");
                    let line = it
                        .get("line")
                        .and_then(|v| v.as_n().ok())
                        .and_then(|n| n.parse::<u64>().ok())
                        .unwrap_or(0);
                    let rank = it
                        .get("rank")
                        .and_then(|v| v.as_n().ok())
                        .and_then(|n| n.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    Some((path, kind, line, rank))
                })
                .collect();
        out.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));
        out
    }

    /// Which files reference `name`? (the caller set, name-based).
    pub async fn callers(&self, state: &WorkerState, name: &str) -> Vec<String> {
        query_prefix(state, &self.pk, &format!("R#{name}#"))
            .await
            .into_iter()
            .filter_map(|it| {
                let sk = item_s(&it, "sk");
                sk.splitn(3, '#').nth(2).map(|s| s.to_string())
            })
            .collect()
    }

    /// Files that import `path` (reverse import edges).
    pub async fn importers(&self, state: &WorkerState, path: &str) -> Vec<String> {
        query_prefix(state, &self.pk, &format!("RI#{path}#"))
            .await
            .into_iter()
            .filter_map(|it| {
                let sk = item_s(&it, "sk");
                sk.splitn(3, '#').nth(2).map(|s| s.to_string())
            })
            .collect()
    }

    /// Exact impacted-file set for a change to `paths`: every file that imports
    /// one of them, plus every file referencing a symbol they define. Returns
    /// (file, why) pairs, deduped, bounded.
    pub async fn impacted_by(
        &self,
        state: &WorkerState,
        paths: &[String],
    ) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for path in paths.iter().take(30) {
            for f in self.importers(state, path).await {
                if !paths.contains(&f) && seen.insert(f.clone()) {
                    out.push((f, format!("imports {path}")));
                }
            }
            // Symbols defined in this file → their callers.
            for m in query_prefix(state, &self.pk, &format!("F#{path}")).await {
                if let Some(AttributeValue::Ss(sks)) = m.get("sks") {
                    // Bounded: 15 symbols per changed file keeps the query
                    // fan-out sane on wide PRs (the agent can dig further with
                    // graph_callers on specific symbols).
                    for sk in sks.iter().filter(|s| s.starts_with("NM#")).take(15) {
                        if let Some(name) = sk.split('#').nth(1) {
                            for f in self.callers(state, name).await {
                                if !paths.contains(&f) && seen.insert(f.clone()) {
                                    out.push((f, format!("references `{name}`")));
                                }
                            }
                        }
                    }
                }
            }
            if out.len() > 200 {
                break; // pathological fan-out — the summary says "200+"
            }
        }
        out
    }
}

// ─── The agents' tools ──────────────────────────────────────────────────────

/// The graph lookups' source on the data source log and in "What the run
/// used".
const SOURCE_ID: &str = "code-graph";
const SOURCE_NAME: &str = "Code graph";

/// The three lookup tool names.
pub const TOOLS: [&str; 3] = ["graph_definition", "graph_callers", "graph_impact"];

/// What the graph reflects, said on every tool so no agent mistakes it for
/// its own branch.
const BRANCH_CAVEAT: &str = "The graph reflects the repository's default branch as last \
     indexed, not edits made on this work's branch: read the file to see your own changes.";

/// The graph lookup tools: one set of definitions for every agent that reads
/// code (planner, plan chat, coder, CI fixes, review replies, reviewer, and
/// team agents they lend tools to). `multi_repo` adds the optional `repo`
/// for agents that may look at the team's other repositories.
pub fn tools(multi_repo: bool) -> Vec<ToolDefinition> {
    let with_repo = |mut props: serde_json::Value| {
        if multi_repo {
            props["repo"] = serde_json::json!({
                "type": "string",
                "description": "owner/name of another of the team's repositories; leave out for this one"
            });
        }
        props
    };
    vec![
        ToolDefinition {
            name: "graph_definition".to_string(),
            description: format!(
                "EXACT lookup: where is this symbol DEFINED? Returns path, kind, line and an \
                 importance rank from the repo's code graph. Faster and more precise than \
                 search_code for known symbol names. {BRANCH_CAVEAT}"
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": with_repo(serde_json::json!({
                    "name": {"type": "string", "description": "symbol name, e.g. processOrder"}
                })),
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "graph_callers".to_string(),
            description: format!(
                "EXACT lookup: which files REFERENCE this symbol (its caller set)? Use before \
                 changing a function or type to find every caller to update. Name-based: \
                 confirm surprising hits by reading the file. {BRANCH_CAVEAT}"
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": with_repo(serde_json::json!({"name": {"type": "string"}})),
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "graph_impact".to_string(),
            description: format!(
                "EXACT lookup: every file that imports OR references symbols defined in the \
                 given files: what changing them reaches. Use on the files you plan to change \
                 (or a PR's changed files) to know where breakage can show up. {BRANCH_CAVEAT}"
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": with_repo(serde_json::json!({
                    "paths": {"type": "array", "items": {"type": "string"}}
                })),
                "required": ["paths"]
            }),
        },
    ]
}

/// Which agent the tools serve: each is told when to use them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Planner,
    Coder,
    Reviewer,
}

/// What an agent is told about the graph, when it has the tools.
pub fn prompt_line(role: Role) -> &'static str {
    match role {
        Role::Planner => {
            "\n\nThis repository has a CODE GRAPH (graph_definition, graph_callers, \
             graph_impact). Before you plan a change to a symbol or file, run graph_impact on \
             the files you expect to change and graph_callers on the symbols whose behavior or \
             signature changes, then list every affected file (and repository) in the plan so \
             the coder updates them too. The graph reflects the default branch."
        }
        Role::Coder => {
            "\n\nThis repository has a CODE GRAPH (graph_definition, graph_callers, \
             graph_impact). Before you change a function, type or exported symbol, call \
             graph_callers on it (or graph_impact on its file) and update every caller in the \
             same change. The graph reflects the default branch, not your edits on this branch."
        }
        Role::Reviewer => {
            "\nThis repo has a CODE GRAPH: prefer graph_definition/graph_callers/graph_impact \
             for exact structural lookups (where is X defined, who calls X, what a change \
             reaches); use search_code for text and unindexed languages. The graph reflects the \
             default branch, not this PR's head."
        }
    }
}

/// Offer the graph to an agent: its tools, and the line telling it when to
/// use them. The one place every pass adds the graph, so no agent's set
/// drifts. Returns whether anything was offered.
pub fn offer(
    graphs: &RepoGraphs,
    role: Role,
    tools: &mut Vec<ToolDefinition>,
    system: &mut String,
) -> bool {
    let defs = graphs.tools();
    if defs.is_empty() {
        return false;
    }
    for d in defs {
        if !tools.iter().any(|t| t.name == d.name) {
            tools.push(d);
        }
    }
    system.push_str(prompt_line(role));
    true
}

/// Whether one repository's graph can answer now, and if not, why.
#[derive(Clone, Debug)]
pub enum Avail {
    Ready(Graph),
    /// Turned off for the repo.
    Off,
    /// On, but not built (yet): queued, indexing, or failed with a reason.
    Building(common::code_graph::Shown),
}

/// What a lookup answers when the repo has no graph to read. Never an empty
/// "no callers": the agent must know the graph wasn't consulted.
pub fn unavailable_reply(repo: &str, avail: &Avail) -> String {
    use common::code_graph::Shown;
    match avail {
        Avail::Ready(_) => String::new(),
        Avail::Off => format!(
            "The code graph is turned off for {repo}, so this lookup wasn't made. Use \
             search_code and read_file instead."
        ),
        Avail::Building(Shown::Failed(why)) => format!(
            "No code graph exists for {repo}: its last build failed ({why}). An admin can \
             rebuild it on the Code graph page. This is NOT an answer of \"no results\": use \
             search_code and read_file instead."
        ),
        Avail::Building(Shown::Indexing) => format!(
            "No code graph exists for {repo} yet: it is being indexed now. This is NOT an \
             answer of \"no results\": use search_code and read_file until it is ready."
        ),
        Avail::Building(_) => format!(
            "No code graph exists for {repo} yet: indexing has been queued and is usually done \
             in a few minutes. This is NOT an answer of \"no results\": use search_code and \
             read_file until it is ready."
        ),
    }
}

/// Queue a full build of `repo`'s graph unless one was already queued
/// (see `common::code_graph::mark_queued`). `installation_id` 0 is looked up.
/// Returns whether this call queued it.
pub async fn queue_full_index(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    installation_id: u64,
    force: bool,
) -> bool {
    let Some((owner, name)) = repo.split_once('/') else {
        return false;
    };
    if state.config.ticket_queue_url.is_empty() {
        return false;
    }
    let table = &state.config.settings_table_name;
    match common::code_graph::mark_queued(&state.dynamo, table, team_id, repo, force).await {
        Ok(true) => {}
        Ok(false) => return false,
        Err(e) => {
            warn!(error = %e, repo, "Code graph: could not mark a build queued");
            return false;
        }
    }
    let installation_id = if installation_id > 0 {
        installation_id
    } else {
        team_installation_id(state, team_id).await.unwrap_or(0)
    };
    let msg = crate::models::WorkerMessage::GraphIndex(GraphIndexMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        branch: String::new(),
        changed_files: None,
    });
    let sent = match (installation_id, serde_json::to_string(&msg)) {
        (0, _) => Err("no GitHub installation for the team".to_string()),
        (_, Ok(body)) => state
            .sqs
            .send_message()
            .queue_url(&state.config.ticket_queue_url)
            .message_body(body)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        (_, Err(e)) => Err(e.to_string()),
    };
    match sent {
        Ok(()) => {
            info!(repo, "Code graph: full build queued");
            true
        }
        Err(e) => {
            warn!(error = %e, repo, "Code graph: could not queue a build");
            common::code_graph::unmark_queued(&state.dynamo, table, team_id, repo).await;
            false
        }
    }
}

/// The team's GitHub App installation, for a build queued without one.
async fn team_installation_id(state: &WorkerState, team_id: &str) -> Option<u64> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.teams_table_name)
        .key("team_id", attr_s(team_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .ok()?
        .item()
        .cloned()?;
    item.get("github_installation_id")
        .or_else(|| item.get("github_install_id"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .filter(|&n| n > 0)
}

/// Whether `repo`'s graph can answer, queuing its first build when it is on
/// and has none (the backfill: once, the first time any work needs it).
pub async fn availability(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    installation_id: u64,
) -> Avail {
    use common::code_graph::{self as cg, Shown};
    let table = &state.config.settings_table_name;
    if !cg::is_enabled(&state.dynamo, table, team_id, repo).await {
        return Avail::Off;
    }
    let has = cg::has_graph(&state.dynamo, table, team_id, repo).await;
    let status = cg::status(&state.dynamo, table, team_id, repo).await;
    let now = chrono::Utc::now().timestamp();
    match cg::shown(true, status.as_ref(), has, now) {
        Shown::Ready => Avail::Ready(Graph {
            pk: cg::graph_pk(team_id, repo),
        }),
        shown => {
            if cg::may_queue(status.as_ref(), has, now)
                && queue_full_index(state, team_id, repo, installation_id, false).await
            {
                return Avail::Building(Shown::Queued);
            }
            Avail::Building(shown)
        }
    }
}

/// The code graph for one piece of work: the repository it is on, and the
/// team's other repositories it may also look at. Built once per pass; the
/// one implementation every agent's executor delegates to, so the tools'
/// semantics can't drift between agents.
pub struct RepoGraphs {
    team_id: String,
    installation_id: u64,
    default_repo: String,
    default: Avail,
    /// Other repositories the work may read (`owner/name`).
    allowed: Vec<String>,
    actor: String,
    pr: u64,
    run: common::live::store::RunRef,
    others: tokio::sync::Mutex<HashMap<String, Avail>>,
}

impl RepoGraphs {
    /// Opens the work's repository's graph, queuing its first build when it
    /// has none. `actor` names the agent on the data source log.
    pub async fn load(
        state: &WorkerState,
        team_id: &str,
        installation_id: u64,
        repo: &str,
        allowed: Vec<String>,
        actor: &str,
        pr: u64,
    ) -> RepoGraphs {
        let default = availability(state, team_id, repo, installation_id).await;
        Self::with(team_id, installation_id, repo, default, allowed, actor, pr)
    }

    /// A handle over a known availability (tests; and callers that already
    /// checked it).
    pub fn with(
        team_id: &str,
        installation_id: u64,
        repo: &str,
        default: Avail,
        allowed: Vec<String>,
        actor: &str,
        pr: u64,
    ) -> RepoGraphs {
        RepoGraphs {
            team_id: team_id.to_string(),
            installation_id,
            default_repo: repo.to_string(),
            default,
            allowed: allowed
                .into_iter()
                .filter(|r| !r.eq_ignore_ascii_case(repo))
                .collect(),
            actor: actor.to_string(),
            pr,
            run: common::live::store::RunRef::default(),
            others: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn with_run(mut self, run_id: &str, origin: String) -> Self {
        self.run = common::live::store::RunRef {
            run_id: run_id.to_string(),
            origin,
        };
        self
    }

    /// The work's repository's graph, when it is built.
    pub fn ready(&self) -> Option<&Graph> {
        match &self.default {
            Avail::Ready(g) => Some(g),
            _ => None,
        }
    }

    /// The tools to offer: none only when the graph is off for the work's
    /// repository and there is no other repository to look at. A graph still
    /// being built is offered, so a lookup says so instead of the agent
    /// guessing.
    pub fn tools(&self) -> Vec<ToolDefinition> {
        if matches!(self.default, Avail::Off) && self.allowed.is_empty() {
            return Vec::new();
        }
        tools(!self.allowed.is_empty())
    }

    /// Runs a graph tool. `None` when `name` isn't one.
    pub async fn handle(
        &self,
        state: &WorkerState,
        name: &str,
        input: &serde_json::Value,
    ) -> Option<String> {
        if !TOOLS.contains(&name) {
            return None;
        }
        let started = std::time::Instant::now();
        let asked = input
            .get("repo")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|r| !r.is_empty());
        let (repo, outcome, text) = match asked {
            Some(r) if !r.eq_ignore_ascii_case(&self.default_repo) => {
                match self.allowed.iter().find(|a| a.eq_ignore_ascii_case(r)) {
                    None => (
                        r.to_string(),
                        "refused",
                        format!("{r} isn't one of the repositories this work may read."),
                    ),
                    Some(other) => {
                        let avail = self.other(state, other).await;
                        let (outcome, text) = answer(state, other, &avail, name, input).await;
                        (other.clone(), outcome, text)
                    }
                }
            }
            _ => {
                let (outcome, text) =
                    answer(state, &self.default_repo, &self.default, name, input).await;
                (self.default_repo.clone(), outcome, text)
            }
        };
        let query = format!(
            "{name} {}",
            common::truncate_str(
                &input
                    .get("name")
                    .map(|v| v.as_str().unwrap_or("").to_string())
                    .or_else(|| input.get("paths").map(|v| v.to_string()))
                    .unwrap_or_default(),
                300
            )
        );
        let logged = common::live::store::log_activity_in(
            &state.dynamo,
            &state.config.settings_table_name,
            &self.team_id,
            &common::live::store::Activity {
                source_id: SOURCE_ID.into(),
                source_name: SOURCE_NAME.into(),
                actor: self.actor.clone(),
                repo,
                pr: self.pr,
                query,
                outcome,
                detail: if outcome == "ok" {
                    common::live::count(text.lines().count(), "line")
                } else {
                    common::truncate_str(&text, 300).to_string()
                },
                ms: started.elapsed().as_millis(),
            },
            &self.run,
        )
        .await;
        if let Err(e) = logged {
            warn!(error = %e, "Could not record a code graph lookup");
        }
        Some(common::head_tail_str(&text, 8_000))
    }

    /// Another of the team's repositories, opened once per pass.
    async fn other(&self, state: &WorkerState, repo: &str) -> Avail {
        let key = repo.to_lowercase();
        if let Some(a) = self.others.lock().await.get(&key) {
            return a.clone();
        }
        let avail = availability(state, &self.team_id, repo, self.installation_id).await;
        self.others.lock().await.insert(key, avail.clone());
        avail
    }
}

/// One lookup against one repository's graph: (outcome, text).
async fn answer(
    state: &WorkerState,
    repo: &str,
    avail: &Avail,
    name: &str,
    input: &serde_json::Value,
) -> (&'static str, String) {
    let Avail::Ready(graph) = avail else {
        return ("refused", unavailable_reply(repo, avail));
    };
    let text = match name {
        "graph_definition" => {
            let sym = input.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let defs = graph.definitions(state, sym).await;
            if defs.is_empty() {
                format!("No definition of `{sym}` in the graph (unindexed language, or defined dynamically — fall back to search_code).")
            } else {
                defs.iter()
                    .take(10)
                    .map(|(p, k, l, r)| format!("{p}:{l} ({k}, rank {r:.4})"))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
        "graph_callers" => {
            let sym = input.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let mut callers = graph.callers(state, sym).await;
            callers.sort();
            if callers.is_empty() {
                format!("No files reference `{sym}` (or it's in an unindexed language — confirm with search_code).")
            } else {
                format!(
                    "{} file(s) reference `{sym}`:\n{}",
                    callers.len(),
                    callers
                        .iter()
                        .take(40)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            }
        }
        _ => {
            let paths: Vec<String> = input
                .get("paths")
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let impacted = graph.impacted_by(state, &paths).await;
            if impacted.is_empty() {
                "No other files import or reference symbols from those files.".to_string()
            } else {
                format!(
                    "{} impacted file(s):\n{}",
                    impacted.len(),
                    impacted
                        .iter()
                        .take(60)
                        .map(|(f, why)| format!("{f} — {why}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            }
        }
    };
    ("ok", text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_typescript_defs_refs_imports() {
        let src = r#"
import { helper } from "./util";
export function processOrder(o: Order): void { helper(o); validate(o); }
class OrderService { run() { this.processAll(); } }
"#;
        let facts = extract("src/order.ts", src).expect("ts supported");
        let names: Vec<&str> = facts.defs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"processOrder"));
        assert!(names.contains(&"OrderService"));
        assert!(facts.refs.iter().any(|r| r == "validate"));
        assert!(facts.refs.iter().any(|r| r == "helper"));
        // Own defs are not self-references.
        assert!(!facts.refs.iter().any(|r| r == "processOrder"));
        assert!(facts.imports.iter().any(|i| i == "./util"));
    }

    #[test]
    fn extracts_python_and_rust() {
        let py = extract(
            "dag.py",
            "import helpers\ndef build_dag():\n    helpers.make_task()\n",
        )
        .expect("py supported");
        assert!(py
            .defs
            .iter()
            .any(|d| d.name == "build_dag" && d.kind == "fn"));
        assert!(py.refs.iter().any(|r| r == "make_task"));

        let rs = extract(
            "lib.rs",
            "use crate::store::save;\npub struct Engine;\npub fn run() { save(); }\n",
        )
        .expect("rs supported");
        assert!(rs
            .defs
            .iter()
            .any(|d| d.name == "Engine" && d.kind == "struct"));
        assert!(rs.defs.iter().any(|d| d.name == "run"));
        assert!(rs.refs.iter().any(|r| r == "save"));
    }

    #[test]
    fn unsupported_language_is_none() {
        assert!(extract("main.tf", "resource \"aws_s3_bucket\" \"b\" {}").is_none());
    }

    #[test]
    fn import_resolution_relative_and_dotted() {
        let files: HashSet<String> = ["src/util.ts", "pkg/mod/helpers.py", "src/lib/mod.rs"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            resolve_import("./util", "src/order.ts", &files),
            Some("src/util.ts".to_string())
        );
        assert_eq!(
            resolve_import("pkg.mod.helpers", "dag.py", &files),
            Some("pkg/mod/helpers.py".to_string())
        );
        assert_eq!(resolve_import("nonexistent", "a.ts", &files), None);
    }

    #[test]
    fn pagerank_ranks_the_hub_highest() {
        // c is imported by a and b → c must outrank both.
        let nodes: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let ranks = pagerank(&nodes, &[(0, 2), (1, 2)]);
        assert!(ranks[2] > ranks[0]);
        assert!(ranks[2] > ranks[1]);
        // Ranks form a probability distribution.
        let sum: f64 = ranks.iter().sum();
        assert!((sum - 1.0).abs() < 0.01);
    }

    #[test]
    fn pagerank_empty_graph() {
        assert!(pagerank(&[], &[]).is_empty());
    }

    #[test]
    fn stopwords_and_identifiers() {
        assert!(is_stopword("use"));
        assert!(is_stopword("ab"));
        assert!(!is_stopword("processOrder"));
        assert!(is_identifier("snake_case_2"));
        assert!(!is_identifier("2fast"));
        assert!(!is_identifier("a.b"));
    }

    #[test]
    fn duplicate_definition_names_keep_the_first() {
        // Two `impl` blocks each define `new`; the TS file overloads `run`.
        let rs = "struct A;\nimpl A { fn new() -> A { A } }\nstruct B;\nimpl B { fn new() -> B { B } }\n";
        let facts = extract("src/lib.rs", rs).expect("rs supported");
        let news: Vec<&Def> = facts.defs.iter().filter(|d| d.name == "new").collect();
        assert_eq!(news.len(), 1);
        assert_eq!(news[0].line, 2, "keeps the first definition");
    }

    #[test]
    fn manifest_and_writes_carry_each_key_once() {
        let facts = FileFacts {
            defs: vec![
                Def {
                    name: "run".into(),
                    kind: "fn",
                    line: 1,
                },
                Def {
                    name: "run".into(),
                    kind: "fn",
                    line: 9,
                },
            ],
            refs: vec!["helper".into(), "helper".into()],
            imports: vec![],
        };
        let resolved = vec!["src/b.ts".to_string(), "src/b.ts".to_string()];
        let reqs = file_write_requests("pk", "src/a.ts", &facts, &resolved, 0.1);
        let keys: Vec<(String, String)> = reqs.iter().filter_map(request_key).collect();
        let unique: HashSet<&(String, String)> = keys.iter().collect();
        assert_eq!(keys.len(), unique.len(), "no duplicate keys in one batch");
        let manifest = reqs
            .iter()
            .filter_map(|r| r.put_request())
            .find(|p| {
                p.item()
                    .get("sk")
                    .and_then(|v| v.as_s().ok())
                    .map(String::as_str)
                    == Some("F#src/a.ts")
            })
            .expect("manifest written");
        let sks = manifest.item().get("sks").unwrap().as_ss().unwrap();
        let set: HashSet<&String> = sks.iter().collect();
        assert_eq!(sks.len(), set.len());
        assert_eq!(sks.len(), 4); // NM#run, R#helper, I#..., RI#...
    }

    #[test]
    fn dedupe_requests_drops_repeated_keys() {
        let reqs = vec![
            put_req("pk", "a".into(), vec![]),
            del_req("pk", "a"),
            put_req("pk", "b".into(), vec![]),
        ];
        assert_eq!(dedupe_requests(reqs).len(), 2);
    }

    #[test]
    fn throttling_codes_are_retried_others_are_not() {
        assert!(is_throttle_code(Some(
            "ProvisionedThroughputExceededException"
        )));
        assert!(is_throttle_code(Some("ThrottlingException")));
        assert!(!is_throttle_code(Some("ValidationException")));
        assert!(!is_throttle_code(None));
        assert!(backoff(1) < backoff(2));
        assert_eq!(backoff(50), backoff(5));
    }

    fn graphs(default: Avail, others: &[&str]) -> RepoGraphs {
        RepoGraphs::with(
            "t",
            1,
            "acme/app",
            default,
            others.iter().map(|s| s.to_string()).collect(),
            "planner",
            0,
        )
    }

    fn queued() -> Avail {
        Avail::Building(common::code_graph::Shown::Queued)
    }

    #[test]
    fn the_tools_say_what_branch_they_read() {
        for t in tools(false) {
            assert!(t.description.contains("default branch"), "{}", t.name);
            assert!(t.input_schema["properties"].get("repo").is_none());
        }
        for t in tools(true) {
            assert!(t.input_schema["properties"].get("repo").is_some());
        }
        let names: Vec<String> = tools(false).into_iter().map(|t| t.name).collect();
        assert_eq!(names, TOOLS.map(String::from).to_vec());
    }

    #[test]
    fn no_graph_says_indexing_is_queued_never_no_callers() {
        let r = unavailable_reply("acme/app", &queued());
        assert!(r.contains("indexing has been queued"), "{r}");
        assert!(r.contains("NOT an answer"), "{r}");
        assert!(!r.starts_with("No files reference"));
        let failed = unavailable_reply(
            "acme/app",
            &Avail::Building(common::code_graph::Shown::Failed(
                "GitHub refused access.".into(),
            )),
        );
        assert!(failed.contains("GitHub refused access.") && failed.contains("rebuild"));
        assert!(unavailable_reply("acme/app", &Avail::Off).contains("turned off"));
    }

    #[test]
    fn an_explicit_off_offers_no_tools() {
        let mut tools_out = Vec::new();
        let mut system = String::new();
        assert!(!offer(
            &graphs(Avail::Off, &[]),
            Role::Coder,
            &mut tools_out,
            &mut system
        ));
        assert!(tools_out.is_empty() && system.is_empty());
        // Off here, but the planner may still read the team's other repos.
        assert_eq!(graphs(Avail::Off, &["acme/api"]).tools().len(), 3);
    }

    #[test]
    fn a_graph_being_built_is_still_offered_with_when_to_use_it() {
        let mut tools_out = Vec::new();
        let mut system = String::new();
        assert!(offer(
            &graphs(queued(), &[]),
            Role::Planner,
            &mut tools_out,
            &mut system
        ));
        assert_eq!(tools_out.len(), 3);
        assert!(system.contains("graph_impact") && system.contains("affected file"));
        // Offered twice: no duplicate definitions.
        offer(
            &graphs(queued(), &[]),
            Role::Planner,
            &mut tools_out,
            &mut system,
        );
        assert_eq!(tools_out.len(), 3);
        assert!(prompt_line(Role::Coder).contains("graph_callers"));
        assert!(prompt_line(Role::Reviewer).contains("default branch"));
    }

    #[test]
    fn the_work_repo_is_not_listed_twice() {
        let g = graphs(queued(), &["ACME/app", "acme/api"]);
        assert_eq!(g.allowed, vec!["acme/api".to_string()]);
    }
}
