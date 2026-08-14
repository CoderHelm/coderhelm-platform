//! Persistent per-repo code graph — so the reviewer knows the STRUCTURE of a
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
//!   - FRESHNESS: full index on enable; push webhooks to the default branch
//!     re-index ONLY the changed files. No scheduled rebuilds, never stale.
//!   - RANKING: PageRank over the file-level reference/import graph, recomputed
//!     on every index pass (the whole partition is one paginated Query). Ranks
//!     tell the agent which files are load-bearing — the "read these first"
//!     signal agents otherwise lack.
//!
//! The graph is exposed to the review agent as typed lookup tools and injected
//! into the review prompt as an exact impacted-file set. It is never an open
//! query endpoint.

use crate::clients::github::GitHubClient;
use crate::models::GraphIndexMessage;
use crate::WorkerState;
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
    for chunk in requests.chunks(25) {
        let mut pending = chunk.to_vec();
        for _ in 0..3 {
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
                Err(e) => {
                    warn!(error = %e, "code graph: batch write failed");
                    break;
                }
            }
        }
    }
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
    loop {
        let mut q = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", attr_s(pk))
            .expression_attribute_values(":p", attr_s(prefix));
        if let Some(k) = start_key {
            q = q.set_exclusive_start_key(Some(k));
        }
        match q.send().await {
            Ok(out) => {
                items.extend(out.items().to_vec());
                match out.last_evaluated_key() {
                    Some(k) if !k.is_empty() => start_key = Some(k.clone()),
                    _ => break,
                }
            }
            Err(e) => {
                warn!(error = %e, "code graph: query failed");
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
    for d in &facts.defs {
        let sk = format!("NM#{}#{}", d.name, path);
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
        contributed.push(sk.clone());
        reqs.push(put_req(pk, sk, vec![]));
    }
    for imp in resolved_imports {
        let fwd = format!("I#{path}#{imp}");
        let rev = format!("RI#{imp}#{path}");
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
pub async fn run(
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
        let resolved: Vec<String> = facts
            .imports
            .iter()
            .filter_map(|s| resolve_import(s, path, &file_set))
            .collect();
        extracted.push((path.clone(), facts, resolved));
    }

    // Rank over the WHOLE graph: current partition state + this pass's files.
    // Edges: file →(imports)→ file, and file →(references name)→ defining file.
    let (ranks, def_counts, ref_counts) = {
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
        (ranks, def_site.len(), ref_counts.len())
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

    info!(
        repo = %format!("{owner}/{repo}"),
        files = extracted.len(),
        items = wrote,
        "Code graph: index pass complete"
    );
    Ok(())
}

// ─── Query API (what the agent's tools call) ────────────────────────────────

/// A live handle to one repo's graph. Cheap to construct; presence-checked.
pub struct Graph {
    pk: String,
}

impl Graph {
    /// Some(_) only if the repo has an indexed graph (META present).
    pub async fn open(
        state: &WorkerState,
        team_id: &str,
        owner: &str,
        repo: &str,
    ) -> Option<Graph> {
        let pk = graph_pk(team_id, owner, repo);
        let meta = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(&pk))
            .key("sk", attr_s("META"))
            .send()
            .await
            .ok()?
            .item()
            .cloned()?;
        let _ = meta;
        Some(Graph { pk })
    }

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

/// Shared handler for the three graph lookup tools — ONE implementation used by
/// both the reviewer and the PR-maker agents so their semantics can't drift.
/// Returns None for a tool name it doesn't own (caller falls through).
pub async fn handle_tool(
    state: &WorkerState,
    graph: Option<&Graph>,
    name: &str,
    input: &serde_json::Value,
) -> Option<String> {
    if !matches!(name, "graph_definition" | "graph_callers" | "graph_impact") {
        return None;
    }
    let Some(graph) = graph else {
        return Some("(code graph not available for this repo)".to_string());
    };
    match name {
        "graph_definition" => {
            let sym = input.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let defs = graph.definitions(state, sym).await;
            Some(if defs.is_empty() {
                format!("No definition of `{sym}` in the graph (unindexed language, or defined dynamically — fall back to search_code).")
            } else {
                defs.iter()
                    .take(10)
                    .map(|(p, k, l, r)| format!("{p}:{l} ({k}, rank {r:.4})"))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
        }
        "graph_callers" => {
            let sym = input.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let mut callers = graph.callers(state, sym).await;
            callers.sort();
            Some(if callers.is_empty() {
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
            })
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
            Some(if impacted.is_empty() {
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
            })
        }
    }
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
}
