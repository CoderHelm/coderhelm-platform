//! The team's app map in the worker (`common::app_map` has the rules).
//!
//! - [`refresh_repo`]: after a code graph pass, read that repository's graph
//!   (files, import edges, the per-file app facts) and its code map, regroup
//!   its parts, and describe only the parts that are new or changed, in one
//!   call on the team's light model, metered under feature `app_map`.
//! - [`run`]: a whole team (the dashboard's first visit or Rebuild): every
//!   repository with a graph is refreshed; one without gets its first graph
//!   build queued, and the map follows when that build finishes.

use std::collections::BTreeMap;

use aws_sdk_dynamodb::types::AttributeValue;
use common::app_map::{self as am, RepoInput, RepoMap};
use common::app_surface::Surface;
use tracing::{info, warn};

use super::code_graph::{item_s, query_prefix, APP_FACTS_PREFIX};
use crate::models::{AppMapMessage, TokenUsage};
use crate::WorkerState;

/// What one repository's refresh did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Refreshed {
    pub parts: usize,
    pub described: usize,
    pub reused: usize,
}

/// Reads `repo`'s graph as the app map's input. `None` without a graph.
async fn read_graph(state: &WorkerState, team_id: &str, repo: &str) -> Option<(RepoInput, u64)> {
    let pk = common::code_graph::graph_pk(team_id, repo);
    let meta = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(pk.clone()))
        .key("sk", AttributeValue::S(common::code_graph::META_SK.into()))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())?;
    let commit = item_s(&meta, "indexed_sha");
    let version = meta
        .get("surface")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    let (files, facts, imports) = tokio::join!(
        query_prefix(state, &pk, "F#"),
        query_prefix(state, &pk, APP_FACTS_PREFIX),
        query_prefix(state, &pk, "I#"),
    );
    let files: Vec<String> = files
        .iter()
        .filter_map(|it| item_s(it, "sk").strip_prefix("F#").map(str::to_string))
        .collect();
    let surfaces: BTreeMap<String, Surface> = facts
        .iter()
        .filter_map(|it| {
            let path = item_s(it, "sk").strip_prefix(APP_FACTS_PREFIX)?.to_string();
            Some((path, Surface::from_json(&item_s(it, "s"))?))
        })
        .collect();
    let imports: Vec<(String, String)> = imports
        .iter()
        .filter_map(|it| {
            let sk = item_s(it, "sk");
            let mut p = sk.splitn(3, '#').skip(1);
            Some((p.next()?.to_string(), p.next()?.to_string()))
        })
        .collect();
    Some((
        RepoInput {
            repo: repo.to_string(),
            commit,
            files,
            surfaces,
            imports,
        },
        version,
    ))
}

/// Brings `repo`'s share of the map to its graph. Never fails the caller.
pub async fn refresh_repo(state: &WorkerState, team_id: &str, repo: &str) -> Option<Refreshed> {
    let call = refresh_inner(state, team_id, repo);
    match common::usage::with_feature(common::usage::feature::APP_MAP, call).await {
        Ok(r) => r,
        Err(e) => {
            warn!(repo, error = %e, "App map: the repository could not be brought up to date");
            None
        }
    }
}

async fn refresh_inner(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
) -> Result<Option<Refreshed>, String> {
    let table = &state.config.settings_table_name;
    let Some((input, version)) = read_graph(state, team_id, repo).await else {
        return Ok(None);
    };
    let old = am::load_repo(&state.dynamo, table, team_id, repo).await;
    let mut parts = am::build_parts(&input);
    let todo = am::carry_descriptions(old.as_ref(), &mut parts);
    let reused = parts.len() - todo.len();
    let mut described = 0;
    if !todo.is_empty() {
        let areas: BTreeMap<String, String> =
            common::code_map::load(&state.dynamo, table, team_id, repo)
                .await
                .map(|m| {
                    m.sections
                        .into_iter()
                        .filter_map(|s| {
                            s.id.strip_prefix(common::code_map::AREA_PREFIX)
                                .map(|a| (a.to_string(), s.text.clone()))
                        })
                        .collect()
                })
                .unwrap_or_default();
        described = describe(state, team_id, repo, &input, &mut parts, &todo, &areas).await;
    }
    let mut next: RepoMap = am::repo_map(&input, parts, &chrono::Utc::now().to_rfc3339());
    next.reindex_asked_at = old
        .as_ref()
        .map(|o| o.reindex_asked_at.clone())
        .unwrap_or_default();
    // A graph built before the app facts existed: read it again once, in
    // full; the map follows that build.
    if version < common::app_surface::VERSION as u64 && next.reindex_asked_at != input.commit {
        if super::code_graph::queue_full_index(state, team_id, repo, 0, true).await {
            info!(repo, "App map: the graph is read again for its app facts");
        }
        next.reindex_asked_at = input.commit.clone();
    }
    am::save_repo(&state.dynamo, table, team_id, &next).await?;
    let r = Refreshed {
        parts: next.parts.len(),
        described,
        reused,
    };
    info!(
        repo,
        parts = r.parts,
        described = r.described,
        reused = r.reused,
        "App map: repository brought up to date"
    );
    Ok(Some(r))
}

/// Describes the `todo` parts in calls of at most
/// [`am::MAX_DESCRIBE_PER_CALL`]. A part the reply leaves out keeps its
/// placeholder or older text and is asked again next time.
async fn describe(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    input: &RepoInput,
    parts: &mut [am::Part],
    todo: &[String],
    areas: &BTreeMap<String, String>,
) -> usize {
    let provider = match crate::agent::provider::ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            warn!(repo, error = %e, "App map: no model for this team; parts keep their folder names");
            return 0;
        }
    };
    let mut usage = TokenUsage::default();
    let mut done = 0;
    for chunk in todo.chunks(am::MAX_DESCRIBE_PER_CALL) {
        let asked: Vec<&am::Part> = parts.iter().filter(|p| chunk.contains(&p.id)).collect();
        let prompt = am::describe_prompt(&asked, &input.surfaces, areas);
        let reply = crate::agent::anthropic::converse_json(
            &provider.client(),
            &state.config.light_model_id,
            am::DESCRIBE_SYSTEM,
            &prompt,
            &am::describe_schema(),
            true,
            "low",
            4_000,
            &mut usage,
        )
        .await;
        match reply {
            Ok(v) => done += am::apply_descriptions(&v, chunk, parts, &input.commit).len(),
            Err(e) => warn!(repo, error = %e, "App map: parts could not be described this time"),
        }
    }
    info!(
        repo,
        asked = todo.len(),
        described = done,
        input_tokens = usage.input_tokens,
        output_tokens = usage.output_tokens,
        "App map: parts described"
    );
    done
}

/// Builds or updates a team's whole map.
pub async fn run(state: &WorkerState, msg: AppMapMessage) {
    let table = &state.config.settings_table_name;
    let team_id = msg.team_id.as_str();
    let repos: Vec<String> = if msg.repo.is_empty() {
        super::plan::fetch_team_repos(state, team_id).await
    } else {
        vec![msg.repo.clone()]
    };
    let now = || chrono::Utc::now().timestamp();
    let mut st = am::Status {
        state: "building".into(),
        at: now(),
        done: 0,
        total: repos.len() as u32,
        error: String::new(),
    };
    am::set_status(&state.dynamo, table, team_id, &st).await;
    let mut waiting = 0;
    for repo in &repos {
        let enabled = common::code_graph::is_enabled(&state.dynamo, table, team_id, repo).await;
        if !enabled {
            st.done += 1;
            continue;
        }
        match refresh_repo(state, team_id, repo).await {
            Some(_) => {}
            None => {
                // No graph yet: its first build is queued once; the map
                // follows when it finishes.
                let has = common::code_graph::has_graph(&state.dynamo, table, team_id, repo).await;
                if !has {
                    super::code_graph::queue_full_index(state, team_id, repo, 0, false).await;
                    waiting += 1;
                }
            }
        }
        st.done += 1;
        st.at = now();
        am::set_status(&state.dynamo, table, team_id, &st).await;
    }
    // Repositories the team no longer has leave the map.
    if msg.repo.is_empty() && !repos.is_empty() {
        if let Ok(kept) = am::load_all(&state.dynamo, table, team_id).await {
            for m in kept {
                if !repos.iter().any(|r| r.eq_ignore_ascii_case(&m.repo)) {
                    am::delete_repo(&state.dynamo, table, team_id, &m.repo).await;
                }
            }
        }
    }
    st.state = "ready".into();
    st.at = now();
    st.error = if waiting > 0 {
        format!("{waiting} repositories are still being read for the first time; they join the map when that finishes.")
    } else {
        String::new()
    };
    am::set_status(&state.dynamo, table, team_id, &st).await;
    info!(
        team_id,
        repos = repos.len(),
        waiting,
        "App map: team map brought up to date"
    );
}

#[cfg(test)]
mod tests {
    //! End to end over a made-up harbor app in two repositories: the files
    //! go through the graph indexer's own extraction and import resolution,
    //! then the app map, and the chain from a screen to the endpoint that
    //! answers it and the outside service behind it is checked.
    use super::*;
    use crate::passes::code_graph::{extract, resolve_import};
    use std::collections::HashSet;

    fn index(repo: &str, files: &[(&str, &str)]) -> RepoInput {
        let set: HashSet<String> = files.iter().map(|(p, _)| p.to_string()).collect();
        let mut input = RepoInput {
            repo: repo.into(),
            commit: "c1".into(),
            files: files.iter().map(|(p, _)| p.to_string()).collect(),
            ..Default::default()
        };
        for (path, src) in files {
            if let Some(facts) = extract(path, src) {
                for spec in &facts.imports {
                    if let Some(to) = resolve_import(spec, path, &set) {
                        input.imports.push((path.to_string(), to));
                    }
                }
            }
            let s = common::app_surface::scan(path, src);
            if !s.is_empty() {
                input.surfaces.insert(path.to_string(), s);
            }
        }
        input
    }

    #[test]
    fn a_screen_leads_to_the_endpoint_and_the_service_behind_it() {
        let web = index(
            "harbor/web",
            &[
                ("package.json", r#"{"name": "harbor-web", "dependencies": {"next": "15"}}"#),
                (".env.example", "NEXT_PUBLIC_HARBOR_API=http://localhost:4000\n"),
                ("app/slips/[id]/page.tsx", "import { bookSlip } from '../../../lib/slips';\nexport default function SlipPage() { return null; }\n"),
                ("lib/slips.ts", "export async function bookSlip(id: string) {\n  return fetch(`${process.env.NEXT_PUBLIC_HARBOR_API}/slips/${id}/bookings`, { method: 'POST' });\n}\n"),
            ],
        );
        let api = index(
            "harbor/api",
            &[
                ("Cargo.toml", "[package]\nname = \"harbor-api\"\n[dependencies]\naxum = \"0.7\"\nstripe = \"0.1\"\n"),
                ("src/main.rs", "use axum::{routing::post, Router};\nmod bookings;\nfn app() -> Router { Router::new().route(\"/slips/:id/bookings\", post(bookings::create)) }\n"),
                ("src/bookings.rs", "use stripe::Client;\npub async fn create() { charge().await }\nasync fn charge() {}\n"),
            ],
        );
        // The graph resolved the screen's import of its data helper.
        assert!(web
            .imports
            .contains(&("app/slips/[id]/page.tsx".into(), "lib/slips.ts".into())));
        let maps: Vec<RepoMap> = [web, api]
            .iter()
            .map(|i| am::repo_map(i, am::build_parts(i), "now"))
            .collect();
        let map = am::assemble(&maps);
        let screen = map
            .screens
            .iter()
            .find(|s| s.route == "/slips/:id")
            .expect("screen");
        let chain = am::screen_chain(&map, &maps, &screen.id).expect("chain");
        let req = &chain["requests"][0];
        assert_eq!(req["method"], "POST");
        assert_eq!(req["sure"], "likely");
        assert_eq!(req["to"][0]["repo"], "harbor/api");
        assert_eq!(req["to"][0]["path"], "/slips/:id/bookings");
        assert_eq!(req["to"][0]["file"], "src/main.rs");
        let parts: Vec<&str> = chain["parts"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|p| p["kind"].as_str())
            .collect();
        assert_eq!(parts, vec!["web app", "server"]);
        // The server's part uses Stripe (its manifest and its code say so).
        assert!(map
            .connections
            .iter()
            .any(|c| c.from == "harbor/api#." && c.to == "outside:stripe"));
        let line = map
            .connections
            .iter()
            .find(|c| c.kind == "calls")
            .expect("a calls line");
        assert_eq!(
            (line.from.as_str(), line.to.as_str()),
            ("harbor/web#.", "harbor/api#.")
        );
    }
}
