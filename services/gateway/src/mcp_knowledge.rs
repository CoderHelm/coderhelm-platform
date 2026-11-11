//! The knowledge tools on `/mcp`: `knowledge_search` and `knowledge_list`,
//! for Amazon Bedrock AgentCore Gateway and any other MCP client. They need
//! the `knowledge:read` scope.
//!
//! They read exactly what Chat reads for the same person
//! (`routes::knowledge::chat_readable`): the caller's role must be able to
//! ask Chat questions (`brain_ask`), a base must be one their reading role
//! reaches and open to them by name, and the search is Chat's own
//! (`common::knowledge::search::search`), with connected search tools that
//! sign each person in searched as that person. A knowledge key
//! (`knowledge_keys`) reads as the admin who made it, narrowed to the bases
//! it was given, and never a base read with each person's own sign-in
//! (there is no person to sign in as). Access is read on every call.
//!
//! A passage's text goes back to the caller only; it is never logged, and
//! the team's log of the call names the tool and the base, not the query.

use serde_json::{json, Value};

use common::knowledge::search::{search, Embedder, Hit, Retriever, Store};
use common::knowledge::{self as kb, Base};

use crate::permissions::Holder;

pub const SEARCH: &str = "knowledge_search";
pub const LIST: &str = "knowledge_list";
pub const DEFAULT_LIMIT: usize = 8;
pub const MAX_LIMIT: usize = 20;
pub const MAX_QUERY_CHARS: usize = 1_000;

/// Whether `name` is one of these tools.
pub fn is_tool(name: &str) -> bool {
    name == SEARCH || name == LIST
}

/// The tools as `tools/list` shows them.
pub fn listing() -> Vec<Value> {
    let ann = |title: &str| {
        json!({
            "title": title,
            "readOnlyHint": true,
            "destructiveHint": false,
            "idempotentHint": true,
            "openWorldHint": false,
        })
    };
    vec![
        json!({
            "name": SEARCH,
            "title": "Search knowledge",
            "description": "Searches the team's Coderhelm knowledge bases you can read (synced from Notion, Confluence, Google Drive, Sanity, GitHub, websites and uploaded files) and returns the best passages, each with its source's title, link, last updated time and score. Use it to ground answers in the team's own documents.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "What to find, in plain words." },
                    "base": { "type": "string", "description": "Only this knowledge base: its id or name from knowledge_list. Default: every one you can read." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": MAX_LIMIT, "description": "Most passages to return. Default 8." }
                },
                "required": ["query"],
                "additionalProperties": false,
            },
            "annotations": ann("Search knowledge"),
        }),
        json!({
            "name": LIST,
            "title": "List knowledge bases",
            "description": "Lists the Coderhelm knowledge bases you can read: id, name, description and whether it is a source of truth.",
            "inputSchema": { "type": "object", "properties": {}, "required": [], "additionalProperties": false },
            "annotations": ann("List knowledge bases"),
        }),
    ]
}

/// Who is reading.
#[derive(Debug, Clone)]
pub struct Reader {
    pub team_id: String,
    /// What their role holds now (a key: its maker's).
    pub holder: Holder,
    pub person: kb::Person,
    /// A knowledge key's bases; `None` for a person's own connection.
    pub key_bases: Option<Vec<String>>,
}

impl Reader {
    /// The email connected search tools sign in as: the person's own, never
    /// a key's maker (a key skips those bases).
    pub fn signed_in_as(&self) -> Option<&str> {
        self.key_bases
            .is_none()
            .then_some(self.person.email.as_str())
    }
}

/// The bases `r` reads: Chat's, and for a key only its own and never one
/// read with each person's sign-in.
pub fn readable(bases: Vec<Base>, r: &Reader) -> Result<Vec<Base>, String> {
    if !r.holder.holds("brain_ask") {
        return Err("Your role in Coderhelm can't ask questions in Chat, so it can't search knowledge here either. Ask an admin.".into());
    }
    let mut out = crate::routes::knowledge::chat_readable(bases, &r.holder, &r.person);
    if let Some(only) = &r.key_bases {
        out.retain(|b| only.contains(&b.id) && !b.per_person());
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// The bases one search covers: every readable one, or the one named by id
/// or name.
pub fn pick(readable: &[Base], base: Option<&str>) -> Result<Vec<String>, String> {
    match base.map(str::trim).filter(|b| !b.is_empty()) {
        None => Ok(readable.iter().map(|b| b.id.clone()).collect()),
        Some(b) => readable
            .iter()
            .find(|x| x.id == b || x.name.eq_ignore_ascii_case(b))
            .map(|x| vec![x.id.clone()])
            .ok_or_else(|| {
                "There is no knowledge base by that id or name that you can read. knowledge_list shows them.".into()
            }),
    }
}

/// One passage as the tool returns it.
pub fn passage(h: &Hit, app_url: &str) -> Value {
    let link = if h.url.is_empty() {
        format!("{app_url}/knowledge/base?id={}", h.kb_id)
    } else {
        h.url.clone()
    };
    json!({
        "text": h.text,
        "title": h.file_name,
        "location": h.loc,
        "link": link,
        "updated_at": h.as_of,
        "score": (h.score * 10_000.0).round() / 10_000.0,
        "base": h.kb_name,
        "base_id": h.kb_id,
        "source": if h.source_kind.is_empty() { "upload" } else { h.source_kind.as_str() },
        "source_of_truth": h.authoritative,
    })
}

/// `knowledge_list`.
pub fn list(readable: &[Base]) -> Value {
    json!({
        "bases": readable.iter().map(|b| json!({
            "id": b.id,
            "name": b.name,
            "description": b.description,
            "source_of_truth": b.authoritative,
        })).collect::<Vec<_>>()
    })
}

/// A short account of a call's arguments for the team's log: never the
/// query.
pub fn audit_detail(tool: &str, args: &Value) -> String {
    match args
        .get("base")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        Some(b) => format!("{tool}: base {}", common::truncate_str(b, 80)),
        None => tool.to_string(),
    }
}

/// `knowledge_search` over what `reader` reads.
#[allow(clippy::too_many_arguments)]
pub async fn run_search(
    store: &dyn Store,
    embedder: &dyn Embedder,
    retriever: &dyn Retriever,
    reader: &Reader,
    args: &Value,
    app_url: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(Value, Vec<kb::search::Stale>, Searched), String> {
    let query = args
        .get("query")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|q| !q.is_empty())
        .ok_or("Give query: what to find.")?;
    if query.chars().count() > MAX_QUERY_CHARS {
        return Err(format!("Keep query under {MAX_QUERY_CHARS} characters."));
    }
    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .map_or(DEFAULT_LIMIT, |l| (l as usize).clamp(1, MAX_LIMIT));
    let bases = readable(store.bases(&reader.team_id).await?, reader)?;
    let ids = pick(&bases, args.get("base").and_then(Value::as_str))?;
    if ids.is_empty() {
        return Ok((
            json!({ "passages": [], "note": "You can't read any knowledge base yet." }),
            Vec::new(),
            Searched::default(),
        ));
    }
    let found = search(
        store,
        embedder,
        retriever,
        &reader.team_id,
        &ids,
        query,
        limit,
        now,
    )
    .await
    .map_err(|_| "The knowledge bases couldn't be searched. Try again.".to_string())?;
    let unreached: Vec<Value> = found
        .unreached
        .iter()
        .map(|u| json!({ "base": u.kb_name, "base_id": u.kb_id, "reason": u.reason }))
        .collect();
    let mut out = json!({
        "passages": found.hits.iter().map(|h| passage(h, app_url)).collect::<Vec<_>>(),
    });
    if !unreached.is_empty() {
        out["unreached"] = Value::Array(unreached);
    }
    let searched = Searched {
        kb_ids: ids,
        query: query.to_string(),
        hits: found
            .hits
            .iter()
            .map(|h| (h.kb_id.clone(), h.file_id.clone(), h.file_name.clone()))
            .collect(),
    };
    Ok((out, found.stale, searched))
}

/// What a search covered and found, for the bases' usage.
#[derive(Debug, Default)]
pub struct Searched {
    pub kb_ids: Vec<String>,
    pub query: String,
    /// `(base, item, title)`.
    pub hits: Vec<(String, String, String)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::knowledge::search::testing::{
        remote_base, remote_hit, FakeRetriever, MemoryStore, WordEmbedder,
    };

    fn base(id: &str, name: &str, role: &str) -> Base {
        Base {
            id: id.into(),
            name: name.into(),
            read_role: role.into(),
            ..Default::default()
        }
    }

    fn team() -> MemoryStore {
        let st = MemoryStore::default();
        st.add_file(
            "T",
            &base("kbm", "Handbook", "member"),
            "f1",
            "handbook.md",
            "2026-11-01",
            &["The deploy freeze starts Friday."],
        );
        st.add_file(
            "T",
            &base("kba", "Admin notes", "admin"),
            "f2",
            "admin.md",
            "2026-11-01",
            &["The deploy budget is secret."],
        );
        let mut named = base("kbn", "Legal", "member");
        named.readers = vec!["user:lee@x.com".into()];
        st.add_file(
            "T",
            &named,
            "f3",
            "legal.md",
            "2026-11-01",
            &["The deploy contract terms."],
        );
        let mut per = remote_base("kbp", "Glean", "111122223333");
        if let kb::Backend::Remote(r) = &mut per.backend {
            r.per_person = true;
        }
        st.add_base("T", &per);
        st.add_file(
            "U",
            &base("kbx", "Other team", "viewer"),
            "f9",
            "x.md",
            "2026-11-01",
            &["The deploy of another team."],
        );
        st
    }

    fn person(email: &str, role: &str) -> Reader {
        Reader {
            team_id: "T".into(),
            holder: Holder::builtin(role),
            person: kb::Person::new(email, role),
            key_bases: None,
        }
    }

    async fn ask(st: &MemoryStore, r: &Reader, args: Value) -> Result<Vec<String>, String> {
        let fr = FakeRetriever::default();
        fr.answer(
            "T",
            "kbp",
            Ok(vec![remote_hit("glean/doc", "The deploy plan from Glean.")]),
        );
        let (v, _, _) = run_search(
            st,
            &WordEmbedder,
            &fr,
            r,
            &args,
            "https://app.x",
            chrono::Utc::now(),
        )
        .await?;
        let mut ids: Vec<String> = v["passages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["base_id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    #[tokio::test]
    async fn search_returns_only_what_chat_would_show_the_person() {
        let st = team();
        let q = json!({ "query": "deploy" });
        // A member: member bases, their own sign-in base; not admin's, not a
        // base for named readers they aren't one of, never another team's.
        assert_eq!(
            ask(&st, &person("ann@x.com", "member"), q.clone())
                .await
                .unwrap(),
            vec!["kbm", "kbp"]
        );
        // A named reader also reads Legal.
        assert_eq!(
            ask(&st, &person("lee@x.com", "member"), q.clone())
                .await
                .unwrap(),
            vec!["kbm", "kbn", "kbp"]
        );
        // An admin reads admin bases, but a named base still only by name, as in Chat.
        assert_eq!(
            ask(&st, &person("boss@x.com", "admin"), q.clone())
                .await
                .unwrap(),
            vec!["kba", "kbm", "kbp"]
        );
        // A viewer can't ask Chat, so can't search here.
        assert!(ask(&st, &person("v@x.com", "viewer"), q.clone())
            .await
            .is_err());
        // Naming a base they can't read finds nothing to search.
        let err = ask(
            &st,
            &person("ann@x.com", "member"),
            json!({ "query": "deploy", "base": "kba" }),
        )
        .await
        .unwrap_err();
        assert!(err.contains("no knowledge base"), "{err}");
        assert!(ask(
            &st,
            &person("ann@x.com", "member"),
            json!({ "query": "deploy", "base": "kbx" })
        )
        .await
        .is_err());
        // By name works too.
        assert_eq!(
            ask(
                &st,
                &person("ann@x.com", "member"),
                json!({ "query": "deploy", "base": "handbook" })
            )
            .await
            .unwrap(),
            vec!["kbm"]
        );
        assert!(ask(&st, &person("ann@x.com", "member"), json!({}))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_key_reads_its_own_bases_as_its_maker_and_never_a_per_person_one() {
        let st = team();
        let key = |bases: &[&str], role: &str| Reader {
            key_bases: Some(bases.iter().map(|b| b.to_string()).collect()),
            ..person("boss@x.com", role)
        };
        let q = json!({ "query": "deploy" });
        assert_eq!(
            ask(&st, &key(&["kbm", "kbp"], "admin"), q.clone())
                .await
                .unwrap(),
            vec!["kbm"]
        );
        assert_eq!(
            ask(&st, &key(&["kbm", "kba"], "admin"), q.clone())
                .await
                .unwrap(),
            vec!["kba", "kbm"]
        );
        // The maker was made a member: the admin base drops out at once.
        assert_eq!(
            ask(&st, &key(&["kbm", "kba"], "member"), q.clone())
                .await
                .unwrap(),
            vec!["kbm"]
        );
        // No sign-in is ever used for a key.
        assert_eq!(key(&["kbm"], "admin").signed_in_as(), None);
        assert_eq!(person("a@x.com", "member").signed_in_as(), Some("a@x.com"));
    }

    #[tokio::test]
    async fn passages_carry_title_link_time_and_score() {
        let st = team();
        let fr = FakeRetriever::default();
        let (v, _, _) = run_search(
            &st,
            &WordEmbedder,
            &fr,
            &person("ann@x.com", "member"),
            &json!({ "query": "deploy freeze", "base": "kbm" }),
            "https://app.x",
            chrono::Utc::now(),
        )
        .await
        .unwrap();
        let p = &v["passages"][0];
        assert_eq!(p["title"], "handbook.md");
        assert_eq!(p["link"], "https://app.x/knowledge/base?id=kbm");
        assert_eq!(p["updated_at"], "2026-11-01");
        assert_eq!(p["base"], "Handbook");
        assert_eq!(p["source"], "upload");
        assert!(p["score"].as_f64().unwrap() > 0.0);
        assert!(p["text"].as_str().unwrap().contains("freeze"));
    }

    #[test]
    fn the_list_and_the_log_line() {
        let r = person("ann@x.com", "member");
        let bases = vec![
            base("b", "Beta", "member"),
            base("a", "Alpha", "admin"),
            base("c", "Cee", "member"),
        ];
        let got = readable(bases, &r).unwrap();
        let v = list(&got);
        assert_eq!(v["bases"].as_array().unwrap().len(), 2);
        assert_eq!(v["bases"][0]["name"], "Beta");
        assert_eq!(
            audit_detail(SEARCH, &json!({ "query": "salaries", "base": "kbm" })),
            "knowledge_search: base kbm"
        );
        assert!(!audit_detail(SEARCH, &json!({ "query": "salaries" })).contains("salaries"));
        assert_eq!(listing().len(), 2);
        assert!(is_tool(SEARCH) && is_tool(LIST) && !is_tool("list_runs"));
    }
}
