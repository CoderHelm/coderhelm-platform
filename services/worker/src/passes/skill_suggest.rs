//! Suggests an "always apply" setting for a repository's skills and
//! commands: which of the team's repositories, passes and paths an item is
//! clearly meant for (see `common::skill_apply`).
//!
//! Runs after every sync. Only items whose text, or the team's list of
//! repositories, changed since their last suggestion are sent, all of them
//! in one model call on the light model, counted as `skill_sync`. A
//! suggestion is only shown to an admin; nothing here switches a setting on.

use common::skill_apply::{suggestion_key, Suggestion, PASSES};
use common::skills::{Kind, SkillMeta, SkillStore};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::models::TokenUsage;
use crate::WorkerState;

/// Items sent in one call; the rest wait for the next sync.
const MAX_ITEMS: usize = 40;
/// How much of an item's text is sent.
const ITEM_CHARS: usize = 1_500;
const MAX_TOKENS: i32 = 8_000;

pub const SYSTEM: &str = "You read a software team's written skills and commands (guidance \
     its coding agents can follow) and say which ones are clearly meant for particular \
     repositories of the team. Suggest a repository only when the item's name, description or \
     text names it or names the framework, app or area that only that repository has. When an \
     item is general (it would fit any repository), suggest nothing: give an empty repos list. \
     Passes: planner (plans the change), coder (writes the code), reviewer (judges a pull \
     request's code), pr (writes pull request descriptions), review_comments (writes review \
     comments), replies (writes replies to people), release_notes (writes release notes). Code \
     guidance usually fits coder and reviewer; writing guidance fits pr, review_comments, \
     replies and release_notes. Paths are optional globs such as apps/web/** that the change \
     must touch, only when the item clearly covers one part of a repository. The reason is one \
     short sentence for the team's admin.";

/// The answer's shape.
pub fn output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "index": { "type": "integer" },
                        "repos": { "type": "array", "items": { "type": "string" } },
                        "passes": { "type": "array", "items": { "type": "string", "enum": PASSES } },
                        "paths": { "type": "array", "items": { "type": "string" } },
                        "reason": { "type": "string" }
                    },
                    "required": ["index", "repos", "passes", "paths", "reason"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["items"],
        "additionalProperties": false
    })
}

/// The items to suggest for: skills and commands whose suggestion was
/// worked out from other text or another list of repositories.
pub fn to_suggest<'a>(
    items: &'a [(SkillMeta, String)],
    team_repos: &[String],
) -> Vec<&'a (SkillMeta, String)> {
    items
        .iter()
        .filter(|(m, _)| m.kind != Kind::Agent)
        .filter(|(m, _)| m.suggestion.hash != suggestion_key(&m.hash, team_repos))
        .take(MAX_ITEMS)
        .collect()
}

/// What the model is sent: the team's repositories and each item, numbered.
pub fn user_message(team_repos: &[String], items: &[&(SkillMeta, String)]) -> String {
    let mut out = format!(
        "The team's repositories:\n{}\n\nItems:",
        team_repos
            .iter()
            .map(|r| format!("- {r}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    for (i, (m, body)) in items.iter().enumerate() {
        let (_, _, text) = common::skills::parse_front_matter(body);
        out.push_str(&format!(
            "\n\n<item index=\"{i}\">\n{}:{} from {} ({})\nDescription: {}\n{}\n</item>",
            m.kind.as_str(),
            m.name,
            m.repo,
            m.path,
            m.description,
            common::truncate_str(text.trim(), ITEM_CHARS)
        ));
    }
    out
}

/// Each item's suggestion from the answer, in the order sent. An item the
/// answer leaves out gets none.
pub fn suggestions(answer: &Value, count: usize, team_repos: &[String]) -> Vec<Suggestion> {
    let mut out = vec![Suggestion::default(); count];
    for a in answer["items"].as_array().into_iter().flatten() {
        let Some(i) = a["index"]
            .as_u64()
            .map(|i| i as usize)
            .filter(|i| *i < count)
        else {
            continue;
        };
        out[i] = Suggestion::from_answer(a, team_repos);
    }
    out
}

/// Works out suggestions for `repo`'s items that need one and keeps them.
pub async fn run(state: &WorkerState, team_id: &str, repo: &str) {
    let call = run_inner(state, team_id, repo);
    common::usage::with_feature(common::usage::feature::SKILL_SYNC, call).await
}

async fn run_inner(state: &WorkerState, team_id: &str, repo: &str) {
    let st = SkillStore {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
        team: team_id,
    };
    let items: Vec<(SkillMeta, String)> = match st.compat_queue(repo).await {
        Ok(i) => i.into_iter().map(|(m, body, _)| (m, body)).collect(),
        Err(e) => {
            warn!(error = %e, "Skill suggestions: couldn't read the items");
            return;
        }
    };
    let team_repos = super::plan::fetch_team_repos(state, team_id).await;
    if team_repos.is_empty() {
        return;
    }
    let todo = to_suggest(&items, &team_repos);
    if todo.is_empty() {
        return;
    }
    let provider = match crate::agent::provider::ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Skill suggestions: no model for this team; skipped");
            return;
        }
    };
    let mut usage = TokenUsage::default();
    let answer = crate::agent::anthropic::converse_json(
        &provider.client(),
        &state.config.light_model_id,
        SYSTEM,
        &user_message(&team_repos, &todo),
        &output_schema(),
        // The API holds the answer to the schema.
        true,
        "low",
        MAX_TOKENS,
        &mut usage,
    )
    .await;
    let answer = match answer {
        Ok(a) => a,
        Err(e) => {
            // Nothing is kept, so the next sync asks again.
            warn!(team_id, repo, error = %e, "Skill suggestions failed");
            return;
        }
    };
    let found = suggestions(&answer, todo.len(), &team_repos);
    let mut suggested = 0usize;
    for ((m, _), sg) in todo.iter().zip(&found) {
        let key = suggestion_key(&m.hash, &team_repos);
        match st.put_suggestion(repo, &m.path, &m.hash, &key, sg).await {
            Ok(_) => suggested += usize::from(sg.some()),
            Err(e) => warn!(error = %e, "Skill suggestion couldn't be kept"),
        }
    }
    info!(
        team_id,
        repo,
        asked = todo.len(),
        suggested,
        input_tokens = usage.input_tokens,
        output_tokens = usage.output_tokens,
        "Skill suggestions done"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(name: &str, kind: Kind, hash: &str) -> (SkillMeta, String) {
        (
            SkillMeta {
                repo: "acme/playbook".into(),
                path: format!("{}s/{name}.md", kind.as_str()),
                kind,
                name: name.into(),
                description: format!("About {name}"),
                sha: "s".into(),
                hash: hash.into(),
                enabled: true,
                files: vec![],
                missing_tools: vec![],
                uses: 0,
                last_used_at: String::new(),
                last_used_by: String::new(),
                updated_at: String::new(),
                asks: 0,
                last_asked_at: String::new(),
                switched_by: String::new(),
                switched_at: String::new(),
                compat: Default::default(),
                always: Default::default(),
                suggestion: Default::default(),
            },
            format!("---\nname: {name}\n---\nUse the web app's components."),
        )
    }

    fn repos() -> Vec<String> {
        vec!["acme/web".into(), "acme/api".into()]
    }

    #[test]
    fn only_skills_and_commands_whose_text_or_repos_changed_are_asked() {
        let mut done = item("done", Kind::Skill, "h1");
        done.0.suggestion.hash = suggestion_key("h1", &repos());
        let items = vec![
            done,
            item("new", Kind::Command, "h2"),
            item("bot", Kind::Agent, "h3"),
        ];
        let todo = to_suggest(&items, &repos());
        let names: Vec<&str> = todo.iter().map(|(m, _)| m.name.as_str()).collect();
        assert_eq!(names, vec!["new"], "unchanged and agents are skipped");
        let fewer = vec!["acme/web".to_string()];
        assert_eq!(
            to_suggest(&items, &fewer).len(),
            2,
            "the repositories changed"
        );
    }

    #[test]
    fn the_message_numbers_items_without_their_front_matter() {
        let items = [item("web-style", Kind::Skill, "h")];
        let refs: Vec<&(SkillMeta, String)> = items.iter().collect();
        let msg = user_message(&repos(), &refs);
        assert!(msg.contains("- acme/web\n- acme/api"));
        assert!(msg.contains("<item index=\"0\">\nskill:web-style from acme/playbook"));
        assert!(msg.contains("Use the web app's components."));
        assert!(!msg.contains("name: web-style"));
    }

    #[test]
    fn answers_land_on_their_items_and_bad_ones_are_dropped() {
        let answer = json!({ "items": [
            { "index": 1, "repos": ["acme/web"], "passes": ["coder", "reviewer"], "paths": [], "reason": "Names the web app." },
            { "index": 0, "repos": [], "passes": ["coder"], "paths": [], "reason": "General." },
            { "index": 9, "repos": ["acme/api"], "passes": ["coder"], "paths": [], "reason": "Out of range." },
        ]});
        let found = suggestions(&answer, 3, &repos());
        assert!(!found[0].some(), "general: no repository, no suggestion");
        assert_eq!(found[1].repos, vec!["acme/web"]);
        assert_eq!(found[1].reason, "Names the web app.");
        assert!(!found[2].some(), "left out of the answer");
        assert_eq!(
            output_schema()["properties"]["items"]["items"]["properties"]["passes"]["items"]
                ["enum"]
                .as_array()
                .unwrap()
                .len(),
            PASSES.len()
        );
    }
}
