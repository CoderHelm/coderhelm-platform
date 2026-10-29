//! Building on another open pull request instead of copying it.
//!
//! A run once started its branch from the default branch while the ticket
//! depended on another open (work-in-progress) pull request, and the
//! branch's first commits re-created that pull request's commits under new
//! hashes. The rules here:
//!
//! - [`dependency`]: a ticket that names the open pull request it depends on
//!   (a link or `#123` in a line that says it depends on, builds on, is
//!   blocked by or comes after it), or the ticket key of one, starts its
//!   branch from that pull request's branch, and its pull request targets
//!   that branch (stacked). It states the dependency and the merge order.
//! - [`copied`]: before the pull request opens, the branch's commits are
//!   compared with every other open pull request's commits by patch id (the
//!   change itself, whitespace and line numbers aside). A branch that
//!   re-created another pull request's commits is rebased onto that pull
//!   request's branch, which drops the copies, and stacks on it; when that
//!   can't be done cleanly the run refuses, saying which commits it copied.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

use regex::Regex;
use serde_json::Value;

use crate::clients::github::{GitHubClient, RebaseResult};

/// The open pull request a branch builds on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Dependency {
    pub number: u64,
    pub branch: String,
    pub title: String,
    /// How it was found (the ticket's line, or the commits it copied).
    pub why: String,
}

impl Dependency {
    /// The pull request section: what it builds on and the merge order.
    pub fn section(&self) -> String {
        format!(
            "{SECTION_START}\n## Builds on #{n}\n\nThis pull request is stacked on #{n} (`{b}`, \
             \"{t}\"): its base is that branch, so its diff shows only this ticket's change. {why}\n\n\
             Merge order: #{n} first, then this one. When #{n} merges, GitHub moves this pull \
             request's base to #{n}'s base; CoderHelm rebases it if needed.\n{SECTION_END}",
            n = self.number,
            b = self.branch,
            t = self.title.replace('"', "'"),
            why = self.why.trim(),
        )
    }
}

pub const SECTION_START: &str = "<!-- coderhelm:stack -->";
pub const SECTION_END: &str = "<!-- /coderhelm:stack -->";

/// Words that make a line say the ticket depends on what it names.
const DEPENDS: &[&str] = &[
    "depends on",
    "dependent on",
    "depend on",
    "blocked by",
    "builds on",
    "build on",
    "built on",
    "on top of",
    "stacked on",
    "after",
    "requires",
    "needs",
    "follow-up to",
    "follows",
    "wip",
];

fn says_depends(line: &str) -> bool {
    let l = line.to_ascii_lowercase();
    DEPENDS.iter().any(|w| {
        l.match_indices(w).any(|(i, _)| {
            let before = l[..i].chars().next_back();
            let after = l[i + w.len()..].chars().next();
            !before.is_some_and(|c| c.is_ascii_alphanumeric())
                && !after.is_some_and(|c| c.is_ascii_alphanumeric())
        })
    })
}

/// What a ticket names as its dependencies: pull request numbers of this
/// repository, and other tickets' keys, from the lines that say it depends
/// on them.
pub fn named(text: &str, owner: &str, repo: &str, own_ticket: &str) -> (Vec<u64>, Vec<String>) {
    static KEY: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let key = KEY.get_or_init(|| Regex::new(r"\b([A-Z][A-Z0-9]{1,9}-\d{1,6})\b").unwrap());
    let url = Regex::new(&format!(
        r"(?i)github\.com/{}/{}/pull/(\d+)",
        regex::escape(owner),
        regex::escape(repo)
    ))
    .unwrap();
    static NUM: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let num = NUM.get_or_init(|| Regex::new(r"(?i)(?:^|[\s(])(?:pr\s*)?#(\d{1,7})\b").unwrap());
    let mut prs = Vec::new();
    let mut keys = Vec::new();
    for line in text.lines() {
        if !says_depends(line) {
            continue;
        }
        for c in url.captures_iter(line).chain(num.captures_iter(line)) {
            if let Ok(n) = c[1].parse::<u64>() {
                if !prs.contains(&n) {
                    prs.push(n);
                }
            }
        }
        for c in key.captures_iter(line) {
            let k = c[1].to_string();
            if !k.eq_ignore_ascii_case(own_ticket) && !keys.contains(&k) {
                keys.push(k);
            }
        }
    }
    (prs, keys)
}

/// The open pull request (from `open`, the open pulls listing) a ticket
/// depends on: one it names by number, else one whose title or branch
/// carries a ticket key it names. Only pull requests from this repository
/// (a fork's branch can't be a base), never the run's own branch.
pub fn pick(
    open: &[Value],
    prs: &[u64],
    keys: &[String],
    owner: &str,
    repo: &str,
    own_branch: &str,
) -> Option<Dependency> {
    let full = format!("{owner}/{repo}").to_ascii_lowercase();
    let usable = |p: &&Value| {
        p["head"]["repo"]["full_name"]
            .as_str()
            .is_some_and(|f| f.eq_ignore_ascii_case(&full))
            && p["head"]["ref"].as_str().is_some_and(|b| b != own_branch)
    };
    let dep = |p: &Value, why: String| Dependency {
        number: p["number"].as_u64().unwrap_or(0),
        branch: p["head"]["ref"].as_str().unwrap_or("").to_string(),
        title: p["title"].as_str().unwrap_or("").to_string(),
        why,
    };
    for n in prs {
        if let Some(p) = open
            .iter()
            .filter(usable)
            .find(|p| p["number"].as_u64() == Some(*n))
        {
            return Some(dep(p, format!("The ticket says it depends on #{n}.")));
        }
    }
    for k in keys {
        let kl = k.to_ascii_lowercase();
        if let Some(p) = open.iter().filter(usable).find(|p| {
            p["title"]
                .as_str()
                .is_some_and(|t| t.to_ascii_lowercase().contains(&kl))
                || p["head"]["ref"]
                    .as_str()
                    .is_some_and(|b| b.to_ascii_lowercase().contains(&kl))
        }) {
            return Some(dep(
                p,
                format!("The ticket says it depends on {k}, whose open pull request this is."),
            ));
        }
    }
    None
}

/// The open pull request the ticket says it depends on, if any.
pub async fn dependency(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    ticket_id: &str,
    text: &str,
    own_branch: &str,
) -> Option<Dependency> {
    let (prs, keys) = named(text, owner, repo, ticket_id);
    if prs.is_empty() && keys.is_empty() {
        return None;
    }
    let open = github.list_open_pulls(owner, repo).await.ok()?;
    pick(open.as_array()?, &prs, &keys, owner, repo, own_branch)
}

/// Why a run refuses to open its pull request: its branch copied another
/// open pull request's commits and couldn't be stacked on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused(pub String);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stopped before opening a pull request: {}", self.0)
    }
}

impl std::error::Error for Refused {}

/// At the start of a run: the open pull request the ticket depends on
/// (never one of this ticket's own earlier attempts) becomes the base its
/// branch starts from and its pull request targets.
pub async fn for_ticket(
    github: &GitHubClient,
    msg: &mut crate::models::TicketMessage,
    own_attempts: &[String],
) -> Option<Dependency> {
    let text = format!("{}\n{}", msg.title, msg.body);
    let d = dependency(
        github,
        &msg.repo_owner,
        &msg.repo_name,
        &msg.ticket_id,
        &text,
        "",
    )
    .await
    .filter(|d| !own_attempts.contains(&d.branch))?;
    msg.base_branch = d.branch.clone();
    Some(d)
}

/// Right before the pull request opens: refuses a branch that copied
/// another open pull request's commits unless it could be stacked on it.
/// Returns the dependency the pull request states (the ticket's, or the
/// one found by its commits).
pub async fn before_pr(
    github: &GitHubClient,
    msg: &mut crate::models::TicketMessage,
    branch: &str,
    from_ticket: Option<Dependency>,
) -> Result<Option<Dependency>, Refused> {
    let mut base = msg.base_branch.clone();
    let found = settle(github, &msg.repo_owner, &msg.repo_name, &mut base, branch)
        .await
        .map_err(Refused)?;
    msg.base_branch = base;
    Ok(found.or_else(|| from_ticket.filter(|d| d.branch == msg.base_branch)))
}

// ── Patch ids ───────────────────────────────────────────────────────────────

/// A commit's patch id: the same for the same change whatever its hash,
/// parent, message, line numbers or whitespace (like `git patch-id`).
pub fn patch_id(files: &[(String, String)]) -> Option<u64> {
    let mut by_path: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for (path, patch) in files {
        let lines: Vec<String> = patch
            .lines()
            .filter(|l| {
                (l.starts_with('+') || l.starts_with('-'))
                    && !l.starts_with("+++")
                    && !l.starts_with("---")
            })
            .map(|l| {
                let (sign, rest) = l.split_at(1);
                format!(
                    "{sign}{}",
                    rest.chars()
                        .filter(|c| !c.is_whitespace())
                        .collect::<String>()
                )
            })
            .collect();
        by_path.entry(path.as_str()).or_default().extend(lines);
    }
    if by_path.values().all(|l| l.is_empty()) {
        return None;
    }
    let mut h = std::collections::hash_map::DefaultHasher::new();
    by_path.hash(&mut h);
    Some(h.finish())
}

/// Another open pull request's commits the branch re-created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copy {
    pub dep: Dependency,
    /// The branch's commits whose change is one of that pull request's.
    pub commits: Vec<String>,
}

/// The branch's commits that match another pull request's, by patch id:
/// `ours` and `theirs` are (sha, patch id).
pub fn matches(ours: &[(String, u64)], theirs: &[(String, u64)]) -> Vec<String> {
    let ids: BTreeSet<u64> = theirs.iter().map(|(_, i)| *i).collect();
    ours.iter()
        .filter(|(_, i)| ids.contains(i))
        .map(|(s, _)| s.clone())
        .collect()
}

const MAX_COMMITS: usize = 60;
const MAX_PRS: usize = 40;

async fn ids(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    shas: &[String],
) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    for sha in shas.iter().take(MAX_COMMITS) {
        if let Ok(files) = github.commit_patches(owner, repo, sha).await {
            if let Some(id) = patch_id(&files) {
                out.push((sha.clone(), id));
            }
        }
    }
    out
}

/// The open pull request whose commits the branch re-created the most of,
/// when any (only pull requests that change one of the branch's files are
/// read).
pub async fn copied(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base: &str,
    branch: &str,
) -> Option<Copy> {
    let ours_shas: Vec<String> = github
        .compare_commits(owner, repo, base, branch, MAX_COMMITS)
        .await
        .ok()?
        .iter()
        .filter_map(|c| c["sha"].as_str().map(String::from))
        .collect();
    if ours_shas.is_empty() {
        return None;
    }
    let ours = ids(github, owner, repo, &ours_shas).await;
    let our_files: BTreeSet<String> = github
        .compare_changed_files(owner, repo, base, branch)
        .await
        .ok()?
        .into_iter()
        .collect();
    let open = github.list_open_pulls(owner, repo).await.ok()?;
    let full = format!("{owner}/{repo}").to_ascii_lowercase();
    let mut best: Option<Copy> = None;
    for p in open.as_array()?.iter().take(MAX_PRS) {
        let head = p["head"]["ref"].as_str().unwrap_or("");
        let same_repo = p["head"]["repo"]["full_name"]
            .as_str()
            .is_some_and(|f| f.eq_ignore_ascii_case(&full));
        if head == branch || !same_repo {
            continue;
        }
        let n = p["number"].as_u64().unwrap_or(0);
        let touches = github
            .list_pull_files(owner, repo, n)
            .await
            .map(|f| f.iter().any(|(path, _)| our_files.contains(path)))
            .unwrap_or(false);
        if !touches {
            continue;
        }
        let their_shas: Vec<String> = github
            .list_pr_commits(owner, repo, n)
            .await
            .unwrap_or_default()
            .iter()
            .filter_map(|c| c["sha"].as_str().map(String::from))
            .collect();
        let theirs = ids(github, owner, repo, &their_shas).await;
        let m = matches(&ours, &theirs);
        if !m.is_empty() && best.as_ref().is_none_or(|b| m.len() > b.commits.len()) {
            best = Some(Copy {
                dep: Dependency {
                    number: n,
                    branch: head.to_string(),
                    title: p["title"].as_str().unwrap_or("").to_string(),
                    why: format!(
                        "The branch had re-created {} of its commits; it was rebased onto `{head}` \
                         so they come from #{n} itself.",
                        m.len()
                    ),
                },
                commits: m,
            });
        }
    }
    best
}

/// Before the pull request opens: a branch that copied another open pull
/// request's commits is rebased onto that pull request's branch, and
/// `base` becomes that branch. Returns the dependency to state in the pull
/// request, or why the run refuses (the copies couldn't be dropped).
pub async fn settle(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base: &mut String,
    branch: &str,
) -> Result<Option<Dependency>, String> {
    let Some(c) = copied(github, owner, repo, base, branch).await else {
        return Ok(None);
    };
    let short: Vec<String> = c
        .commits
        .iter()
        .map(|s| s[..7.min(s.len())].to_string())
        .collect();
    let refuse = |why: String| {
        format!(
            "the branch re-creates {} commit(s) of open pull request #{} (`{}`) under new hashes \
             ({}), and {why}. Build on #{}'s branch instead of copying its commits.",
            c.commits.len(),
            c.dep.number,
            c.dep.branch,
            short.join(", "),
            c.dep.number
        )
    };
    let lease = github
        .get_ref(owner, repo, branch)
        .await
        .map_err(|e| refuse(format!("its head couldn't be read ({e})")))?;
    match github
        .rebase_branch(owner, repo, branch, &c.dep.branch, &lease)
        .await
    {
        Ok(RebaseResult::Rebased(_)) | Ok(RebaseResult::UpToDate) => {
            *base = c.dep.branch.clone();
            Ok(Some(c.dep))
        }
        Ok(RebaseResult::NotClean(why)) => Err(refuse(format!(
            "rebasing onto that branch didn't apply cleanly ({why})"
        ))),
        Ok(RebaseResult::Moved) => Err(refuse(
            "the branch moved while it was being rebased".to_string(),
        )),
        Err(e) => Err(refuse(format!("rebasing onto that branch failed ({e})"))),
    }
}

/// The body with the stack section replaced (or added after the first
/// paragraph's line, the ticket line).
pub fn put_section(body: &str, section: &str) -> String {
    if let (Some(a), Some(b)) = (body.find(SECTION_START), body.find(SECTION_END)) {
        if b > a {
            return format!("{}{section}{}", &body[..a], &body[b + SECTION_END.len()..]);
        }
    }
    format!("{}\n\n{section}", body.trim_end())
}

/// States the dependency in the open pull request.
pub async fn note_pr(github: &GitHubClient, owner: &str, repo: &str, pr: u64, dep: &Dependency) {
    let Ok(p) = github.get_pull_request(owner, repo, pr).await else {
        return;
    };
    let body = p["body"].as_str().unwrap_or("");
    let updated = put_section(body, &dep.section());
    if updated != body {
        if let Err(e) = github.update_pr_body(owner, repo, pr, &updated).await {
            tracing::warn!(pr, error = %e, "Could not state the stacked pull request's dependency");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ticket_names_what_it_depends_on() {
        let text = "Replace the two flags with one field.\n\
                    Depends on https://github.com/acme/web/pull/41 (the schema work).\n\
                    Similar to #7.\n\
                    Blocked by ABC-12.";
        let (prs, keys) = named(text, "acme", "web", "ABC-99");
        assert_eq!(prs, vec![41]);
        assert_eq!(keys, vec!["ABC-12".to_string()]);
        // A mention without a dependency word is not a dependency.
        assert!(!prs.contains(&7));
        // The ticket's own key never is.
        assert!(named("builds on ABC-99", "acme", "web", "ABC-99")
            .1
            .is_empty());
        assert_eq!(named("This needs PR #5 first", "a", "b", "").0, vec![5]);
        assert!(named("Dependency injection #3", "a", "b", "").0.is_empty());
    }

    fn pr(n: u64, branch: &str, title: &str, repo: &str) -> Value {
        serde_json::json!({
            "number": n,
            "title": title,
            "head": {"ref": branch, "repo": {"full_name": repo}},
        })
    }

    #[test]
    fn the_dependency_is_an_open_pr_of_this_repo() {
        let open = vec![
            pr(41, "fork-branch", "Schema", "someone/web"),
            pr(42, "abc-12-schema", "ABC-12 schema work", "acme/web"),
            pr(43, "coderhelm/abc-99", "This ticket", "acme/web"),
        ];
        // #41 is a fork's branch: not a base.
        assert!(pick(&open, &[41], &[], "acme", "web", "coderhelm/abc-99").is_none());
        let d = pick(
            &open,
            &[],
            &["ABC-12".into()],
            "acme",
            "web",
            "coderhelm/abc-99",
        )
        .unwrap();
        assert_eq!((d.number, d.branch.as_str()), (42, "abc-12-schema"));
        let s = d.section();
        assert!(s.contains("Merge order: #42 first, then this one."));
        // The run's own branch is never its own dependency.
        assert!(pick(&open, &[43], &[], "acme", "web", "coderhelm/abc-99").is_none());
    }

    #[test]
    fn patch_ids_match_the_same_change_under_a_new_hash() {
        let a = vec![(
            "web/a.ts".to_string(),
            "@@ -1,3 +1,3 @@\n context\n-const x = 1\n+const x = 2\n".to_string(),
        )];
        let b = vec![(
            "web/a.ts".to_string(),
            "@@ -10,3 +12,3 @@\n other context\n-const x = 1\n+const  x = 2\n".to_string(),
        )];
        let c = vec![(
            "web/a.ts".to_string(),
            "@@ -1,3 +1,3 @@\n-const x = 1\n+const x = 3\n".to_string(),
        )];
        assert_eq!(patch_id(&a), patch_id(&b));
        assert_ne!(patch_id(&a), patch_id(&c));
        assert_eq!(patch_id(&[("x".into(), String::new())]), None);
        let ours = vec![
            ("s1".to_string(), patch_id(&a).unwrap()),
            ("s2".to_string(), patch_id(&c).unwrap()),
        ];
        let theirs = vec![("t1".to_string(), patch_id(&b).unwrap())];
        assert_eq!(matches(&ours, &theirs), vec!["s1".to_string()]);
    }

    #[test]
    fn the_section_is_replaced_in_place() {
        let d = Dependency {
            number: 4,
            branch: "b".into(),
            title: "t".into(),
            why: "w".into(),
        };
        let once = put_section("Body", &d.section());
        let twice = put_section(&once, &d.section());
        assert_eq!(once, twice);
    }
}
