//! The customer details guard: a public repository's pull request text never
//! carries details of an install CoderHelm serves.
//!
//! The terms come from CoderHelm's own records ([`records`]), read on the
//! server for each check and kept only in memory: the GitHub organizations
//! and repositories of every team on the install, their Jira sites and
//! project keys, Confluence sites and spaces, members' email domains, AWS
//! account ids, custom domains, the names of linked self-hosted installs and
//! the install's own organization name. Nobody keeps a list, and no term, hash
//! or digest of one is ever written anywhere: not in a repository, a log, a
//! check run or an error.
//!
//! A term is matched on word boundaries, ignoring case and the separators
//! between its words, and inside joined forms ("Acme Robots", "acme-robots",
//! "AcmeRobots" and "acmerobots" are one term). A term made only of common
//! words ([`is_generic`]), or of three characters or fewer, is skipped; a
//! Jira project key is matched as a ticket key (`KEY-123`) whatever its length.
//!
//! What the guard says in public is only where: "PR text contains a customer
//! detail" and the place (the title, a body line, a commit, a file and line).
//! It never names the term or its kind, and it never counts terms.
//!
//! Only a pull request whose head is the repository itself (never a fork)
//! and whose author is an organization member or CoderHelm's own app is
//! checked ([`trusted`]). Any other gets the same neutral "skipped" result
//! whatever its text says, so nobody outside can use the check to test
//! whether a name belongs to a customer.

pub mod records;

use std::collections::{HashMap, HashSet};
use std::fmt;

/// The check run's name. The CI gate and the merge refusal look it up by
/// this name, so it never changes.
pub const CHECK_NAME: &str = "Customer details guard";

/// The one line every failing location carries in public.
pub const FINDING: &str = "PR text contains a customer detail";

/// The output of a pull request the guard does not evaluate (a fork, or an
/// author outside the organization). The same for every such pull request.
pub const SKIPPED_TITLE: &str = "Skipped";
pub const SKIPPED_SUMMARY: &str =
    "Checked only for pull requests from a branch of this repository by a member of its organization.";

pub const PASSED_TITLE: &str = "No customer details";
pub const PASSED_SUMMARY: &str = "The title, the body, the commit messages and the added documentation and fixture lines carry no customer details.";

/// What a term is, for the guard's own logs (with the position, never the
/// text). The check run never shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    OrgName,
    RepoName,
    TicketHost,
    TicketKey,
    WikiHost,
    WikiSpace,
    ChatWorkspace,
    EmailDomain,
    CloudAccount,
    CustomDomain,
    TeamName,
    InstallName,
}

impl Kind {
    pub const ALL: [Kind; 12] = [
        Kind::OrgName,
        Kind::RepoName,
        Kind::TicketHost,
        Kind::TicketKey,
        Kind::WikiHost,
        Kind::WikiSpace,
        Kind::ChatWorkspace,
        Kind::EmailDomain,
        Kind::CloudAccount,
        Kind::CustomDomain,
        Kind::TeamName,
        Kind::InstallName,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Kind::OrgName => "customer org name",
            Kind::RepoName => "customer repo name",
            Kind::TicketHost => "ticket site host",
            Kind::TicketKey => "ticket key",
            Kind::WikiHost => "wiki site host",
            Kind::WikiSpace => "wiki space",
            Kind::ChatWorkspace => "chat workspace",
            Kind::EmailDomain => "member email domain",
            Kind::CloudAccount => "cloud account id",
            Kind::CustomDomain => "custom domain",
            Kind::TeamName => "team name",
            Kind::InstallName => "linked install name",
        }
    }
}

/// One team's identifiers, as read from its records. Nothing here is ever
/// logged or shown: its `Debug` prints only how many of each there are.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct TeamRecords {
    pub orgs: Vec<String>,
    pub repos: Vec<String>,
    pub jira_hosts: Vec<String>,
    pub project_keys: Vec<String>,
    pub wiki_hosts: Vec<String>,
    pub wiki_spaces: Vec<String>,
    pub workspaces: Vec<String>,
    pub member_emails: Vec<String>,
    pub aws_accounts: Vec<String>,
    pub custom_domains: Vec<String>,
    pub names: Vec<String>,
    pub install_names: Vec<String>,
}

impl fmt::Debug for TeamRecords {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TeamRecords {{ {} values }}", self.values().count())
    }
}

impl TeamRecords {
    fn values(&self) -> impl Iterator<Item = (Kind, &String)> {
        let groups: [(Kind, &Vec<String>); 12] = [
            (Kind::OrgName, &self.orgs),
            (Kind::RepoName, &self.repos),
            (Kind::TicketHost, &self.jira_hosts),
            (Kind::TicketKey, &self.project_keys),
            (Kind::WikiHost, &self.wiki_hosts),
            (Kind::WikiSpace, &self.wiki_spaces),
            (Kind::ChatWorkspace, &self.workspaces),
            (Kind::EmailDomain, &self.member_emails),
            (Kind::CloudAccount, &self.aws_accounts),
            (Kind::CustomDomain, &self.custom_domains),
            (Kind::TeamName, &self.names),
            (Kind::InstallName, &self.install_names),
        ];
        groups
            .into_iter()
            .flat_map(|(k, v)| v.iter().map(move |s| (k, s)))
    }
}

/// Every team of an install (on the hosted side, each linked self-hosted
/// install too). A team's `orgs` say which GitHub accounts it is, so the
/// pull request's own organization is recognised as itself, not as a
/// customer.
#[derive(Clone, Default)]
pub struct InstallRecords {
    pub teams: Vec<TeamRecords>,
    /// Names that belong to the install as a whole (its organization name,
    /// the hosts it is served on): the operator's own, so they count only
    /// on a repository of none of the install's teams.
    pub install: TeamRecords,
}

impl fmt::Debug for InstallRecords {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InstallRecords {{ {} teams }}", self.teams.len())
    }
}

/// One term, normalized: lowercase letters and digits only. Its `Debug`
/// shows only the kind.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Term {
    joined: String,
    kind: Kind,
    /// Matched as a ticket key (`KEY-123`) as well as a word.
    key: bool,
    /// Matched as a word (a key of three letters or fewer, or a common
    /// word, is matched only as a ticket key).
    word: bool,
}

impl fmt::Debug for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Term({})", self.kind.as_str())
    }
}

impl Term {
    pub fn kind(&self) -> Kind {
        self.kind
    }
}

/// The terms of an install, as the matcher looks them up. Its `Debug` shows
/// nothing of them, and nothing here is serializable.
#[derive(Clone, Default)]
pub struct TermSet {
    words: HashMap<String, Kind>,
    keys: HashMap<String, Kind>,
    /// The longest joined term, in bytes: how far a match looks ahead.
    longest: usize,
}

impl fmt::Debug for TermSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TermSet { .. }")
    }
}

impl TermSet {
    pub fn is_empty(&self) -> bool {
        self.words.is_empty() && self.keys.is_empty()
    }

    /// How many terms there are of each kind: for the read-only dry run of
    /// an install, which prints counts and never a term.
    pub fn counts(&self) -> Vec<(Kind, usize)> {
        let mut by: HashMap<Kind, usize> = HashMap::new();
        for k in self.words.values().chain(self.keys.values()) {
            *by.entry(*k).or_default() += 1;
        }
        let mut out: Vec<(Kind, usize)> = by.into_iter().collect();
        out.sort();
        out
    }

    fn add(&mut self, t: Term) {
        if t.word {
            self.longest = self.longest.max(t.joined.len());
            self.words.entry(t.joined.clone()).or_insert(t.kind);
        }
        if t.key {
            self.keys.entry(t.joined).or_insert(t.kind);
        }
    }
}

static COMMON_WORDS: &str = include_str!("common_words.txt");

fn common_words() -> &'static HashSet<&'static str> {
    static SET: std::sync::OnceLock<HashSet<&'static str>> = std::sync::OnceLock::new();
    SET.get_or_init(|| {
        COMMON_WORDS
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect()
    })
}

/// Pure: is this word in the common-words list.
pub fn is_common_word(word: &str) -> bool {
    common_words().contains(word)
}

/// The words of a text, lowercase: runs of letters and digits, split where a
/// lowercase letter meets an uppercase one and where letters meet digits
/// ("AcmeRobots2" is "acme", "robots", "2"). Each with its byte range.
pub fn words(text: &str) -> Vec<(usize, usize, String)> {
    let mut out = Vec::new();
    let mut start: Option<usize> = None;
    let mut prev: Option<char> = None;
    let flush = |s: usize, e: usize, out: &mut Vec<(usize, usize, String)>| {
        if e > s {
            out.push((s, e, text[s..e].to_lowercase()));
        }
    };
    for (i, c) in text.char_indices() {
        if c.is_alphanumeric() {
            let split = match prev {
                Some(p) if p.is_alphanumeric() => {
                    (p.is_lowercase() && c.is_uppercase())
                        || (p.is_alphabetic() && c.is_numeric())
                        || (p.is_numeric() && c.is_alphabetic())
                }
                _ => false,
            };
            match start {
                Some(s) if split => {
                    flush(s, i, &mut out);
                    start = Some(i);
                }
                None => start = Some(i),
                _ => {}
            }
        } else if let Some(s) = start.take() {
            flush(s, i, &mut out);
        }
        prev = Some(c);
    }
    if let Some(s) = start {
        flush(s, text.len(), &mut out);
    }
    out
}

fn joined(text: &str) -> String {
    words(text).into_iter().map(|(_, _, w)| w).collect()
}

/// Pure: is a term too common to be a customer detail. A term made of one
/// common word is; so is a repository name made only of common words ("web
/// app", "data pipeline"), which ordinary text says all the time. A name of
/// several words (an organization's, a team's) is distinctive as a whole and
/// stays.
pub fn is_generic(term: &str, kind: Kind) -> bool {
    let ws: Vec<String> = words(term).into_iter().map(|(_, _, w)| w).collect();
    let all = ws.concat();
    if all.len() <= 3 {
        return true;
    }
    if is_common_word(&all) {
        return true;
    }
    let all_common = ws
        .iter()
        .all(|w| w.len() <= 3 || is_common_word(w) || w.chars().all(|c| c.is_ascii_digit()));
    match kind {
        Kind::RepoName | Kind::WikiSpace => all_common,
        _ => false,
    }
}

/// The name a host is known by: the label before its public suffix
/// ("acme" of `acme.atlassian.net`, of `wiki.acme.co.uk`, of `acme.com`).
pub fn site_label(host: &str) -> Option<String> {
    let host = host
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let host = host.split(['/', ':', '?', '#']).next().unwrap_or("");
    let labels: Vec<&str> = host
        .trim_end_matches('.')
        .split('.')
        .filter(|l| !l.is_empty())
        .collect();
    if labels.len() < 2 {
        return None;
    }
    // Shared hosts: the customer is the leftmost label.
    let shared = ["atlassian.net", "jira.com", "coderhelm.com", "slack.com"];
    let tail2 = labels[labels.len() - 2..].join(".").to_ascii_lowercase();
    if shared.contains(&tail2.as_str()) && labels.len() >= 3 {
        return Some(labels[0].to_ascii_lowercase());
    }
    // A two-part public suffix (`co.uk`, `com.au`): one label further left.
    let n = labels.len();
    let idx = if n >= 3 && labels[n - 1].len() <= 3 && labels[n - 2].len() <= 3 {
        n - 3
    } else {
        n - 2
    };
    Some(labels[idx].to_ascii_lowercase())
}

fn host_of(value: &str) -> String {
    let v = value
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    v.split(['/', ':', '?', '#'])
        .next()
        .unwrap_or("")
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

fn push_term(out: &mut Vec<Term>, raw: &str, kind: Kind) {
    let j = joined(raw);
    if j.is_empty() {
        return;
    }
    if kind == Kind::TicketKey {
        // A project key: always a ticket key; a bare word only when it is
        // long enough and not a common word.
        let word = !is_generic(raw, kind);
        out.push(Term {
            joined: j,
            kind,
            key: true,
            word,
        });
        return;
    }
    if kind == Kind::CloudAccount {
        if j.len() == 12 && j.chars().all(|c| c.is_ascii_digit()) {
            out.push(Term {
                joined: j,
                kind,
                key: false,
                word: true,
            });
        }
        return;
    }
    if is_generic(raw, kind) {
        return;
    }
    out.push(Term {
        joined: j,
        kind,
        key: false,
        word: true,
    });
}

fn push_host(out: &mut Vec<Term>, raw: &str, kind: Kind) {
    let host = host_of(raw);
    if host.is_empty() {
        return;
    }
    let Some(label) = site_label(&host) else {
        push_term(out, &host, kind);
        return;
    };
    // A host on a common name (a public mail host, `github.com`) says
    // nothing about anyone.
    if is_generic(&label, kind) {
        return;
    }
    push_term(out, &host, kind);
    push_term(out, &label, kind);
}

/// Every term of one team's records, generic ones skipped.
pub fn team_terms(r: &TeamRecords) -> Vec<Term> {
    let mut out = Vec::new();
    for (kind, v) in r.values() {
        match kind {
            Kind::TicketHost | Kind::WikiHost | Kind::CustomDomain => push_host(&mut out, v, kind),
            Kind::EmailDomain => {
                if let Some((_, domain)) = v.rsplit_once('@') {
                    push_host(&mut out, domain, kind);
                }
            }
            Kind::RepoName => {
                // `owner/name` or a bare name: the name is the term.
                let name = v.rsplit('/').next().unwrap_or(v);
                push_term(&mut out, name, kind);
            }
            _ => push_term(&mut out, v, kind),
        }
    }
    out
}

/// Pure: the terms the guard looks for in a pull request. `own` are the
/// GitHub accounts the text may name as itself: the repository's owner and
/// the pull request's author. The team (or teams) that is one of them is
/// left out, with every term equal to one of its own: an organization or a
/// person naming itself is not a leak.
pub fn derive(records: &InstallRecords, own_accounts: &[&str]) -> TermSet {
    let accounts: HashSet<String> = own_accounts
        .iter()
        .map(|a| joined(a))
        .filter(|a| !a.is_empty())
        .collect();
    let is_self = |t: &TeamRecords| t.orgs.iter().any(|o| accounts.contains(&joined(o)));
    let mut own: HashSet<String> = accounts.clone();
    for t in records.teams.iter().filter(|t| is_self(t)) {
        for term in team_terms(t) {
            own.insert(term.joined);
        }
    }
    let mut set = TermSet::default();
    // The install's own names are its operator's: on a repository of one of
    // its teams they are that team's own.
    let any_self = records.teams.iter().any(is_self);
    let install = (!any_self).then_some(&records.install);
    let others = records.teams.iter().filter(|t| !is_self(t));
    for t in others.chain(install) {
        for term in team_terms(t) {
            if !own.contains(&term.joined) {
                set.add(term);
            }
        }
    }
    set
}

/// Where in the pull request a line is.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Place {
    Title,
    Body { line: usize },
    Commit { sha: String, line: usize },
    File { path: String, line: usize },
}

impl Place {
    /// The location as the check run says it.
    pub fn describe(&self) -> String {
        match self {
            Place::Title => "PR title".to_string(),
            Place::Body { line } => format!("PR body line {line}"),
            Place::Commit { sha, line } => {
                let short: String = sha.chars().take(7).collect();
                format!("commit {short} message line {line}")
            }
            Place::File { path, line } => format!("{path}:{line}"),
        }
    }
}

/// One text the guard reads: where it is, and its lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    Title(String),
    Body(String),
    Commit {
        sha: String,
        message: String,
    },
    /// Lines a file adds: (new line number, text).
    Added {
        path: String,
        lines: Vec<(usize, String)>,
    },
}

/// One line that carries a term: where, and (for the guard's own log only)
/// what kind. The term itself is never kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hit {
    pub place: Place,
    pub kind: Kind,
}

/// Pure: the first term in a line, if any.
pub fn find_in_line(line: &str, terms: &TermSet) -> Option<Kind> {
    if terms.is_empty() {
        return None;
    }
    let ws = words(line);
    for i in 0..ws.len() {
        // A ticket key: the key, a dash, digits.
        if let Some(kind) = terms.keys.get(&ws[i].2) {
            if let Some(next) = ws.get(i + 1) {
                let gap = &line[ws[i].1..next.0];
                if gap == "-" && next.2.chars().all(|c| c.is_ascii_digit()) {
                    return Some(*kind);
                }
            }
        }
        // A term: one or more whole words that join to it.
        let mut acc = String::new();
        for w in &ws[i..] {
            acc.push_str(&w.2);
            if acc.len() > terms.longest {
                break;
            }
            if let Some(kind) = terms.words.get(&acc) {
                return Some(*kind);
            }
        }
    }
    None
}

fn lines_of(text: &str) -> impl Iterator<Item = (usize, &str)> {
    text.lines().enumerate().map(|(i, l)| (i + 1, l))
}

/// Pure: every line of the sources that carries a term, in order, one hit
/// per line.
pub fn scan(sources: &[Source], terms: &TermSet) -> Vec<Hit> {
    let mut hits = Vec::new();
    let mut check = |place: Place, line: &str| {
        if let Some(kind) = find_in_line(line, terms) {
            hits.push(Hit { place, kind });
        }
    };
    for s in sources {
        match s {
            Source::Title(t) => {
                for (_, l) in lines_of(t) {
                    check(Place::Title, l);
                }
            }
            Source::Body(b) => {
                for (n, l) in lines_of(b) {
                    check(Place::Body { line: n }, l);
                }
            }
            Source::Commit { sha, message } => {
                for (n, l) in lines_of(message) {
                    check(
                        Place::Commit {
                            sha: sha.clone(),
                            line: n,
                        },
                        l,
                    );
                }
            }
            Source::Added { path, lines } => {
                for (n, l) in lines {
                    check(
                        Place::File {
                            path: path.clone(),
                            line: *n,
                        },
                        l,
                    );
                }
            }
        }
    }
    hits
}

/// Pure: is a changed file one whose added lines the guard reads: anything
/// under `docs/`, and test and incident fixtures.
pub fn reads_file(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    if p.starts_with("docs/") || p.contains("/docs/") {
        return true;
    }
    let segments: Vec<&str> = p.split('/').collect();
    let dir_hit = segments[..segments.len().saturating_sub(1)]
        .iter()
        .any(|s| {
            matches!(
                *s,
                "test"
                    | "tests"
                    | "testdata"
                    | "test_data"
                    | "fixtures"
                    | "fixture"
                    | "__fixtures__"
                    | "__tests__"
                    | "incidents"
                    | "goldens"
                    | "snapshots"
                    | "__snapshots__"
            ) || s.ends_with("_goldens")
                || s.ends_with("_cases")
        });
    if dir_hit {
        return true;
    }
    let file = segments.last().copied().unwrap_or("");
    let stem = file.split('.').next().unwrap_or("");
    stem.starts_with("test_")
        || stem.ends_with("_test")
        || stem.ends_with("_tests")
        || stem == "tests"
        || stem.contains("fixture")
        || file.contains(".test.")
        || file.contains(".spec.")
}

/// Pure: the added lines of a unified diff patch (GitHub's `patch` of one
/// file), with their new line numbers.
pub fn added_lines(patch: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut n = 0usize;
    for line in patch.lines() {
        if let Some(rest) = line.strip_prefix("@@") {
            // "@@ -a,b +c,d @@"
            n = rest
                .split_whitespace()
                .find_map(|p| p.strip_prefix('+'))
                .and_then(|p| p.split(',').next())
                .and_then(|p| p.parse().ok())
                .unwrap_or(0);
            continue;
        }
        if line.starts_with("+++") || line.starts_with("---") {
            continue;
        }
        if let Some(text) = line.strip_prefix('+') {
            out.push((n, text.to_string()));
            n += 1;
        } else if line.starts_with(' ') {
            n += 1;
        }
    }
    out
}

/// Who opened a pull request and from where, as GitHub reports it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Origin {
    /// `owner/name` of the repository the pull request is on.
    pub base_repo: String,
    /// `owner/name` of the head's repository; empty when GitHub has none (a
    /// deleted fork).
    pub head_repo: String,
    pub author_login: String,
    /// `User` or `Bot`.
    pub author_type: String,
    /// GitHub's `author_association` (`OWNER`, `MEMBER`, `COLLABORATOR`, ...).
    pub author_association: String,
    /// The author has write access or more to the repository, from GitHub's
    /// collaborator permission. GitHub shows an app an organization member
    /// whose membership is private as a plain contributor in
    /// `author_association`, so membership alone misses most members; write
    /// access is what lets someone push a branch to the repository at all.
    pub author_can_write: bool,
}

impl Origin {
    pub fn from_pr(pr: &serde_json::Value) -> Self {
        let s = |v: &serde_json::Value| v.as_str().unwrap_or("").to_string();
        Self {
            base_repo: s(&pr["base"]["repo"]["full_name"]),
            head_repo: s(&pr["head"]["repo"]["full_name"]),
            author_login: s(&pr["user"]["login"]),
            author_type: s(&pr["user"]["type"]),
            author_association: s(&pr["author_association"]),
            author_can_write: false,
        }
    }
}

/// Pure: is this CoderHelm's own app (its bot account).
pub fn is_own_app(login: &str, user_type: &str) -> bool {
    let l = login.to_ascii_lowercase();
    user_type == "Bot" && l.ends_with("[bot]") && l.starts_with("coderhelm")
}

/// Pure: may the guard evaluate this pull request's text. Only a branch of
/// the repository itself (never a fork), opened by an organization member
/// or owner, someone with write access to the repository, or CoderHelm's
/// own app. Anything else is skipped with one
/// fixed output, so the check is never an oracle for outsiders.
pub fn trusted(o: &Origin) -> bool {
    let member = matches!(o.author_association.as_str(), "OWNER" | "MEMBER");
    same_repo(o) && (member || o.author_can_write || is_own_app(&o.author_login, &o.author_type))
}

/// Pure: is the head a branch of the repository itself (never a fork).
pub fn same_repo(o: &Origin) -> bool {
    !o.head_repo.is_empty() && o.head_repo.eq_ignore_ascii_case(&o.base_repo)
}

/// Pure: does the guard check this repository's pull requests. A
/// self-hosted install serves one company, so every repository of it. The
/// multi-tenant hosted deployment checks only the repositories of the
/// account that owns its GitHub App (CoderHelm's own): a customer's
/// repository is never checked against other customers' names, so no
/// customer can use the check to learn who else is one.
pub fn checks_repo(self_hosted: bool, repo_owner: &str, app_owner: Option<&str>) -> bool {
    self_hosted || app_owner.is_some_and(|o| !o.is_empty() && o.eq_ignore_ascii_case(repo_owner))
}

/// The check run's conclusion and output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    /// `success`, `failure` or `neutral`.
    pub conclusion: &'static str,
    pub title: String,
    pub summary: String,
    /// File locations, for annotations: (path, line).
    pub annotations: Vec<(String, usize)>,
}

/// Pure: the result of a pull request the guard does not evaluate. Takes
/// nothing about the pull request, so it can't differ by its text.
pub fn skipped() -> Verdict {
    Verdict {
        conclusion: "neutral",
        title: SKIPPED_TITLE.to_string(),
        summary: SKIPPED_SUMMARY.to_string(),
        annotations: Vec::new(),
    }
}

/// Pure: the result from the hits: only where, never what or which kind.
pub fn verdict(hits: &[Hit]) -> Verdict {
    if hits.is_empty() {
        return Verdict {
            conclusion: "success",
            title: PASSED_TITLE.to_string(),
            summary: PASSED_SUMMARY.to_string(),
            annotations: Vec::new(),
        };
    }
    let mut places: Vec<&Place> = hits.iter().map(|h| &h.place).collect();
    places.dedup();
    let mut summary = String::from("Rewrite these lines in general terms, then save the edit or push the change; the check runs again by itself.\n\n");
    for p in &places {
        summary.push_str(&format!("- {}: {FINDING}\n", p.describe()));
    }
    let annotations = places
        .iter()
        .filter_map(|p| match p {
            Place::File { path, line } => Some((path.clone(), *line)),
            _ => None,
        })
        .collect();
    Verdict {
        conclusion: "failure",
        title: FINDING.to_string(),
        summary,
        annotations,
    }
}

/// Pure: the check run's request body for `head_sha`.
pub fn check_body(head_sha: &str, v: &Verdict) -> serde_json::Value {
    let annotations: Vec<serde_json::Value> = v
        .annotations
        .iter()
        .take(50)
        .map(|(path, line)| {
            serde_json::json!({
                "path": path,
                "start_line": line,
                "end_line": line,
                "annotation_level": "failure",
                "message": FINDING,
            })
        })
        .collect();
    let mut output = serde_json::json!({ "title": v.title, "summary": v.summary });
    if !annotations.is_empty() {
        output["annotations"] = serde_json::json!(annotations);
    }
    serde_json::json!({
        "name": CHECK_NAME,
        "head_sha": head_sha,
        "status": "completed",
        "conclusion": v.conclusion,
        "output": output,
    })
}

/// What the guard's latest check run on a commit says, for the merge
/// refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardState {
    /// No check run of the guard on the commit.
    Missing,
    /// Queued or in progress.
    Running,
    Failing,
    /// Passed, or skipped (a fork's pull request).
    Passing,
}

/// Pure: the guard's state from a `check-runs` listing of a commit (the
/// newest of its runs counts: an edit's run replaces the push's).
pub fn guard_state(runs: &[serde_json::Value]) -> GuardState {
    let latest = runs
        .iter()
        .filter(|r| r["name"].as_str() == Some(CHECK_NAME))
        .max_by(|a, b| {
            let ka = (
                a["started_at"].as_str().unwrap_or(""),
                a["id"].as_u64().unwrap_or(0),
            );
            let kb = (
                b["started_at"].as_str().unwrap_or(""),
                b["id"].as_u64().unwrap_or(0),
            );
            ka.cmp(&kb)
        });
    let Some(r) = latest else {
        return GuardState::Missing;
    };
    if r["status"].as_str() != Some("completed") {
        return GuardState::Running;
    }
    match r["conclusion"].as_str().unwrap_or("") {
        "success" | "neutral" | "skipped" => GuardState::Passing,
        _ => GuardState::Failing,
    }
}

/// How long a guarded head may wait for the guard's first run before a
/// merge goes ahead without it: longer than the guard's own retries take, so
/// only a guard that never runs at all (it is not set up for the
/// installation, say) stops holding merges. CI then checks the PR with its
/// own rules (`.github/scripts/customer_guard_gate.py`).
pub const MISSING_GRACE_SECS: i64 = 60 * 60;

/// Pure: why CoderHelm won't merge, from the guard's state on the head,
/// whether the guard checks this pull request (a public repository it
/// [`checks_repo`]), and how long ago the head was committed (`None`:
/// unknown). None: the guard doesn't hold it back. A failing or running
/// guard always holds it; a missing one only within [`MISSING_GRACE_SECS`].
pub fn merge_refusal(
    state: GuardState,
    guarded: bool,
    head_age_secs: Option<i64>,
) -> Option<&'static str> {
    match state {
        GuardState::Failing => {
            Some("Customer details guard is failing: the PR text carries a customer detail")
        }
        GuardState::Running => Some("Customer details guard is still running"),
        GuardState::Missing if guarded && head_age_secs.is_none_or(|a| a < MISSING_GRACE_SECS) => {
            Some("Customer details guard has not run on this commit yet")
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests;
