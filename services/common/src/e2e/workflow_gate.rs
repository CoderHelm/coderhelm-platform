//! Why a check was skipped on a pull request's head, read from its
//! workflow file: the job runs only on a pull request that isn't a draft,
//! only with a label, or for a reason CoderHelm can't read.
//!
//! A failing-test fix once stayed a draft waiting for its staging deploy
//! and staging end-to-end checks to go green. Those jobs run only when the
//! pull request is not a draft (`github.event.pull_request.draft == false`),
//! so on the draft they were skipped, and the draft waited on them every ten
//! minutes for twelve hours: draft, so skipped, so still a draft.
//!
//! Later the same reading took a prefix test
//! (`startsWith(github.event.label.name, 'E2E:')`) for a label named `E2E:`,
//! and CoderHelm added that label, which matched no real label and so was
//! created in the repository. It also added a label that, on its own, could
//! not run the skipped job: the job's condition needs that label *and* the
//! staging label.
//!
//! The rules:
//!
//! - [`read`] reads each job's `if:` as an expression (multi-line `>-`
//!   blocks included) into the label combinations that run it
//!   ([`LabelWay`]): every label a way needs, and every label that stops it.
//!   A job that `needs:` another is skipped when that one is, so it asks for
//!   that one's labels too. Labels are patterns: `X` is a label named
//!   exactly `X`, `X*` one starting with `X` (and longer), `*X*` one
//!   containing `X`, `*X` one ending with `X`. A prefix is never a label.
//! - [`sort`] tells, for each check skipped on the head, why: a skipped
//!   check on a draft whose workflow is draft gated runs once the pull
//!   request is marked ready ([`Skipped::draft_gated`]); one whose job asks
//!   for labels the pull request lacks runs once they are added; any other
//!   is unknown.
//! - [`resolve`] turns a pattern into the one existing repository label it
//!   means, or nothing when that isn't certain; [`choose`] picks the labels
//!   CoderHelm may add (each one alone runs a skipped check, exists in the
//!   repository, and is never a deploy, staging or production label, nor
//!   added next to one).
//! - [`Skipped::action`] is the one plain thing a person can do when
//!   CoderHelm can't start them itself.

use serde::{Deserialize, Serialize};

/// One combination of labels that runs a job: every pattern in `need` is
/// on the pull request (or is the label just added) and none in `forbid` is.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelWay {
    #[serde(default)]
    pub need: Vec<String>,
    #[serde(default)]
    pub forbid: Vec<String>,
}

/// One job's label conditions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobGate {
    /// The job's key under `jobs:`.
    pub id: String,
    /// Its `name:`, when it has one (a check is named after it).
    #[serde(default)]
    pub name: String,
    /// The label combinations that run it (with those of the jobs it
    /// needs). Empty: it asks for no label.
    #[serde(default)]
    pub ways: Vec<LabelWay>,
    /// Its condition couldn't be read into label combinations.
    #[serde(default)]
    pub unreadable: bool,
}

/// What a workflow file needs before its jobs run on a pull request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowGate {
    /// Its jobs (or some of them) run only when the pull request is not a
    /// draft.
    #[serde(default)]
    pub draft: bool,
    /// Each job's label conditions.
    #[serde(default)]
    pub jobs: Vec<JobGate>,
    /// It starts when a pull request is marked ready (`ready_for_review` in
    /// its `pull_request` types). Without it, a job that skips drafts stays
    /// skipped after the pull request is marked ready, until the next push.
    #[serde(default)]
    pub on_ready: bool,
}

impl WorkflowGate {
    /// Every label pattern any job asks for, in order.
    pub fn labels(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for j in &self.jobs {
            for w in &j.ways {
                for p in &w.need {
                    if !out.iter().any(|o| o.eq_ignore_ascii_case(p)) {
                        out.push(p.clone());
                    }
                }
            }
        }
        out
    }

    /// The job a check is named after: its `name` (or key), a matrix
    /// variant (`test (a)`), or a reusable workflow's job (`deploy / e2e`).
    pub fn job_for(&self, check: &str) -> Option<&JobGate> {
        let c = check.trim().to_ascii_lowercase();
        self.jobs.iter().find(|j| {
            let n = if j.name.is_empty() { &j.id } else { &j.name };
            let n = n.trim().to_ascii_lowercase();
            if let Some(i) = n.find("${{") {
                let stem = n[..i].trim_end();
                return !stem.is_empty() && c.starts_with(stem);
            }
            c == n || c.starts_with(&format!("{n} / ")) || c.starts_with(&format!("{n} ("))
        })
    }
}

const DRAFT_CONDITIONS: &[&str] = &[
    "pull_request.draft==false",
    "pull_request.draft=='false'",
    "pull_request.draft!=true",
    "false==github.event.pull_request.draft",
    "!github.event.pull_request.draft",
    "!(github.event.pull_request.draft)",
];

/// Ways past this many are not read: the condition is treated as
/// unreadable rather than guessed at.
const MAX_WAYS: usize = 64;

/// The gates of one workflow file, from its text. Never fails: an
/// unreadable file has no gates.
pub fn read(yaml: &str) -> WorkflowGate {
    let mut g = WorkflowGate::default();
    for line in yaml.lines() {
        let line = line.split(" #").next().unwrap_or(line);
        let compact: String = line.chars().filter(|c| !c.is_whitespace()).collect();
        let lower = compact.to_ascii_lowercase();
        if DRAFT_CONDITIONS.iter().any(|c| lower.contains(c)) {
            g.draft = true;
        }
    }
    g.jobs = jobs(yaml);
    let types = pull_request_types(yaml);
    g.on_ready = types.iter().any(|t| t == "ready_for_review");
    // Started only when marked ready: it never starts on a draft's push.
    if g.on_ready
        && !types
            .iter()
            .any(|t| matches!(t.as_str(), "opened" | "reopened" | "synchronize"))
    {
        g.draft = true;
    }
    g
}

/// Each job's label ways, with those of the jobs it needs.
fn jobs(yaml: &str) -> Vec<JobGate> {
    let Ok(v) = serde_yaml::from_str::<serde_yaml::Value>(yaml) else {
        return Vec::new();
    };
    let Some(map) = v.get("jobs").and_then(|j| j.as_mapping()) else {
        return Vec::new();
    };
    // (id, name, own ways or None when unreadable, needs, runs whatever its
    // needs did)
    type Raw = (String, String, Option<Vec<LabelWay>>, Vec<String>, bool);
    let mut raw: Vec<Raw> = Vec::new();
    for (k, job) in map {
        let Some(id) = k.as_str() else { continue };
        let name = job
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("")
            .to_string();
        let cond = match job.get("if") {
            Some(serde_yaml::Value::String(s)) => s.clone(),
            Some(serde_yaml::Value::Bool(b)) => b.to_string(),
            _ => String::new(),
        };
        let needs: Vec<String> = match job.get("needs") {
            Some(serde_yaml::Value::String(s)) => vec![s.clone()],
            Some(serde_yaml::Value::Sequence(s)) => s
                .iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect(),
            _ => Vec::new(),
        };
        let lower = cond.to_ascii_lowercase();
        let always = ["always()", "cancelled()", "failure()"]
            .iter()
            .any(|f| lower.contains(f));
        raw.push((id.to_string(), name, ways_of(&cond), needs, always));
    }
    fn effective(raw: &[Raw], id: &str, depth: usize) -> Option<Vec<LabelWay>> {
        let Some((_, _, own, needs, always)) = raw.iter().find(|r| r.0 == id) else {
            return Some(Vec::new());
        };
        let mut ways = own.clone()?;
        if *always || depth > 8 {
            return Some(ways);
        }
        for n in needs {
            let theirs = effective(raw, n, depth + 1)?;
            ways = and_ways(&ways, &theirs)?;
        }
        Some(ways)
    }
    raw.iter()
        .map(|(id, name, _, _, _)| match effective(&raw, id, 0) {
            Some(ways) => JobGate {
                id: id.clone(),
                name: name.clone(),
                ways,
                unreadable: false,
            },
            None => JobGate {
                id: id.clone(),
                name: name.clone(),
                ways: Vec::new(),
                unreadable: true,
            },
        })
        .collect()
}

/// Both sets of ways at once. Either side empty asks for no label, so the
/// other side stands.
fn and_ways(a: &[LabelWay], b: &[LabelWay]) -> Option<Vec<LabelWay>> {
    if a.is_empty() {
        return Some(b.to_vec());
    }
    if b.is_empty() {
        return Some(a.to_vec());
    }
    let mut out = Vec::new();
    for x in a {
        for y in b {
            out.push(merge(x, y));
            if out.len() > MAX_WAYS {
                return None;
            }
        }
    }
    Some(out)
}

fn merge(x: &LabelWay, y: &LabelWay) -> LabelWay {
    let mut w = x.clone();
    for p in &y.need {
        if !w.need.iter().any(|q| q.eq_ignore_ascii_case(p)) {
            w.need.push(p.clone());
        }
    }
    for p in &y.forbid {
        if !w.forbid.iter().any(|q| q.eq_ignore_ascii_case(p)) {
            w.forbid.push(p.clone());
        }
    }
    w
}

/// The label ways of one `if:` condition: only ways that need a label.
/// A way that needs none was false on the skipped run for some other
/// reason, which adding a label doesn't change. None when unreadable.
pub fn ways_of(cond: &str) -> Option<Vec<LabelWay>> {
    let mut s = cond.trim();
    if let Some(inner) = s.strip_prefix("${{").and_then(|x| x.strip_suffix("}}")) {
        s = inner.trim();
    }
    if s.is_empty() {
        return Some(Vec::new());
    }
    let mut p = Parser {
        s: s.as_bytes(),
        src: s,
        i: 0,
    };
    let e = p.or()?;
    p.ws();
    if p.i < p.s.len() {
        return None;
    }
    let ways = dnf(&e, false)?;
    let mut out: Vec<LabelWay> = Vec::new();
    for w in ways.into_iter().filter(|w| !w.need.is_empty()) {
        if !out.contains(&w) {
            out.push(w);
        }
    }
    Some(out)
}

#[derive(Debug, Clone, PartialEq)]
enum Expr {
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    /// A label pattern the condition asks for.
    Label(String),
    /// Something other than a label.
    Other,
}

struct Parser<'a> {
    s: &'a [u8],
    src: &'a str,
    i: usize,
}

impl Parser<'_> {
    fn ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }

    fn eat(&mut self, t: &str) -> bool {
        self.ws();
        if self.src[self.i..].starts_with(t) {
            self.i += t.len();
            true
        } else {
            false
        }
    }

    fn or(&mut self) -> Option<Expr> {
        let mut xs = vec![self.and()?];
        while self.eat("||") {
            xs.push(self.and()?);
        }
        Some(if xs.len() == 1 {
            xs.remove(0)
        } else {
            Expr::Or(xs)
        })
    }

    fn and(&mut self) -> Option<Expr> {
        let mut xs = vec![self.unary()?];
        while self.eat("&&") {
            xs.push(self.unary()?);
        }
        Some(if xs.len() == 1 {
            xs.remove(0)
        } else {
            Expr::And(xs)
        })
    }

    fn unary(&mut self) -> Option<Expr> {
        self.ws();
        if self.src[self.i..].starts_with('!') && !self.src[self.i..].starts_with("!=") {
            self.i += 1;
            return Some(Expr::Not(Box::new(self.unary()?)));
        }
        if self.src[self.i..].starts_with('(') {
            // A group, unless it is followed by a comparison (`(a) == b`),
            // which is read as one term.
            let save = self.i;
            self.i += 1;
            if let Some(e) = self.or() {
                if self.eat(")") {
                    let after = self.i;
                    self.ws();
                    let rest = &self.src[self.i..];
                    if !(rest.starts_with("==") || rest.starts_with("!=")) {
                        self.i = after;
                        return Some(e);
                    }
                }
            }
            self.i = save;
        }
        self.atom()
    }

    /// One term: up to `&&`, `||` or an unmatched `)`, outside quotes.
    fn atom(&mut self) -> Option<Expr> {
        self.ws();
        let start = self.i;
        let mut depth = 0i32;
        let mut quote: Option<u8> = None;
        while self.i < self.s.len() {
            let c = self.s[self.i];
            if let Some(q) = quote {
                if c == q {
                    quote = None;
                }
                self.i += 1;
                continue;
            }
            match c {
                b'\'' | b'"' => quote = Some(c),
                b'(' => depth += 1,
                b')' if depth == 0 => break,
                b')' => depth -= 1,
                b'&' | b'|' if depth == 0 && self.s.get(self.i + 1) == Some(&c) => break,
                _ => {}
            }
            self.i += 1;
        }
        if quote.is_some() || depth != 0 {
            return None;
        }
        let text = self.src[start..self.i].trim();
        if text.is_empty() {
            return None;
        }
        Some(term(text))
    }
}

/// What one term asks of the labels.
fn term(text: &str) -> Expr {
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    let lower = compact.to_ascii_lowercase();
    let values = quoted(text);
    let first = || values.first().cloned().unwrap_or_default();
    if lower.contains("labels.*.name") {
        // The text compared comes last (`join(…, ','), 'E2E:'`).
        let v = values.last().cloned().unwrap_or_default();
        if v.is_empty() {
            return Expr::Other;
        }
        // Membership in the label list is an exact name; any test on the
        // joined (or JSON) text matches part of a name.
        if lower.starts_with("contains(github.event.pull_request.labels.*.name,")
            || lower.starts_with("contains(github.event.issue.labels.*.name,")
        {
            return Expr::Label(v);
        }
        return Expr::Label(format!("*{v}*"));
    }
    if lower.contains("event.label.name") {
        // `contains(fromJSON('["a","b"]'), github.event.label.name)`
        if lower.contains("fromjson(") {
            let names: Vec<String> = serde_json::from_str::<Vec<String>>(&first())
                .unwrap_or_default()
                .into_iter()
                .filter(|n| !n.is_empty())
                .collect();
            if names.is_empty() {
                return Expr::Other;
            }
            return Expr::Or(names.into_iter().map(Expr::Label).collect());
        }
        let v = first();
        if v.is_empty() {
            return Expr::Other;
        }
        if lower.starts_with("startswith(") {
            return Expr::Label(format!("{v}*"));
        }
        if lower.starts_with("endswith(") {
            return Expr::Label(format!("*{v}"));
        }
        if lower.starts_with("contains(") {
            return Expr::Label(format!("*{v}*"));
        }
        if lower.contains("!=") {
            return Expr::Not(Box::new(Expr::Label(v)));
        }
        if lower.contains("==") {
            return Expr::Label(v);
        }
    }
    Expr::Other
}

/// The ways (in disjunctive form) of an expression, `neg` when it is
/// negated. None past [`MAX_WAYS`].
fn dnf(e: &Expr, neg: bool) -> Option<Vec<LabelWay>> {
    match e {
        Expr::Other => Some(vec![LabelWay::default()]),
        Expr::Label(p) => Some(vec![if neg {
            LabelWay {
                forbid: vec![p.clone()],
                ..Default::default()
            }
        } else {
            LabelWay {
                need: vec![p.clone()],
                ..Default::default()
            }
        }]),
        Expr::Not(x) => dnf(x, !neg),
        Expr::And(xs) | Expr::Or(xs) => {
            let conj = matches!(e, Expr::And(_)) != neg;
            let mut acc: Option<Vec<LabelWay>> = None;
            for x in xs {
                let w = dnf(x, neg)?;
                acc = Some(match acc {
                    None => w,
                    Some(a) if conj => {
                        let mut out = Vec::new();
                        for p in &a {
                            for q in &w {
                                out.push(merge(p, q));
                            }
                        }
                        out
                    }
                    Some(mut a) => {
                        a.extend(w);
                        a
                    }
                });
                if acc.as_ref().is_some_and(|a| a.len() > MAX_WAYS) {
                    return None;
                }
            }
            acc
        }
    }
}

/// The `types` of the workflow's `pull_request` (and `pull_request_target`)
/// triggers; empty when it lists none (GitHub's default then leaves out
/// `ready_for_review`).
fn pull_request_types(yaml: &str) -> Vec<String> {
    let Ok(v) = serde_yaml::from_str::<serde_yaml::Value>(yaml) else {
        return Vec::new();
    };
    let Some(on) = v.get("on").or_else(|| v.get(serde_yaml::Value::Bool(true))) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for ev in ["pull_request", "pull_request_target"] {
        for t in on
            .get(ev)
            .and_then(|e| e.get("types"))
            .and_then(|t| t.as_sequence())
            .into_iter()
            .flatten()
        {
            if let Some(t) = t.as_str() {
                out.push(t.to_string());
            }
        }
    }
    out
}

/// The quoted strings in a condition fragment, in order.
fn quoted(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = s.char_indices();
    while let Some((i, c)) = chars.next() {
        if c != '\'' && c != '"' {
            continue;
        }
        let rest = &s[i + 1..];
        match rest.find(c) {
            Some(j) => {
                out.push(rest[..j].to_string());
                for _ in 0..=j {
                    chars.next();
                }
            }
            None => break,
        }
    }
    out
}

/// Whether `label` matches a pattern. A prefix (`E2E:*`) or suffix
/// pattern needs more than the fixed part: `E2E:` itself is not an
/// `E2E:` label. Case-insensitive, as GitHub compares them.
pub fn matches(pattern: &str, label: &str) -> bool {
    let (p, l) = (pattern.to_ascii_lowercase(), label.to_ascii_lowercase());
    match (p.strip_prefix('*'), p.strip_suffix('*')) {
        (Some(_), Some(_)) if p.len() >= 2 => l.contains(&p[1..p.len() - 1]),
        (None, Some(prefix)) => l.len() > prefix.len() && l.starts_with(prefix),
        (Some(suffix), None) => l.len() > suffix.len() && l.ends_with(suffix),
        _ => p == l,
    }
}

/// A pattern that names one label exactly (no `*`).
pub fn is_exact(pattern: &str) -> bool {
    !pattern.starts_with('*') && !pattern.ends_with('*')
}

/// Text that reads as the start of a label rather than a whole one: it
/// ends with a separator (`E2E:`, `area/`, `ci-`).
pub fn is_bare_prefix(name: &str) -> bool {
    name.trim().ends_with([':', '/', '-', '_', '.', ' '])
}

/// Deploy, staging and production labels cost a shared environment: never
/// added by CoderHelm, and nothing is added next to one.
pub fn is_deploy_label(name: &str) -> bool {
    let l = name.to_ascii_lowercase();
    ["prod", "deploy", "staging"].iter().any(|w| l.contains(w))
}

/// The one existing repository label a pattern means, or None when that
/// isn't certain. An exact pattern is a label of that name; a contains
/// pattern whose text is a whole name (`*CI:DEPLOY_STAGING*`) is that
/// label; otherwise the one repository label it matches, or, among
/// several, the one `preferred` (the team's settings) names. A prefix is
/// never a label, and neither is a bare prefix that exists only because it
/// was once added by mistake.
pub fn resolve(pattern: &str, repo_labels: &[String], preferred: &[String]) -> Option<String> {
    let find = |name: &str| {
        repo_labels
            .iter()
            .find(|l| l.eq_ignore_ascii_case(name))
            .cloned()
    };
    if is_exact(pattern) {
        return find(pattern).filter(|l| !is_bare_prefix(l));
    }
    if let Some(inner) = pattern
        .strip_prefix('*')
        .and_then(|p| p.strip_suffix('*'))
        .filter(|i| !i.is_empty() && !is_bare_prefix(i))
    {
        if let Some(l) = find(inner) {
            return Some(l);
        }
    }
    let hits: Vec<&String> = repo_labels
        .iter()
        .filter(|l| !is_bare_prefix(l) && matches(pattern, l))
        .collect();
    match hits.as_slice() {
        [one] => Some((*one).clone()),
        [] => None,
        many => {
            let named: Vec<&&String> = many
                .iter()
                .filter(|l| preferred.iter().any(|p| p.eq_ignore_ascii_case(l)))
                .collect();
            match named.as_slice() {
                [one] => Some((**one).clone()),
                _ => None,
            }
        }
    }
}

/// A check skipped because labels are missing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelGated {
    pub check: String,
    /// Each of these alone runs it (patterns, or the real labels they
    /// resolved to).
    #[serde(default)]
    pub any: Vec<String>,
    /// When no one label does: these together.
    #[serde(default)]
    pub all: Vec<String>,
    /// The check's own job was found in its workflow. When it wasn't, the
    /// labels are the workflow's, and CoderHelm only names them.
    #[serde(default)]
    pub own: bool,
}

/// The checks skipped on a head, sorted by why.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Skipped {
    /// Skipped because the pull request is a draft: they run once it is
    /// marked ready.
    pub draft_gated: Vec<String>,
    /// Skipped because the pull request is a draft, in a workflow that
    /// doesn't start when it is marked ready: they run on the next push
    /// after it is ready.
    pub draft_no_start: Vec<String>,
    /// Skipped because labels are missing.
    pub label_gated: Vec<LabelGated>,
    /// Skipped for a reason CoderHelm can't read.
    pub unknown: Vec<String>,
}

/// Whether a label on the pull request matches a pattern.
fn on(on_pr: &[String], pattern: &str) -> bool {
    on_pr.iter().any(|l| matches(pattern, l))
}

/// The label ways a check is gated by: its job's, else (job not found) the
/// workflow's, with whether they are its own. None: unreadable.
fn ways_for(g: &WorkflowGate, check: &str) -> Option<(Vec<LabelWay>, bool)> {
    match g.job_for(check) {
        Some(j) if j.unreadable => None,
        Some(j) => Some((j.ways.clone(), true)),
        None => {
            if g.jobs.iter().any(|j| j.unreadable) {
                return None;
            }
            Some((g.jobs.iter().flat_map(|j| j.ways.clone()).collect(), false))
        }
    }
}

/// Sorts skipped checks by why they were skipped. `skipped` holds each
/// check with its workflow's gate, `None` when the workflow couldn't be
/// read; `on_pr` is the pull request's labels. A check whose job needs a
/// label the pull request lacks is label-gated even on a draft: marking it
/// ready alone wouldn't run it. A draft-gated check skipped on a pull
/// request that is ready now was skipped while it was a draft.
pub fn sort(skipped: &[(String, Option<WorkflowGate>)], on_pr: &[String]) -> Skipped {
    let mut s = Skipped::default();
    for (name, gate) in skipped {
        let Some(g) = gate else {
            s.unknown.push(name.clone());
            continue;
        };
        let Some((ways, own)) = ways_for(g, name) else {
            s.unknown.push(name.clone());
            continue;
        };
        if !ways.is_empty() {
            let live: Vec<&LabelWay> = ways
                .iter()
                .filter(|w| !w.forbid.iter().any(|f| on(on_pr, f)))
                .collect();
            let missing: Vec<Vec<String>> = live
                .iter()
                .map(|w| w.need.iter().filter(|p| !on(on_pr, p)).cloned().collect())
                .collect();
            if live.is_empty() {
                // A label on the pull request stops it.
                s.unknown.push(name.clone());
                continue;
            }
            if !missing.iter().any(|m| m.is_empty()) {
                let mut any: Vec<String> = Vec::new();
                for m in missing.iter().filter(|m| m.len() == 1) {
                    if !any.iter().any(|a| a.eq_ignore_ascii_case(&m[0])) {
                        any.push(m[0].clone());
                    }
                }
                let all = if any.is_empty() {
                    missing
                        .iter()
                        .min_by_key(|m| m.len())
                        .cloned()
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };
                s.label_gated.push(LabelGated {
                    check: name.clone(),
                    any,
                    all,
                    own,
                });
                continue;
            }
        }
        if g.draft && g.on_ready {
            s.draft_gated.push(name.clone());
        } else if g.draft {
            s.draft_no_start.push(name.clone());
        } else {
            s.unknown.push(name.clone());
        }
    }
    s
}

/// The labels CoderHelm may add to run label-gated checks, as existing
/// repository labels: for each check whose own job it read, the one label
/// that alone runs it. Nothing for a check where that isn't one certain
/// label, nothing that is a deploy, staging or production label, and
/// nothing at all while the pull request carries one (a second labeled
/// event next to a deploy label has cancelled a deploy before). The team's
/// settings decide after this (`review_pr::may_add_label`).
pub fn choose(
    s: &Skipped,
    repo_labels: &[String],
    preferred: &[String],
    on_pr: &[String],
) -> Vec<String> {
    if on_pr.iter().any(|l| is_deploy_label(l)) {
        return Vec::new();
    }
    let mut out: Vec<String> = Vec::new();
    for g in s.label_gated.iter().filter(|g| g.own) {
        let mut real: Vec<String> = Vec::new();
        for p in &g.any {
            if let Some(l) = resolve(p, repo_labels, preferred) {
                if !real.iter().any(|r| r.eq_ignore_ascii_case(&l)) {
                    real.push(l);
                }
            }
        }
        if let [one] = real.as_slice() {
            if !is_deploy_label(one) && !out.iter().any(|o| o.eq_ignore_ascii_case(one)) {
                out.push(one.clone());
            }
        }
    }
    out
}

/// Labels CoderHelm added earlier (`ours`) that it takes off again: a bare
/// prefix (`E2E:`), a label the repository doesn't have, or one a skipped
/// check's job mentions but that, by [`choose`] on the pull request
/// without CoderHelm's labels, isn't the label that runs it (it was added
/// by the reading that took a combination for a choice). A label no
/// skipped check mentions did its job and stays.
pub fn to_take_off(
    rows: &[(String, Option<WorkflowGate>)],
    on_pr: &[String],
    ours: &[String],
    repo_labels: &[String],
    preferred: &[String],
) -> Vec<String> {
    let ours_on: Vec<&String> = ours
        .iter()
        .filter(|o| on_pr.iter().any(|l| l.eq_ignore_ascii_case(o)))
        .collect();
    if ours_on.is_empty() {
        return Vec::new();
    }
    let without: Vec<String> = on_pr
        .iter()
        .filter(|l| !ours.iter().any(|o| o.eq_ignore_ascii_case(l)))
        .cloned()
        .collect();
    let chosen = choose(&sort(rows, &without), repo_labels, preferred, &without);
    let mentioned = |l: &str| {
        rows.iter().any(|(name, g)| {
            g.as_ref()
                .and_then(|g| ways_for(g, name))
                .is_some_and(|(ways, _)| ways.iter().any(|w| w.need.iter().any(|p| matches(p, l))))
        })
    };
    let mut out: Vec<String> = Vec::new();
    for l in ours_on {
        let junk = is_bare_prefix(l) || !repo_labels.iter().any(|r| r.eq_ignore_ascii_case(l));
        let unneeded = mentioned(l) && !chosen.iter().any(|c| c.eq_ignore_ascii_case(l));
        if (junk || unneeded) && !out.iter().any(|o| o.eq_ignore_ascii_case(l)) {
            out.push(l.clone());
        }
    }
    out
}

fn list(v: &[String]) -> String {
    v.iter()
        .map(|n| format!("`{n}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A pattern as a person reads it: `E2E:…` for a prefix.
fn shown(p: &str) -> String {
    match (p.strip_prefix('*'), p.strip_suffix('*')) {
        (Some(_), Some(_)) if p.len() >= 2 => format!("`…{}…`", &p[1..p.len() - 1]),
        (None, Some(prefix)) => format!("`{prefix}…`"),
        (Some(suffix), None) => format!("`…{suffix}`"),
        _ => format!("`{p}`"),
    }
}

impl Skipped {
    /// Every skipped check runs once the pull request is marked ready.
    pub fn all_draft_gated(&self) -> bool {
        !self.draft_gated.is_empty()
            && self.draft_no_start.is_empty()
            && self.label_gated.is_empty()
            && self.unknown.is_empty()
    }

    /// The labels that would run the label-gated checks (any one of them,
    /// or all of a combination).
    pub fn labels(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for g in &self.label_gated {
            for l in g.any.iter().chain(&g.all) {
                if !out.iter().any(|o| o.eq_ignore_ascii_case(l)) {
                    out.push(l.clone());
                }
            }
        }
        out
    }

    /// Names each label pattern by the one repository label it means, when
    /// that is certain, so the action names a real label.
    pub fn resolve_labels(&mut self, repo_labels: &[String], preferred: &[String]) {
        let fix = |v: &mut Vec<String>| {
            let mut out: Vec<String> = Vec::new();
            for p in v.iter() {
                let l = resolve(p, repo_labels, preferred).unwrap_or_else(|| p.clone());
                if !out.iter().any(|o| o.eq_ignore_ascii_case(&l)) {
                    out.push(l);
                }
            }
            *v = out;
        };
        for g in &mut self.label_gated {
            fix(&mut g.any);
            fix(&mut g.all);
        }
    }

    /// Drops the checks a label just added runs by itself.
    pub fn labels_added(&mut self, added: &[String]) {
        self.label_gated.retain(|g| {
            !g.any.iter().any(|p| {
                added
                    .iter()
                    .any(|a| a.eq_ignore_ascii_case(p) || matches(p, a))
            })
        });
    }

    /// What a person can do, in one sentence, when CoderHelm can't start
    /// the skipped checks (label-gated or unknown) itself.
    pub fn action(&self) -> Option<String> {
        let mut parts = Vec::new();
        if !self.draft_no_start.is_empty() {
            parts.push(format!(
                "{} skip drafts and their workflow doesn't start when a pull request is marked \
                 ready: mark it ready and push to its branch to run them",
                list(&self.draft_no_start)
            ));
        }
        // Checks that the same labels run share one sentence.
        let mut groups: Vec<(Vec<String>, bool, Vec<String>)> = Vec::new();
        for g in &self.label_gated {
            let (labels, together) = if g.any.is_empty() {
                (g.all.clone(), true)
            } else {
                (g.any.clone(), false)
            };
            match groups
                .iter_mut()
                .find(|(l, t, _)| *l == labels && *t == together)
            {
                Some((_, _, checks)) => checks.push(g.check.clone()),
                None => groups.push((labels, together, vec![g.check.clone()])),
            }
        }
        for (labels, together, checks) in groups {
            let word = if labels.len() == 1 { "label" } else { "labels" };
            let names: Vec<String> = labels.iter().map(|l| shown(l)).collect();
            let joined = names.join(if together { " and " } else { " or " });
            parts.push(format!("Add the {joined} {word} to run {}", list(&checks)));
        }
        if !self.unknown.is_empty() {
            parts.push(format!(
                "Start {} on this head (CoderHelm can't tell from the workflow what runs it)",
                list(&self.unknown)
            ));
        }
        if parts.is_empty() {
            return None;
        }
        Some(format!(
            "{}, or mark it ready yourself if they aren't needed.",
            parts.join("; ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAGING: &str = r#"
name: Deploy to Staging
on:
  pull_request:
    types: [opened, synchronize, reopened, ready_for_review, labeled]
jobs:
  deploy:
    name: Deploy to Staging
    if: github.event.pull_request.draft == false && contains(github.event.pull_request.labels.*.name, 'CI:DEPLOY_STAGING')
    runs-on: ubuntu-latest
    steps: []
  e2e:
    name: E2E after staging deploy
    needs: deploy
    runs-on: ubuntu-latest
    steps: []
"#;

    fn job(cond: &str) -> String {
        format!("jobs:\n  a:\n    if: {cond}\n    runs-on: x\n")
    }

    fn need(ways: &[LabelWay]) -> Vec<Vec<String>> {
        ways.iter().map(|w| w.need.clone()).collect()
    }

    #[test]
    fn draft_and_label_conditions_are_read() {
        let g = read(STAGING);
        assert!(g.draft);
        assert!(g.on_ready);
        assert!(!read("on: [pull_request]\njobs:\n  a:\n    if: github.event.pull_request.draft == false\n").on_ready);
        assert_eq!(g.labels(), vec!["CI:DEPLOY_STAGING"]);
        // The job that needs the deploy asks for its label too.
        let e2e = g.job_for("E2E after staging deploy").unwrap();
        assert_eq!(need(&e2e.ways), vec![vec!["CI:DEPLOY_STAGING"]]);
    }

    #[test]
    fn every_way_of_writing_not_a_draft_is_read() {
        for cond in [
            "if: ${{ !github.event.pull_request.draft }}",
            "if: ! github.event.pull_request.draft",
            "if: github.event.pull_request.draft != true",
            "if: ${{ github.event.pull_request.draft == false }}",
            "if: github.event_name != 'pull_request' || github.event.pull_request.draft == false",
        ] {
            assert!(read(&format!("jobs:\n  a:\n    {cond}\n")).draft, "{cond}");
        }
        // Runs only on drafts, or doesn't look at draft at all.
        for cond in [
            "if: github.event.pull_request.draft == true",
            "if: github.ref == 'refs/heads/main'",
        ] {
            assert!(!read(&format!("jobs:\n  a:\n    {cond}\n")).draft, "{cond}");
        }
    }

    #[test]
    fn a_trigger_only_on_ready_for_review_is_draft_gated() {
        let only = "on:\n  pull_request:\n    types: [ready_for_review]\njobs: {}\n";
        assert!(read(only).draft);
        let also = "on:\n  pull_request:\n    types: [opened, synchronize, ready_for_review]\n";
        assert!(!read(also).draft);
    }

    #[test]
    fn exact_prefix_and_contains_conditions_are_patterns_never_prefix_labels() {
        let labels = |c: &str| read(&job(c)).labels();
        // Exact: `==` on the event's label, membership in the label list.
        assert_eq!(
            labels("github.event.label.name == 'CI:E2E'"),
            vec!["CI:E2E"]
        );
        assert_eq!(
            labels("${{ 'CI:E2E' == github.event.label.name }}"),
            vec!["CI:E2E"]
        );
        assert_eq!(
            labels("contains(github.event.pull_request.labels.*.name, 'CI:E2E')"),
            vec!["CI:E2E"]
        );
        // A prefix test is a prefix, never a label named `E2E:`.
        assert_eq!(
            labels("startsWith(github.event.label.name, 'E2E:')"),
            vec!["E2E:*"]
        );
        // Substring tests match part of a name.
        assert_eq!(
            labels("contains(github.event.label.name, 'E2E:')"),
            vec!["*E2E:*"]
        );
        assert_eq!(
            labels("contains(join(github.event.pull_request.labels.*.name, ','), 'E2E:')"),
            vec!["*E2E:*"]
        );
        assert_eq!(
            labels("endsWith(github.event.label.name, ':e2e')"),
            vec!["*:e2e"]
        );
        assert_eq!(
            labels("contains(fromJSON('[\"run-e2e\",\"e2e-all\"]'), github.event.label.name)"),
            vec!["run-e2e", "e2e-all"]
        );
        for l in labels("startsWith(github.event.label.name, 'E2E:')") {
            assert!(!matches(&l, "E2E:"), "a prefix is never a label");
            assert!(matches(&l, "E2E:nav"));
        }
        // Labels that stop a job are not labels that run it.
        assert!(
            labels("${{ !contains(github.event.pull_request.labels.*.name, 'skip-e2e') }}")
                .is_empty()
        );
        assert!(labels("github.event.label.name != 'wip'").is_empty());
    }

    /// The incident's staging job: its `if:` spans lines and every way
    /// needs the staging label.
    const STAGING_FOLDED: &str = r#"
on:
  pull_request:
    types: [labeled, synchronize]
jobs:
  deploy_staging_web:
    name: Deploy Web App
    if: >-
      github.event_name == 'push' ||
      (github.event.action == 'labeled' && contains(github.event.label.name, 'CI:DEPLOY_STAGING')) ||
      (github.event.action == 'labeled' &&
       (github.event.label.name == 'CI:E2E' || startsWith(github.event.label.name, 'E2E:')) &&
       contains(github.event.pull_request.labels.*.name, 'CI:DEPLOY_STAGING')) ||
      (github.event.action == 'synchronize' && contains(github.event.pull_request.labels.*.name, 'CI:DEPLOY_STAGING'))
    runs-on: ubuntu-latest
  e2e_playwright:
    name: E2E after staging deploy
    needs: deploy_staging_web
    uses: ./.github/workflows/e2e-playwright.yml
"#;

    #[test]
    fn a_folded_condition_keeps_its_ands() {
        let g = read(STAGING_FOLDED);
        let e2e = g.job_for("E2E after staging deploy / run").unwrap();
        assert_eq!(
            need(&e2e.ways),
            vec![
                vec!["*CI:DEPLOY_STAGING*"],
                vec!["CI:E2E", "CI:DEPLOY_STAGING"],
                vec!["E2E:*", "CI:DEPLOY_STAGING"],
                vec!["CI:DEPLOY_STAGING"],
            ]
        );
        // Without the staging label no one label but the staging one runs it.
        let rows = vec![("E2E after staging deploy".to_string(), Some(g))];
        let s = sort(&rows, &["ch-review".into()]);
        assert_eq!(
            s.label_gated[0].any,
            vec!["*CI:DEPLOY_STAGING*", "CI:DEPLOY_STAGING"]
        );
        let repo: Vec<String> = ["CI:E2E", "CI:DEPLOY_STAGING", "E2E:nav", "E2E:join"]
            .map(String::from)
            .to_vec();
        // CoderHelm adds nothing: the one label is a staging label.
        assert!(choose(&s, &repo, &[], &["ch-review".into()]).is_empty());
        let mut named = s.clone();
        named.resolve_labels(&repo, &[]);
        assert_eq!(
            named.action().unwrap(),
            "Add the `CI:DEPLOY_STAGING` label to run `E2E after staging deploy`, or mark it \
             ready yourself if they aren't needed."
        );
    }

    #[test]
    fn a_label_is_chosen_from_the_repo_labels_only_when_certain() {
        let repo: Vec<String> = ["CI:E2E", "E2E:nav", "E2E:join", "run-smoke", "E2E:"]
            .map(String::from)
            .to_vec();
        // Exact: the repo's own spelling; a name the repo lacks is nothing.
        assert_eq!(resolve("ci:e2e", &repo, &[]), Some("CI:E2E".into()));
        assert_eq!(resolve("CI:E2E_ALL", &repo, &[]), None);
        // A prefix with several labels is ambiguous, unless the team's
        // settings name one; it never resolves to the bare prefix.
        assert_eq!(resolve("E2E:*", &repo, &[]), None);
        assert_eq!(
            resolve("E2E:*", &repo, &["E2E:nav".into()]),
            Some("E2E:nav".into())
        );
        assert_eq!(resolve("E2E:", &repo, &[]), None);
        assert_eq!(resolve("*E2E:*", &repo, &[]), None);
        assert_eq!(resolve("run-*", &repo, &[]), Some("run-smoke".into()));
        // A contains test on a whole name means that label.
        assert_eq!(resolve("*CI:E2E*", &repo, &[]), Some("CI:E2E".into()));

        let gate = read(&job(
            "github.event.label.name == 'CI:E2E' || startsWith(github.event.label.name, 'E2E:')",
        ));
        let rows = vec![("a".to_string(), Some(gate))];
        let s = sort(&rows, &[]);
        assert_eq!(s.label_gated[0].any, vec!["CI:E2E", "E2E:*"]);
        // One certain label: CI:E2E (the prefix has several).
        assert_eq!(choose(&s, &repo, &[], &[]), vec!["CI:E2E"]);
        // Two certain labels for the same check: ambiguous, nothing.
        let two: Vec<String> = ["CI:E2E", "E2E:nav"].map(String::from).to_vec();
        assert!(choose(&s, &two, &[], &[]).is_empty());
        // Only the prefix, with several labels: nothing, never `E2E:`.
        let prefix_only = read(&job("startsWith(github.event.label.name, 'E2E:')"));
        let s = sort(&[("a".to_string(), Some(prefix_only))], &[]);
        assert!(choose(&s, &repo, &[], &[]).is_empty());
        let a = s.action().unwrap();
        assert!(a.contains("`E2E:…`"), "{a}");
        // Nothing next to a deploy label already on the pull request.
        let s = sort(&rows, &[]);
        assert!(choose(&s, &repo, &[], &["CI:DEPLOY_STAGING".into()]).is_empty());
        // A check whose job isn't found is named, never labelled.
        let s = sort(
            &[(
                "other".to_string(),
                Some(read(&job("github.event.label.name == 'CI:E2E'"))),
            )],
            &[],
        );
        assert!(!s.label_gated[0].own);
        assert!(choose(&s, &repo, &[], &[]).is_empty());
    }

    #[test]
    fn labels_coderhelm_added_by_mistake_come_off() {
        let rows = vec![(
            "E2E after staging deploy".to_string(),
            Some(read(STAGING_FOLDED)),
        )];
        let repo: Vec<String> = [
            "CI:E2E",
            "CI:DEPLOY_STAGING",
            "E2E:nav",
            "E2E:",
            "ch-review",
        ]
        .map(String::from)
        .to_vec();
        let on_pr: Vec<String> = ["ch-review", "CI:E2E", "E2E:"].map(String::from).to_vec();
        let ours: Vec<String> = ["CI:E2E", "E2E:"].map(String::from).to_vec();
        // `E2E:` is a bare prefix; `CI:E2E` alone can't run the staging job.
        assert_eq!(to_take_off(&rows, &on_pr, &ours, &repo, &[]), ours);
        // A label a person added is never taken off.
        assert!(to_take_off(&rows, &on_pr, &[], &repo, &[]).is_empty());
        // A label that ran its check (nothing skipped mentions it) stays.
        assert!(to_take_off(&[], &on_pr, &["CI:E2E".into()], &repo, &[]).is_empty());
    }

    #[test]
    fn skipped_checks_are_sorted_by_why() {
        let staging = read(STAGING);
        let label_only = read(&job(
            "contains(github.event.pull_request.labels.*.name, 'CI:DEPLOY_STAGING')",
        ));
        let rows = vec![
            ("Deploy to Staging".to_string(), Some(staging.clone())),
            (
                "E2E after staging deploy".to_string(),
                Some(staging.clone()),
            ),
        ];
        // The label is on the pull request: only the draft keeps them off.
        let s = sort(&rows, &["CI:DEPLOY_STAGING".into()]);
        assert!(s.all_draft_gated());
        assert_eq!(s.action(), None);
        // A workflow that needs only "not a draft" runs once it is ready.
        let draft_only = WorkflowGate {
            draft: true,
            on_ready: true,
            ..Default::default()
        };
        let s = sort(&[("Deploy Web App".into(), Some(draft_only))], &[]);
        assert_eq!(s.draft_gated, vec!["Deploy Web App"]);
        // Skips drafts but never starts on "ready for review": marking it
        // ready alone runs nothing, so a person is asked.
        let no_start = WorkflowGate {
            draft: true,
            ..Default::default()
        };
        let s = sort(&[("Deploy Web App".into(), Some(no_start))], &[]);
        assert!(s.draft_gated.is_empty());
        assert!(s.action().unwrap().contains("push to its branch"));
        // The label is missing: marking it ready alone wouldn't run them.
        let s = sort(&rows, &["E2E:nav".into()]);
        assert!(!s.all_draft_gated());
        let a = s.action().unwrap();
        assert!(
            a.starts_with(
                "Add the `CI:DEPLOY_STAGING` label to run `Deploy to Staging`, `E2E after staging deploy`"
            ),
            "{a}"
        );
        assert!(a.ends_with("or mark it ready yourself if they aren't needed."));
        let s = sort(
            &[
                ("smoke".to_string(), None),
                ("a".to_string(), Some(label_only)),
            ],
            &[],
        );
        assert_eq!(s.unknown, vec!["smoke"]);
        assert_eq!(s.labels(), vec!["CI:DEPLOY_STAGING"]);
        assert!(s.action().unwrap().contains("Start `smoke` on this head"));
        // Two labels together: named together.
        let both = read(&job(
            "contains(github.event.pull_request.labels.*.name, 'a') && contains(github.event.pull_request.labels.*.name, 'b')",
        ));
        let s = sort(&[("a".to_string(), Some(both))], &[]);
        assert!(s
            .action()
            .unwrap()
            .starts_with("Add the `a` and `b` labels"));
    }
}
