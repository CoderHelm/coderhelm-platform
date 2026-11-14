//! The package manager's own audit, run by code after a change to the
//! dependencies, so the pull request quotes what is left.
//!
//! A dependency update finished with "the remaining critical and high
//! advisory counts are unknown because the audit never ran" listed on its
//! pull request for a person, twice. Nothing a person has was missing: the
//! audit is one command of the package manager the repository already uses.
//!
//! The rules here:
//!
//! - [`audits`] names the audit of every lockfile in a tree, by its package
//!   manager, the way `upgrade_notes::lock_tools` names the command that
//!   writes it.
//! - [`command`] is the one sandbox command that runs them. An audit prints
//!   far more than a build's output keeps, so the command reduces each
//!   report to a few marked lines where it ran: the tool's own summary, a
//!   tally by severity, or the number of distinct advisories. It always
//!   exits 0: an audit that finds advisories is a result, not a failure.
//! - [`read`] turns those lines into counts per lockfile, or why an audit
//!   gave none, and [`Audited::line`] is the sentence the pull request
//!   quotes.
//!
//! Everything here is pure; the sandbox run is the worker's
//! (`worker::passes::settle`).

use crate::upgrade_notes::{lock_tools, LockTool};

/// How a tool's report is reduced to counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reads {
    /// The report carries its own totals by severity
    /// (`"vulnerabilities":{"critical":0,"high":2,...}`).
    Summary,
    /// Each advisory carries a severity: they are tallied.
    Severities,
    /// The tool rates nothing: distinct advisory ids are counted. The
    /// pattern is an extended regular expression over the report with its
    /// whitespace removed.
    Ids(&'static str),
}

/// One lockfile's audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Audit {
    pub manager: &'static str,
    pub lockfile: String,
    /// The audit as people read it (`pnpm audit --json`).
    pub shown: &'static str,
    /// The full command, run in the lockfile's folder.
    pub command: String,
    pub reads: Reads,
}

/// A manager's audit: what people read, what runs (after making the tool
/// available), and how its report is read.
struct Tool {
    shown: &'static str,
    run: String,
    reads: Reads,
}

/// Writes a Python lockfile out as pinned requirements and audits those:
/// `pip-audit` reads no lockfile format of its own.
fn pip_audit(export: &str) -> String {
    format!(
        "(command -v pip-audit >/dev/null 2>&1 || pip install -q pip-audit) && {export} && \
         pip-audit -r /tmp/coderhelm_audit_reqs.txt --no-deps --disable-pip -f json"
    )
}

const PIP_IDS: Reads = Reads::Ids(r#""id":"[A-Z]+-[A-Za-z0-9-]+""#);

/// The audit of `manager`'s lockfile. `berry` is a yarn project with a
/// `.yarnrc.yml` (yarn 2 and later audit through `yarn npm audit`).
fn tool(manager: &str, berry: bool) -> Option<Tool> {
    use crate::e2e::test_package::{ensure_manager, Manager};
    Some(match manager {
        "npm" => Tool {
            shown: "npm audit --json",
            run: "npm audit --json".into(),
            reads: Reads::Summary,
        },
        "pnpm" => Tool {
            shown: "pnpm audit --json",
            run: format!("{}pnpm audit --json", ensure_manager(Manager::Pnpm)),
            reads: Reads::Summary,
        },
        "yarn" if berry => Tool {
            shown: "yarn npm audit --all --recursive --json",
            run: format!(
                "{}yarn npm audit --all --recursive --json",
                ensure_manager(Manager::Yarn)
            ),
            reads: Reads::Severities,
        },
        "yarn" => Tool {
            shown: "yarn audit --json",
            run: format!("{}yarn audit --json", ensure_manager(Manager::Yarn)),
            reads: Reads::Summary,
        },
        "bun" => Tool {
            shown: "bun audit --json",
            run: format!("{}bun audit --json", ensure_manager(Manager::Bun)),
            reads: Reads::Severities,
        },
        "cargo" => Tool {
            shown: "cargo audit --json",
            run: "(command -v cargo-audit >/dev/null 2>&1 || cargo install -q cargo-audit --locked) \
                  && cargo audit --json"
                .into(),
            reads: Reads::Ids(r#""id":"RUSTSEC-[0-9-]+""#),
        },
        "poetry" => Tool {
            shown: "pip-audit (on `poetry export`)",
            run: pip_audit(
                "(command -v poetry >/dev/null 2>&1 || pip install -q poetry) && \
                 (poetry export --without-hashes -f requirements.txt -o /tmp/coderhelm_audit_reqs.txt 2>/dev/null \
                 || (pip install -q poetry-plugin-export && poetry export --without-hashes -f requirements.txt -o /tmp/coderhelm_audit_reqs.txt))",
            ),
            reads: PIP_IDS,
        },
        "uv" => Tool {
            shown: "pip-audit (on `uv export`)",
            run: pip_audit(
                "(command -v uv >/dev/null 2>&1 || pip install -q uv) && \
                 uv export --frozen --no-hashes --no-emit-project -o /tmp/coderhelm_audit_reqs.txt >/dev/null",
            ),
            reads: PIP_IDS,
        },
        "pipenv" => Tool {
            shown: "pip-audit (on `pipenv requirements`)",
            run: pip_audit(
                "(command -v pipenv >/dev/null 2>&1 || pip install -q pipenv) && \
                 pipenv requirements > /tmp/coderhelm_audit_reqs.txt",
            ),
            reads: PIP_IDS,
        },
        "pip-tools" => Tool {
            shown: "pip-audit -r requirements.txt",
            run: pip_audit("cp requirements.txt /tmp/coderhelm_audit_reqs.txt"),
            reads: PIP_IDS,
        },
        "bundler" => Tool {
            shown: "bundle-audit check --update --format json",
            run: "(command -v bundle-audit >/dev/null 2>&1 || gem install -q bundler-audit) && \
                  bundle-audit check --update --format json"
                .into(),
            reads: Reads::Severities,
        },
        "go" => Tool {
            shown: "govulncheck -json ./...",
            run: "(command -v govulncheck >/dev/null 2>&1 || go install golang.org/x/vuln/cmd/govulncheck@latest) \
                  && PATH=\"$PATH:$(go env GOPATH)/bin\" govulncheck -json ./..."
                .into(),
            reads: Reads::Ids(r#""finding":\{"osv":"GO-[0-9-]+""#),
        },
        _ => return None,
    })
}

fn dir_of(path: &str) -> &str {
    path.rsplit_once('/').map(|(d, _)| d).unwrap_or("")
}

/// The audit of every lockfile in `tree`, one per lockfile, in path order.
pub fn audits(tree: &[String]) -> Vec<Audit> {
    lock_tools(tree)
        .into_iter()
        .filter_map(|t: LockTool| {
            let dir = dir_of(&t.lockfile);
            let rc = if dir.is_empty() {
                ".yarnrc.yml".to_string()
            } else {
                format!("{dir}/.yarnrc.yml")
            };
            let berry = tree.contains(&rc);
            let tool = tool(t.manager, berry)?;
            Some(Audit {
                manager: t.manager,
                shown: tool.shown,
                command: if dir.is_empty() {
                    tool.run
                } else {
                    format!("cd '{dir}' && {}", tool.run)
                },
                reads: tool.reads,
                lockfile: t.lockfile,
            })
        })
        .collect()
}

/// The audits of the lockfiles a change touches: `changed` holds the
/// branch's changed paths, and a lockfile is touched when it changed or a
/// file its manager reads did. Empty for a change to no dependency.
pub fn audits_for(tree: &[String], changed: &[String]) -> Vec<Audit> {
    let touched: Vec<String> = lock_tools(tree)
        .into_iter()
        .filter(|t| {
            changed.iter().any(|c| {
                *c == t.lockfile || {
                    let mut hit = false;
                    for g in &t.inputs {
                        match g.strip_prefix('!') {
                            Some(not) if glob(not, c) => return false,
                            Some(_) => {}
                            None => hit |= glob(g, c),
                        }
                    }
                    hit
                }
            })
        })
        .map(|t| t.lockfile)
        .collect();
    audits(tree)
        .into_iter()
        .filter(|a| touched.contains(&a.lockfile))
        .collect()
}

/// A lock tool's input glob against a path: `**/` spans folders, a trailing
/// `/**` takes everything under one, anything else is the path itself.
fn glob(pattern: &str, path: &str) -> bool {
    if let Some(dir) = pattern.strip_suffix("/**") {
        return path.starts_with(&format!("{dir}/"));
    }
    match pattern.split_once("**/") {
        Some((dir, name)) => {
            path.starts_with(dir) && path[dir.len()..].rsplit('/').next() == Some(name)
        }
        None => pattern == path,
    }
}

/// The hosts a manager's audit reads, beyond its registry.
pub fn hosts(manager: &str) -> &'static [&'static str] {
    match manager {
        "npm" | "pnpm" | "yarn" | "bun" => &["registry.npmjs.org", "registry.yarnpkg.com"],
        "cargo" => &[
            "github.com",
            "crates.io",
            "index.crates.io",
            "static.crates.io",
        ],
        "poetry" | "uv" | "pipenv" | "pip-tools" => {
            &["pypi.org", "files.pythonhosted.org", "api.osv.dev"]
        }
        "bundler" => &["rubygems.org", "github.com"],
        "go" => &["vuln.go.dev", "proxy.golang.org", "sum.golang.org"],
        _ => &[],
    }
}

// ── The command and its output ──────────────────────────────────────────

/// What each audit's lines in the output start with.
pub const MARK: &str = "CODERHELM_AUDIT";
const SUMMARY: &str = "CODERHELM_AUDIT_SUMMARY";
const SEVERITY: &str = "CODERHELM_AUDIT_SEVERITY";
const IDS: &str = "CODERHELM_AUDIT_IDS";
const TAIL: &str = "CODERHELM_AUDIT_TAIL";

/// The one sandbox command that runs every audit in `audits`. Each reports
/// `CODERHELM_AUDIT <index> <exit code>`, then its report reduced to marked
/// lines, then the end of what it printed (for an audit that gave no
/// report). Always exits 0.
pub fn command(audits: &[Audit]) -> String {
    let mut parts = Vec::new();
    for (i, a) in audits.iter().enumerate() {
        let f = format!("/tmp/coderhelm_audit_{i}");
        let mut p = format!(
            "({}) > {f}.out 2> {f}.err; c=$?; echo \"{MARK} {i} $c\"; \
             tr -d ' \\n\\r\\t' < {f}.out > {f}.flat; \
             grep -oE '\"vulnerabilities\":\\{{[^{{}}]*\\}}' {f}.flat | tail -n 1 | sed 's/^/{SUMMARY} {i} /'; \
             grep -oiE '\"(severity|criticality)\":\"[a-z]+\"' {f}.flat | tr 'A-Z' 'a-z' | sort | uniq -c | sed 's/^ */{SEVERITY} {i} /'",
            a.command
        );
        if let Reads::Ids(pattern) = a.reads {
            p.push_str(&format!(
                "; grep -oE '{pattern}' {f}.flat | sort -u | wc -l | sed 's/^ */{IDS} {i} /'"
            ));
        }
        p.push_str(&format!(
            "; cat {f}.err {f}.out | tail -c 300 | tr '\\n' ' ' | sed 's/^/{TAIL} {i} /'; echo"
        ));
        parts.push(p);
    }
    parts.push("true".to_string());
    parts.join("; ")
}

/// Advisories left, by severity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub critical: u32,
    pub high: u32,
    /// Moderate, low and informational.
    pub other: u32,
    /// Advisories the tool gives no severity for.
    pub unrated: u32,
}

impl Counts {
    pub fn total(&self) -> u32 {
        self.critical + self.high + self.other + self.unrated
    }
}

/// What one audit came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Counts(Counts),
    /// It gave no report, with the end of what it printed.
    Failed(String),
}

/// One lockfile's audit with its result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Audited {
    pub manager: String,
    pub lockfile: String,
    pub shown: String,
    pub outcome: Outcome,
}

impl Audited {
    /// The sentence the pull request quotes.
    pub fn line(&self) -> String {
        let at = format!("`{}` on `{}`", self.shown, self.lockfile);
        match &self.outcome {
            Outcome::Counts(c) if c.total() == 0 => format!("{at}: no advisories left"),
            Outcome::Counts(c) if c.unrated > 0 && c.unrated == c.total() => format!(
                "{at}: {} advisor{} left (the tool gives no severity)",
                c.unrated,
                if c.unrated == 1 { "y" } else { "ies" }
            ),
            Outcome::Counts(c) => format!(
                "{at}: {} critical, {} high and {} lower advisor{} left",
                c.critical,
                c.high,
                c.other + c.unrated,
                if c.other + c.unrated == 1 { "y" } else { "ies" }
            ),
            Outcome::Failed(why) => format!("{at} gave no report: {why}"),
        }
    }

    pub fn ran(&self) -> bool {
        matches!(self.outcome, Outcome::Counts(_))
    }
}

/// The lines of `output` marked `mark <index> `, each without that prefix.
fn marked<'a>(output: &'a str, mark: &str, index: usize) -> Vec<&'a str> {
    let head = format!("{mark} {index} ");
    output
        .lines()
        .filter_map(|l| l.trim().strip_prefix(head.as_str()))
        .collect()
}

/// The number after `"key":` in a flat JSON object.
fn number(object: &str, key: &str) -> Option<u32> {
    let at = object.find(&format!("\"{key}\":"))? + key.len() + 3;
    let digits: String = object[at..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

fn by_severity(c: &mut Counts, severity: &str, n: u32) {
    match severity {
        "critical" => c.critical += n,
        "high" => c.high += n,
        "none" | "unknown" | "" => c.unrated += n,
        _ => c.other += n,
    }
}

/// Each audit's result from a [`command`] run's output, in order. An audit
/// with no line of its own (the build was cut short) gave no report.
pub fn read(audits: &[Audit], output: &str) -> Vec<Audited> {
    audits
        .iter()
        .enumerate()
        .map(|(i, a)| {
            let exit: Option<i32> = marked(output, MARK, i)
                .first()
                .and_then(|c| c.trim().parse().ok());
            let tail = marked(output, TAIL, i)
                .first()
                .map(|t| crate::truncate_str(t.trim(), 240).to_string())
                .unwrap_or_default();
            let failed = |what: &str| {
                Outcome::Failed(if tail.is_empty() {
                    what.to_string()
                } else {
                    format!("{what} ({tail})")
                })
            };
            let outcome = match (exit, a.reads) {
                (None, _) => failed("the sandbox build ended before it reported"),
                (Some(code), Reads::Summary) => {
                    match marked(output, SUMMARY, i)
                        .into_iter()
                        .find(|s| s.contains("\"critical\":"))
                    {
                        Some(s) => {
                            let n = |k: &str| number(s, k).unwrap_or(0);
                            Outcome::Counts(Counts {
                                critical: n("critical"),
                                high: n("high"),
                                other: n("moderate") + n("low") + n("info"),
                                unrated: 0,
                            })
                        }
                        None => failed(&format!("{} exited {code} with no summary", a.manager)),
                    }
                }
                (Some(code), Reads::Severities) => {
                    let mut c = Counts::default();
                    for l in marked(output, SEVERITY, i) {
                        let mut it = l.split_whitespace();
                        let (Some(n), Some(kv)) =
                            (it.next().and_then(|n| n.parse().ok()), it.next())
                        else {
                            continue;
                        };
                        let sev = kv.rsplit(':').next().unwrap_or("").trim_matches('"');
                        by_severity(&mut c, sev, n);
                    }
                    if code != 0 && c.total() == 0 {
                        failed(&format!(
                            "{} exited {code} and listed no advisory",
                            a.manager
                        ))
                    } else {
                        Outcome::Counts(c)
                    }
                }
                (Some(code), Reads::Ids(_)) => {
                    let n: u32 = marked(output, IDS, i)
                        .first()
                        .and_then(|n| n.trim().parse().ok())
                        .unwrap_or(0);
                    if code != 0 && n == 0 {
                        failed(&format!(
                            "{} exited {code} and listed no advisory",
                            a.manager
                        ))
                    } else {
                        Outcome::Counts(Counts {
                            unrated: n,
                            ..Default::default()
                        })
                    }
                }
            };
            Audited {
                manager: a.manager.to_string(),
                lockfile: a.lockfile.clone(),
                shown: a.shown.to_string(),
                outcome,
            }
        })
        .collect()
}

// ── The production-scoped audit and the advisories it lists ─────────────

/// The audit of production dependencies only, for the managers whose own
/// documentation names the flag: what people read, and what runs. `None`
/// for a manager CoderHelm has no production-scoped audit for (its audit
/// of every dependency is [`audits`]).
pub fn production(manager: &str) -> Option<(&'static str, String)> {
    use crate::e2e::test_package::{ensure_manager, Manager};
    match manager {
        "npm" => Some((
            "npm audit --omit=dev --json",
            "npm audit --omit=dev --json".to_string(),
        )),
        "pnpm" => Some((
            "pnpm audit --prod --json",
            format!("{}pnpm audit --prod --json", ensure_manager(Manager::Pnpm)),
        )),
        _ => None,
    }
}

/// One advisory an audit lists.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Listed {
    /// The advisory's id as the audit names it (a GHSA id), upper case.
    pub id: String,
    pub severity: String,
    pub package: String,
}

/// What one audit listed.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Listing {
    /// What ran, as people read it.
    pub shown: String,
    /// Distinct advisory and package pairs the report holds.
    pub total: usize,
    /// The first [`MAX_LISTED`] of them.
    pub listed: Vec<Listed>,
}

impl Listing {
    /// The report held more than is listed here.
    pub fn cut(&self) -> bool {
        self.total > self.listed.len()
    }
}

/// The production audit's listing and the listing of every dependency.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Listings {
    pub production: Option<Listing>,
    pub all: Option<Listing>,
    /// Why a listing is missing; empty when both were read.
    pub why: String,
}

/// Advisories one listing carries at most: the sandbox keeps only the end
/// of a build's output.
pub const MAX_LISTED: usize = 30;
const LIST_ADV: &str = "CODERHELM_AUDIT_ADV";
const LIST_END: &str = "CODERHELM_AUDIT_LIST";

/// Reduces an audit's JSON report (npm 7 and later, pnpm, yarn, bun) to
/// one line per advisory and package: any object that names a GHSA id (a
/// `github_advisory_id`, or an advisory URL) is an advisory, its package
/// the name beside it. It then prints how many it found and whether the
/// file was a report at all.
const LIST_JS: &str = r#"
const fs=require('fs');const [file,scope,max]=process.argv.slice(2);
let text='';try{text=fs.readFileSync(file,'utf8')}catch(e){}
let docs=[];
try{docs=[JSON.parse(text.slice(text.indexOf('{')))]}catch(e){
  for(const l of text.split('\n')){try{const d=JSON.parse(l);if(d&&typeof d==='object')docs.push(d)}catch(e){}}}
const report=docs.some(d=>d.auditReportVersion||d.vulnerabilities||d.advisories||d.metadata||d.type==='auditSummary'||d.type==='auditAdvisory'||d.children);
const seen=new Map();
function ghsa(o){const g=String(o.github_advisory_id||o.ghsa||'');if(/^GHSA-/i.test(g))return g;
  for(const k of['url','URL']){const m=String(o[k]==null?'':o[k]).match(/GHSA(-[0-9a-z]{4}){3}/i);if(m)return m[0]}return ''}
function walk(o,key){
  if(Array.isArray(o)){for(const x of o)walk(x,key);return}
  if(!o||typeof o!=='object')return;
  const g=ghsa(o);
  if(g){const pkg=String(o.module_name||o.name||o.dependency||o.package||key||'-').replace(/\s/g,'');
    const sev=String(o.severity||o.Severity||'-').toLowerCase().replace(/\s/g,'');
    const k=g.toUpperCase()+' '+pkg;if(!seen.has(k))seen.set(k,sev)}
  const own=(typeof o.value==='string'&&o.children)?o.value:null;
  for(const [k,v] of Object.entries(o))walk(v,own||k)}
for(const d of docs)walk(d,'');
let n=0;for(const [k,sev] of seen){if(n++>=Number(max))break;const [g,pkg]=k.split(' ');console.log(['CODERHELM_AUDIT_ADV',scope,g,sev,pkg].join(' '))}
console.log(['CODERHELM_AUDIT_LIST',scope,seen.size,report?1:0].join(' '));
"#;

/// The shell lines that list what a manager's audits found, for the
/// managers with a [`production`] audit: `{out}` is the file the
/// production audit's report was written to; the audit of every
/// dependency is run here, in `dir`, and listed beside it. `None` for a
/// manager with no production audit.
pub fn listing(manager: &str, dir: &str) -> Option<String> {
    production(manager)?;
    let all = tool(manager, false)?;
    let cd = if dir.is_empty() {
        String::new()
    } else {
        format!("cd '{dir}' && ")
    };
    Some(format!(
        "echo '{}' | base64 -d > /tmp/coderhelm-audit-list.js; \
         node /tmp/coderhelm-audit-list.js {{out}} production {MAX_LISTED}; \
         ( {cd}{} ) > /tmp/coderhelm-audit-all.out 2>/dev/null; \
         node /tmp/coderhelm-audit-list.js /tmp/coderhelm-audit-all.out all {MAX_LISTED}",
        crate::dep_fix::b64(LIST_JS.as_bytes()),
        all.run
    ))
}

/// The listings in a build's output. A scope whose end line is missing, or
/// whose file was no audit report, has no listing, and `why` says so.
pub fn listings(manager: &str, output: &str) -> Listings {
    let mut out = Listings::default();
    let shown_all = tool(manager, false).map(|t| t.shown).unwrap_or("the audit");
    let shown_prod = production(manager)
        .map(|(s, _)| s)
        .unwrap_or("the production audit");
    let mut missing: Vec<String> = vec![];
    for (scope, shown) in [("production", shown_prod), ("all", shown_all)] {
        let mut listed: Vec<Listed> = vec![];
        let mut end: Option<(usize, bool)> = None;
        for line in output.lines() {
            let mut p = line.split_whitespace();
            match (p.next(), p.next()) {
                (Some(LIST_ADV), Some(s)) if s == scope => {
                    if let (Some(id), Some(sev), Some(pkg)) = (p.next(), p.next(), p.next()) {
                        listed.push(Listed {
                            id: id.to_ascii_uppercase(),
                            severity: sev.to_string(),
                            package: pkg.to_string(),
                        });
                    }
                }
                (Some(LIST_END), Some(s)) if s == scope => {
                    if let (Some(n), Some(ok)) = (p.next().and_then(|n| n.parse().ok()), p.next()) {
                        end = Some((n, ok == "1"));
                    }
                }
                _ => {}
            }
        }
        let listing = match end {
            Some((total, true)) => Some(Listing {
                shown: shown.to_string(),
                total,
                listed,
            }),
            Some((_, false)) => {
                missing.push(format!("`{shown}` gave no report"));
                None
            }
            None => {
                missing.push(format!("`{shown}` did not report before the build ended"));
                None
            }
        };
        if scope == "production" {
            out.production = listing;
        } else {
            out.all = listing;
        }
    }
    out.why = missing.join("; ");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|p| p.to_string()).collect()
    }

    /// Every manager CoderHelm writes a lockfile for has its audit.
    #[test]
    fn every_lockfile_manager_has_its_audit() {
        let all = tree(&[
            "package-lock.json",
            "web/pnpm-lock.yaml",
            "classic/yarn.lock",
            "berry/yarn.lock",
            "berry/.yarnrc.yml",
            "edge/bun.lock",
            "svc/Cargo.lock",
            "py/poetry.lock",
            "ml/uv.lock",
            "old/Pipfile.lock",
            "tools/requirements.txt",
            "tools/requirements.in",
            "api/Gemfile.lock",
            "cli/go.sum",
        ]);
        let found = audits(&all);
        let of = |lock: &str| {
            found
                .iter()
                .find(|a| a.lockfile == lock)
                .unwrap_or_else(|| panic!("no audit for {lock}"))
        };
        assert_eq!(of("package-lock.json").command, "npm audit --json");
        assert_eq!(of("package-lock.json").reads, Reads::Summary);
        let pnpm = of("web/pnpm-lock.yaml");
        assert!(pnpm.command.starts_with("cd 'web' && "), "{}", pnpm.command);
        assert!(pnpm.command.ends_with("pnpm audit --json"));
        assert_eq!(of("classic/yarn.lock").shown, "yarn audit --json");
        assert_eq!(of("classic/yarn.lock").reads, Reads::Summary);
        assert_eq!(
            of("berry/yarn.lock").shown,
            "yarn npm audit --all --recursive --json"
        );
        assert_eq!(of("berry/yarn.lock").reads, Reads::Severities);
        assert!(of("edge/bun.lock").command.ends_with("bun audit --json"));
        assert!(of("svc/Cargo.lock").command.ends_with("cargo audit --json"));
        for lock in [
            "py/poetry.lock",
            "ml/uv.lock",
            "old/Pipfile.lock",
            "tools/requirements.txt",
        ] {
            assert!(of(lock).command.contains("pip-audit -r "), "{lock}");
        }
        assert!(of("api/Gemfile.lock")
            .command
            .contains("bundle-audit check"));
        assert!(of("cli/go.sum").command.contains("govulncheck -json ./..."));
        // Every manager the lockfile table names is here, and each names
        // the hosts its audit reads.
        for t in lock_tools(&all) {
            assert!(
                found.iter().any(|a| a.lockfile == t.lockfile),
                "{} has no audit",
                t.manager
            );
            assert!(!hosts(t.manager).is_empty(), "{} has no hosts", t.manager);
        }
        assert_eq!(found.len(), lock_tools(&all).len());
    }

    #[test]
    fn only_the_lockfiles_a_change_touches_are_audited() {
        let t = tree(&[
            "package.json",
            "pnpm-lock.yaml",
            "apps/web/package.json",
            "svc/Cargo.toml",
            "svc/Cargo.lock",
            "docs/readme.md",
        ]);
        let names = |changed: &[&str]| -> Vec<String> {
            audits_for(&t, &tree(changed))
                .into_iter()
                .map(|a| a.lockfile)
                .collect()
        };
        assert_eq!(names(&["apps/web/package.json"]), vec!["pnpm-lock.yaml"]);
        assert_eq!(names(&["svc/Cargo.lock"]), vec!["svc/Cargo.lock"]);
        assert_eq!(names(&["svc/Cargo.toml"]), vec!["svc/Cargo.lock"]);
        assert!(names(&["docs/readme.md", "apps/web/src/a.ts"]).is_empty());
    }

    fn two() -> Vec<Audit> {
        audits(&tree(&["pnpm-lock.yaml", "svc/Cargo.lock"]))
    }

    #[test]
    fn one_command_runs_every_audit_and_never_fails_the_build() {
        let cmd = command(&two());
        assert!(cmd.contains("echo \"CODERHELM_AUDIT 0 $c\""));
        assert!(cmd.contains("echo \"CODERHELM_AUDIT 1 $c\""));
        assert!(cmd.contains("pnpm audit --json"));
        assert!(cmd.contains("cd 'svc' && "));
        // Only a tool that rates nothing has its ids counted.
        assert_eq!(cmd.matches("CODERHELM_AUDIT_IDS").count(), 1);
        assert!(cmd.ends_with("; true"));
    }

    #[test]
    fn counts_are_read_from_each_tools_report() {
        let out = "CODERHELM_AUDIT 0 1\n\
            CODERHELM_AUDIT_SUMMARY 0 \"vulnerabilities\":{\"info\":0,\"low\":1,\"moderate\":4,\"high\":2,\"critical\":0}\n\
            CODERHELM_AUDIT_SEVERITY 0 9 \"severity\":\"high\"\n\
            CODERHELM_AUDIT_TAIL 0 }\n\
            CODERHELM_AUDIT 1 1\n\
            CODERHELM_AUDIT_IDS 1 3\n\
            CODERHELM_AUDIT_TAIL 1 }\n";
        let read = read(&two(), out);
        // The summary stands, never the per-advisory tally beside it (npm
        // and pnpm repeat a severity on every path to a package).
        assert_eq!(
            read[0].outcome,
            Outcome::Counts(Counts {
                critical: 0,
                high: 2,
                other: 5,
                unrated: 0
            })
        );
        assert_eq!(
            read[0].line(),
            "`pnpm audit --json` on `pnpm-lock.yaml`: 0 critical, 2 high and 5 lower advisories left"
        );
        assert_eq!(
            read[1].line(),
            "`cargo audit --json` on `svc/Cargo.lock`: 3 advisories left (the tool gives no severity)"
        );
        assert!(read.iter().all(Audited::ran));
    }

    #[test]
    fn severities_are_tallied_for_tools_without_a_summary() {
        let a = audits(&tree(&["yarn.lock", ".yarnrc.yml", "api/Gemfile.lock"]));
        // In path order: the Gemfile's audit is 0, the root yarn.lock's is 1.
        let out = "CODERHELM_AUDIT 1 1\n\
            CODERHELM_AUDIT_SEVERITY 1 2 \"severity\":\"critical\"\n\
            CODERHELM_AUDIT_SEVERITY 1 1 \"severity\":\"moderate\"\n\
            CODERHELM_AUDIT 0 0\n";
        let read = read(&a, out);
        let api = read.iter().find(|r| r.manager == "bundler").unwrap();
        assert_eq!(api.outcome, Outcome::Counts(Counts::default()));
        assert!(api.line().ends_with("no advisories left"), "{}", api.line());
        let yarn = read.iter().find(|r| r.manager == "yarn").unwrap();
        assert_eq!(
            yarn.outcome,
            Outcome::Counts(Counts {
                critical: 2,
                high: 0,
                other: 1,
                unrated: 0
            })
        );
    }

    #[test]
    fn an_audit_that_gave_no_report_says_why_and_is_never_read_as_clean() {
        let out = "CODERHELM_AUDIT 0 1\n\
            CODERHELM_AUDIT_TAIL 0 ERR_PNPM_AUDIT_BAD_RESPONSE The audit endpoint responded with 503\n";
        let read = read(&two(), out);
        assert!(!read[0].ran());
        assert!(
            read[0].line().contains("gave no report") && read[0].line().contains("503"),
            "{}",
            read[0].line()
        );
        // The build was cut before the second audit printed anything.
        assert_eq!(
            read[1].outcome,
            Outcome::Failed("the sandbox build ended before it reported".into())
        );
        // A tool that rates nothing and exits clean found nothing.
        let clean = super::read(
            &two()[1..],
            "CODERHELM_AUDIT 0 0\nCODERHELM_AUDIT_IDS 0 0\n",
        );
        assert_eq!(clean[0].outcome, Outcome::Counts(Counts::default()));
    }

    /// The reduction itself, run by a real shell on reports shaped like each
    /// tool's: what the sandbox prints is what [`read`] reads.
    #[test]
    fn the_shell_reduces_real_shaped_reports_to_the_marked_lines() {
        let dir = std::env::temp_dir().join(format!("coderhelm-audit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let npm = dir.join("npm.json");
        std::fs::write(
            &npm,
            "{\n  \"vulnerabilities\": {\n    \"left-pad\": {\n      \"severity\": \"high\",\n      \"via\": [{ \"severity\": \"high\" }]\n    }\n  },\n  \"metadata\": {\n    \"vulnerabilities\": {\n      \"info\": 0,\n      \"low\": 0,\n      \"moderate\": 1,\n      \"high\": 1,\n      \"critical\": 0,\n      \"total\": 2\n    }\n  }\n}\n",
        )
        .unwrap();
        let rust = dir.join("rust.json");
        std::fs::write(
            &rust,
            "{\"vulnerabilities\":{\"found\":true,\"count\":2,\"list\":[{\"advisory\":{\"id\":\"RUSTSEC-2024-0001\"}},{\"advisory\":{\"id\":\"RUSTSEC-2024-0002\"}},{\"advisory\":{\"id\":\"RUSTSEC-2024-0001\"}}]}}",
        )
        .unwrap();
        let mut audits = two();
        audits[0].command = format!("cat '{}'; exit 1", npm.display());
        audits[1].command = format!("cat '{}'; exit 1", rust.display());
        let out = std::process::Command::new("sh")
            .arg("-c")
            .arg(command(&audits))
            .output()
            .unwrap();
        assert!(out.status.success(), "the command must exit 0");
        let text = String::from_utf8_lossy(&out.stdout).to_string();
        let read = read(&audits, &text);
        assert_eq!(
            read[0].outcome,
            Outcome::Counts(Counts {
                critical: 0,
                high: 1,
                other: 1,
                unrated: 0
            }),
            "{text}"
        );
        assert_eq!(
            read[1].outcome,
            Outcome::Counts(Counts {
                unrated: 2,
                ..Default::default()
            }),
            "{text}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_production_audit_is_listed_beside_the_audit_of_everything() {
        // Only the managers whose documentation names the flag.
        assert_eq!(production("pnpm").unwrap().0, "pnpm audit --prod --json");
        assert_eq!(production("npm").unwrap().0, "npm audit --omit=dev --json");
        assert!(production("cargo").is_none());
        assert!(listing("cargo", "").is_none());
        let cmd = listing("pnpm", "apps/api").unwrap();
        assert!(cmd.contains("{out} production 30"));
        assert!(cmd.contains("cd 'apps/api' && "));
        assert!(cmd.contains("pnpm audit --json"));
        let out = "CODERHELM_AUDIT_LIST production 0 1\n\
                   CODERHELM_AUDIT_ADV all GHSA-AAAA-BBBB-CCCC high dev-tool\n\
                   CODERHELM_AUDIT_ADV all GHSA-DDDD-EEEE-FFFF moderate @scope/lint-kit\n\
                   CODERHELM_AUDIT_LIST all 2 1\n";
        let l = listings("pnpm", out);
        assert!(l.why.is_empty());
        let prod = l.production.unwrap();
        assert_eq!((prod.total, prod.listed.len()), (0, 0));
        assert_eq!(prod.shown, "pnpm audit --prod --json");
        let all = l.all.unwrap();
        assert_eq!(all.listed[1].package, "@scope/lint-kit");
        assert!(!all.cut());
        // A file that was no report, and a build cut short, give no
        // listing: never an empty one.
        let l = listings("npm", "CODERHELM_AUDIT_LIST production 0 0\n");
        assert!(l.production.is_none() && l.all.is_none());
        assert_eq!(
            l.why,
            "`npm audit --omit=dev --json` gave no report; `npm audit --json` did not report before the build ended"
        );
    }
}
