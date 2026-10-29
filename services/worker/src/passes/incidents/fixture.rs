//! The suite's monorepo and the fakes at its edges.
//!
//! The repository every case works on is invented and generic:
//!
//! - `apps/studio`: the CMS studio, its schema, and a `typegen` script that
//!   writes `apps/studio/schema.json` and, through a `../` path,
//!   `apps/web/src/lib/sanity/sanity.types.ts`;
//! - `apps/web`: the site that reads the CMS;
//! - `e2e/`: end-to-end specs and the map of which spec tests which page.
//!
//! The edges are faked, nothing else:
//!
//! - GitHub: [`FakeGitHub`] holds the base branch, the run's branch and the
//!   open pull requests, and answers in the shapes GitHub returns (a compare
//!   with `files[].filename`, a pull request with `number`, `head`, `body`).
//! - The model: [`judge_reply`] writes a reply the way the proof judge
//!   answers, read by the real parser.
//! - The sandbox: [`FakeSandbox`] is a `CodegenSource` with a real tar.gz
//!   archive and a CodeBuild-shaped log ([`codebuild_log`]), read by the
//!   real `codegen_outcome`.
//! - DynamoDB: [`run_item`] is a run record as the table holds it.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::Write;
use std::sync::Mutex;

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::{json, Value};

use crate::clients::sandbox::{self, Captured, CodegenSource};
use crate::passes::generators::RepoFiles;

pub const OWNER: &str = "acme";
pub const REPO: &str = "site";
pub const BASE: &str = "main";
pub const BRANCH: &str = "coderhelm/gh-41-card-block";
pub const TICKET: &str = "GH-41";
/// Where the studio's typegen writes the site's types, as its config
/// spells it (from `apps/studio`).
pub const TYPES_FROM_STUDIO: &str = "../web/src/lib/sanity/sanity.types.ts";
/// The same file from the repository root.
pub const TYPES: &str = "apps/web/src/lib/sanity/sanity.types.ts";
pub const SCHEMA_JSON: &str = "apps/studio/schema.json";
/// The CMS project's read API host.
pub const CMS_HOST: &str = "proj9.api.sanity.io";

/// The monorepo's files that detection reads, and its tree.
pub fn monorepo() -> RepoFiles {
    let files: Vec<(&str, String)> = vec![
        ("package.json", r#"{"private":true,"scripts":{"dev":"turbo dev"}}"#.into()),
        (
            "apps/studio/package.json",
            r#"{"scripts":{"typegen":"sanity schema extract && sanity typegen generate","deploy":"sanity deploy"}}"#.into(),
        ),
        (
            "apps/studio/sanity-typegen.json",
            format!(
                r#"{{"path":"../web/src/**/*.{{ts,tsx}}","schema":"schema.json","generates":"{TYPES_FROM_STUDIO}"}}"#
            ),
        ),
        (
            "apps/studio/sanity.config.ts",
            "export default defineConfig({ projectId: process.env.SANITY_STUDIO_PROJECT_ID!, dataset: 'staging' })".into(),
        ),
        ("apps/web/package.json", r#"{"scripts":{"build":"next build"}}"#.into()),
        (
            "e2e/test-map.json",
            r#"{"apps/web/src/app/events/page.tsx":["e2e/events.spec.ts"]}"#.into(),
        ),
    ];
    let mut tree: Vec<String> = files.iter().map(|(p, _)| p.to_string()).collect();
    tree.extend(
        [
            "pnpm-lock.yaml",
            "apps/studio/schemaTypes/index.ts",
            "apps/studio/schemaTypes/blocks/hero.ts",
            SCHEMA_JSON,
            TYPES,
            "apps/web/src/app/events/page.tsx",
            "apps/web/src/blocks/hero.tsx",
            "e2e/events.spec.ts",
        ]
        .iter()
        .map(|p| p.to_string()),
    );
    RepoFiles {
        tree,
        files: files.into_iter().map(|(p, t)| (p.to_string(), t)).collect(),
    }
}

/// The studio's public codegen environment.
pub fn codegen_env() -> Vec<(String, String)> {
    vec![
        ("SANITY_STUDIO_PROJECT_ID".into(), "proj9".into()),
        ("SANITY_STUDIO_DATASET".into(), "staging".into()),
    ]
}

/// The plan for the card-block ticket: one task per file, the generated
/// types among them.
pub fn plan_tasks_md() -> String {
    [
        "- [ ] apps/studio/schemaTypes/blocks/card.ts: [create] Create the card block schema",
        "- [ ] apps/studio/schemaTypes/index.ts: [modify] Register the card block",
        "- [ ] apps/web/src/blocks/card.tsx: [create] Create the card component",
        "- [ ] apps/web/src/blocks/index.ts: [modify] Map the card block to its component",
        "- [ ] apps/web/src/lib/sanity/queries.ts: [modify] Fetch the card fields",
        "- [ ] e2e/events.spec.ts: [modify] Cover the card on the events page",
        &format!("- [ ] apps/studio/{TYPES_FROM_STUDIO}: Regenerate the types (generated)"),
    ]
    .join("\n")
}

// ── GitHub ─────────────────────────────────────────────────────────────────

/// An open pull request as the fake holds it.
#[derive(Debug, Clone)]
pub struct FakePr {
    pub number: u64,
    pub branch: String,
    pub body: String,
    pub draft: bool,
}

/// GitHub, in memory: the base branch, the run's branch and the open pull
/// requests. Every write names who made it, so a case can see whether two
/// writers touched one branch.
#[derive(Debug, Default)]
pub struct FakeGitHub {
    pub base: BTreeMap<String, String>,
    pub branches: HashMap<String, BTreeMap<String, String>>,
    pub prs: Vec<FakePr>,
    pub writes: Vec<(String, String)>,
}

impl FakeGitHub {
    /// The monorepo on `main`, and the run's branch cut from it.
    pub fn new() -> FakeGitHub {
        let base: BTreeMap<String, String> = monorepo()
            .tree
            .iter()
            .map(|p| (p.clone(), format!("// {p}\n")))
            .collect();
        let mut branches = HashMap::new();
        branches.insert(BRANCH.to_string(), base.clone());
        FakeGitHub {
            base,
            branches,
            ..Default::default()
        }
    }

    /// One commit by `writer` on `branch`.
    pub fn commit(&mut self, writer: &str, branch: &str, files: &[(&str, &str)]) {
        let b = self.branches.entry(branch.to_string()).or_default();
        for (p, c) in files {
            b.insert(p.to_string(), c.to_string());
        }
        self.writes.push((branch.to_string(), writer.to_string()));
    }

    /// `GET /repos/{o}/{r}/compare/{base}...{branch}`: the files the branch
    /// changes.
    pub fn compare(&self, branch: &str) -> Value {
        let empty = BTreeMap::new();
        let head = self.branches.get(branch).unwrap_or(&empty);
        let files: Vec<Value> = head
            .iter()
            .filter(|(p, c)| self.base.get(*p) != Some(*c))
            .map(|(p, _)| {
                json!({
                    "filename": p,
                    "status": if self.base.contains_key(p) { "modified" } else { "added" },
                })
            })
            .collect();
        json!({ "files": files, "commits": [] })
    }

    /// The branch's changed paths, as `plan_coverage::changed_files` reads
    /// the compare.
    pub fn changed(&self, branch: &str) -> Vec<String> {
        self.compare(branch)["files"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|f| f["filename"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `GET /repos/{o}/{r}/pulls?head={o}:{branch}&state=open`, first item.
    pub fn open_pr_for_branch(&self, branch: &str) -> Option<Value> {
        self.prs.iter().find(|p| p.branch == branch).map(|p| {
            json!({
                "number": p.number,
                "html_url": format!("https://github.com/{OWNER}/{REPO}/pull/{}", p.number),
                "title": "Add the card block",
                "draft": p.draft,
                "state": "open",
                "body": p.body,
                "head": { "ref": p.branch, "sha": "f00d" },
                "base": { "ref": BASE, "sha": "ba5e" },
            })
        })
    }

    pub fn pr(&self, number: u64) -> &FakePr {
        self.prs.iter().find(|p| p.number == number).unwrap()
    }

    pub fn pr_mut(&mut self, number: u64) -> &mut FakePr {
        self.prs.iter_mut().find(|p| p.number == number).unwrap()
    }
}

// ── The model ──────────────────────────────────────────────────────────────

/// A proof judge reply, as the model writes it: some prose, then the
/// findings in a fenced JSON block.
pub fn judge_reply(findings: &[(&str, &str)]) -> String {
    let list: Vec<Value> = findings
        .iter()
        .map(|(point, detail)| json!({"point": point, "detail": detail, "settle": ""}))
        .collect();
    format!(
        "I read the diff against the plan.\n\n```json\n{}\n```\n",
        json!({ "findings": list })
    )
}

// ── The sandbox ────────────────────────────────────────────────────────────

/// A real tar.gz of changed files, paths from the repository root, as the
/// codegen build packs them.
pub fn tarball(files: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut ar = tar::Builder::new(gz);
    for (path, data) in files {
        let mut h = tar::Header::new_gnu();
        h.set_size(data.len() as u64);
        h.set_mode(0o644);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        ar.append_data(&mut h, format!("./{path}"), data.as_slice())
            .unwrap();
    }
    let gz = ar.into_inner().unwrap();
    let mut out = gz.finish().unwrap();
    out.flush().unwrap();
    out
}

/// A CodeBuild log of a codegen build: CodeBuild echoes each buildspec
/// command before it runs it (the echo holds the marker text inside an
/// `echo "..."`), then the commands' own output follows.
pub fn codebuild_log(printed: &[&str]) -> String {
    let mut log = String::from(
        "[Container] 2026/10/28 03:21:02 Running command cd \"$SRC\" && sh -c \"$CODEGEN_CMD\"; CODE=$?; \
         echo \"CODERHELM_CODEGEN_END exit=$CODE\"; if [ $CODE -eq 0 ]; then \
         if aws s3 cp out.tgz \"$OUT\"; then echo \"===CODERHELM_CODEGEN_UPLOADED===\"; \
         else echo \"===CODERHELM_CODEGEN_UPLOAD_FAILED===\"; fi; fi\n\
         [Container] 2026/10/28 03:21:03 Running command if [ -f /tmp/egress.log ]; then grep \
         CODERHELM_EGRESS_BLOCKED /tmp/egress.log; fi\n\
         > studio@ typegen /codebuild/src/apps/studio\n\
         > sanity schema extract && sanity typegen generate\n",
    );
    for l in printed {
        log.push_str(l);
        log.push('\n');
    }
    log
}

/// The sandbox's codegen build, recorded: the archives it hands back, in
/// the order they are read (a retry reads the next), and its log.
pub struct FakeSandbox {
    archives: Mutex<VecDeque<Vec<u8>>>,
    pub log: String,
}

impl FakeSandbox {
    pub fn new(archives: Vec<Vec<u8>>, log: String) -> FakeSandbox {
        FakeSandbox {
            archives: Mutex::new(archives.into()),
            log,
        }
    }
}

#[async_trait::async_trait]
impl CodegenSource for FakeSandbox {
    async fn changed_files(&self, _out_key: &str) -> Captured {
        let next = self.archives.lock().unwrap().pop_front();
        match next {
            Some(bytes) if !bytes.is_empty() => sandbox::read_archive(&bytes).unwrap(),
            _ => Captured::default(),
        }
    }

    async fn build_log(&self, _group: &str, _stream: &str) -> String {
        self.log.clone()
    }

    fn log_retry_delay(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }
}

// ── DynamoDB ───────────────────────────────────────────────────────────────

/// A run record as the runs table holds it.
pub fn run_item(fields: &[(&str, AttributeValue)]) -> HashMap<String, AttributeValue> {
    fields
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

pub fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

pub fn n(v: i64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

// ── The run's end ──────────────────────────────────────────────────────────

/// How a run ended, as a person sees it: its status, its pull request (if
/// any), and every word it wrote (progress notes, the stop reason, the PR
/// description, ticket comments).
#[derive(Debug, Default, Clone)]
pub struct Ending {
    pub status: String,
    pub pr: Option<u64>,
    pub draft: bool,
    pub words: Vec<String>,
}

impl Ending {
    pub fn said(&mut self, text: impl Into<String>) {
        self.words.push(text.into());
    }

    /// The old failure never appears in anything the run wrote.
    pub fn never_says(&self, old: &str) {
        let old_l = old.to_lowercase();
        for w in &self.words {
            assert!(
                !w.to_lowercase().contains(&old_l),
                "the old failure is back: {old:?} in {w:?}"
            );
        }
    }

    pub fn says(&self, part: &str) -> bool {
        self.words.iter().any(|w| w.contains(part))
    }
}

/// `run_passes` (the single-repository pipeline) as source text, for the
/// checks that the pipeline still calls a decision at its place.
pub fn run_passes_src() -> &'static str {
    let src = include_str!("../mod.rs");
    let start = src.find("\nasync fn run_passes(").unwrap();
    let end = start + src[start + 1..].find("\nasync fn ").unwrap();
    &src[start..end]
}

/// One function of `src`, from its `fn` line to the next top-level item.
pub fn fn_src<'a>(src: &'a str, head: &str) -> &'a str {
    let start = src.find(head).unwrap_or_else(|| panic!("{head} is gone"));
    let rest = &src[start + head.len()..];
    let end = [
        "\nasync fn ",
        "\nfn ",
        "\npub(crate) ",
        "\npub fn ",
        "\n#[",
        "\n///",
    ]
    .iter()
    .filter_map(|m| rest.find(m))
    .min()
    .unwrap_or(rest.len());
    &src[start..start + head.len() + end]
}
