//! Finding the Terraform or OpenTofu code that defines a live resource
//! without Terraform's state: by resource type and attributes, never by
//! the literal name alone.
//!
//! - Every directory of `.tf` files is read as a module. A directory is
//!   seen once per way it is used: once per module call that points at it
//!   (with the caller's inputs), or, for a root, once per environment
//!   variable file (`prod.tfvars`, `envs/staging.tfvars`).
//! - A resource block of the right type is scored against the live
//!   resource in each of those: its identifier (a literal, a variable
//!   resolved through the call or the variable file, a prefix with
//!   Terraform's generated suffix), engine, version, tags, subnet group.
//!   A literal that contradicts the live resource rules the block out.
//! - The edit target is where the value is written, which may not be the
//!   resource block: a module call's input, a line of a variable file, a
//!   variable's default. A value several resources share is said so.
//! - A module whose source is a registry or a git address is matched on
//!   the call's own inputs; its resource block cannot be edited.
//!
//! State would answer this exactly (`terraform state list -id=...`), and
//! CoderHelm does not read state: what this finds is graded, and a low
//! grade never becomes a pull request.

use std::collections::BTreeMap;

use regex::Regex;

use super::hcl::{self, Attr, Block, Part, Value};
use super::inventory::{Confidence, Edit, Kind, Location, Resource};
use super::locate::{environment_of, SourceFile};

/// What is matched: the live resource's own values.
#[derive(Debug, Clone, Default)]
pub struct Live {
    pub kind: Kind,
    /// The names it may carry in code (its own, its launch template's).
    pub names: Vec<String>,
    pub engine: String,
    pub engine_version: String,
    pub ami_id: String,
    pub tags: BTreeMap<String, String>,
    pub facts: BTreeMap<String, String>,
}

impl Live {
    pub fn of(r: &Resource) -> Live {
        let mut names = vec![r.name.clone()];
        if let Some(lt) = r.tags.get("coderhelm:launch-template") {
            names.push(lt.clone());
        }
        names.retain(|n| !n.is_empty());
        Live {
            kind: r.kind,
            names,
            engine: r.versions.engine.clone(),
            engine_version: r.versions.engine_version.clone(),
            ami_id: r.versions.ami_id.clone(),
            tags: r
                .tags
                .iter()
                .filter(|(k, _)| !k.starts_with("aws:") && !k.starts_with("coderhelm:"))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            facts: r.facts.clone(),
        }
    }
}

/// What a kind of resource looks like in Terraform.
pub struct Spec {
    pub types: &'static [&'static str],
    /// Attributes that hold its identifier, and a prefix of it.
    pub names: &'static [&'static str],
    pub prefixes: &'static [&'static str],
    /// The attribute a version change edits.
    pub version: &'static str,
    /// Words in a module's source that say it makes this kind.
    pub module_words: &'static [&'static str],
}

pub fn spec(kind: Kind) -> Option<Spec> {
    Some(match kind {
        Kind::RdsInstance => Spec {
            types: &["aws_db_instance"],
            names: &["identifier"],
            prefixes: &["identifier_prefix"],
            version: "engine_version",
            module_words: &["rds", "db", "postgres", "mysql", "database"],
        },
        Kind::RdsCluster => Spec {
            types: &["aws_rds_cluster"],
            names: &["cluster_identifier"],
            prefixes: &["cluster_identifier_prefix"],
            version: "engine_version",
            module_words: &["aurora", "rds", "cluster", "database"],
        },
        Kind::ElasticacheGroup => Spec {
            types: &[
                "aws_elasticache_replication_group",
                "aws_elasticache_cluster",
            ],
            names: &["replication_group_id", "cluster_id"],
            prefixes: &[],
            version: "engine_version",
            module_words: &["elasticache", "redis", "valkey", "cache"],
        },
        Kind::OpensearchDomain => Spec {
            types: &["aws_opensearch_domain", "aws_elasticsearch_domain"],
            names: &["domain_name"],
            prefixes: &[],
            version: "engine_version",
            module_words: &["opensearch", "elasticsearch"],
        },
        Kind::EksNodegroup => Spec {
            types: &["aws_eks_node_group"],
            names: &["node_group_name"],
            prefixes: &["node_group_name_prefix"],
            version: "release_version",
            module_words: &["eks", "node"],
        },
        Kind::LambdaFunction => Spec {
            types: &["aws_lambda_function"],
            names: &["function_name"],
            prefixes: &[],
            version: "runtime",
            module_words: &["lambda"],
        },
        Kind::AutoScalingGroup | Kind::Ec2Instance => Spec {
            types: &[
                "aws_launch_template",
                "aws_autoscaling_group",
                "aws_instance",
            ],
            names: &["name"],
            prefixes: &["name_prefix"],
            version: "image_id",
            module_words: &["autoscaling", "asg", "ec2"],
        },
        _ => return None,
    })
}

fn re(p: &str) -> Regex {
    Regex::new(p).expect("a valid pattern")
}

/// The prefix of a name Terraform generated: the provider appends a
/// timestamp and a counter, 26 characters, to `terraform-` or to the
/// `*_prefix` argument. `None` for a name a person chose.
pub fn generated_prefix(name: &str) -> Option<&str> {
    let at = name.len().checked_sub(26)?;
    let (prefix, tail) = name.split_at_checked(at)?;
    re(r"^\d{18}[0-9a-f]{8}$")
        .is_match(tail)
        .then_some(prefix)
        .filter(|p| !p.is_empty())
}

#[derive(Debug, Clone)]
struct Origin {
    file: String,
    line: u32,
    /// The name the value is written under there.
    name: String,
    via: &'static str,
}

#[derive(Debug, Clone)]
enum Val {
    /// A value read all the way to a literal, and where the literal is.
    Known(String, Origin),
    /// A string with parts that could not be read: a pattern the live
    /// value must fit, and the text as written.
    Pattern(String, String),
    Unknown(String),
}

/// One directory of `.tf` files.
struct Dir<'a> {
    repo: &'a str,
    dir: String,
    files: Vec<(&'a str, Vec<Block>)>,
    /// Variable files in or under the directory.
    tfvars: Vec<(&'a str, Vec<Attr>)>,
}

impl Dir<'_> {
    fn blocks(&self) -> impl Iterator<Item = (&str, &Block)> {
        self.files
            .iter()
            .flat_map(|(p, bs)| bs.iter().map(move |b| (*p, b)))
    }
    fn variable_default(&self, name: &str) -> Option<(&str, &Attr)> {
        self.blocks()
            .filter(|(_, b)| b.kind == "variable" && b.label(0) == name)
            .find_map(|(p, b)| b.attr("default").map(|a| (p, a)))
    }
    fn local(&self, name: &str) -> Option<(&str, &Attr)> {
        self.blocks()
            .filter(|(_, b)| b.kind == "locals")
            .find_map(|(p, b)| b.attr(name).map(|a| (p, a)))
    }
    /// How many resource and module blocks read `var.<name>`.
    fn readers(&self, name: &str) -> usize {
        let needle = format!("var.{name}");
        let word = re(&format!(r"\bvar\.{}\b", regex::escape(name)));
        self.blocks()
            .filter(|(_, b)| matches!(b.kind.as_str(), "resource" | "module"))
            .filter(|(_, b)| {
                fn any(b: &Block, needle: &str, word: &Regex) -> bool {
                    b.attrs
                        .iter()
                        .any(|a| a.expr.contains(needle) && word.is_match(&a.expr))
                        || b.blocks.iter().any(|c| any(c, needle, word))
                }
                any(b, &needle, &word)
            })
            .count()
    }
}

/// One way a directory is used.
#[derive(Debug, Clone, Default)]
struct Ctx {
    vars: BTreeMap<String, Val>,
    /// `module.db.` for a module called as `db`.
    prefix: String,
    env: String,
    /// The variable file or call chain, for the evidence.
    label: String,
    /// The module is called more than once, or with `for_each`.
    repeated: bool,
}

fn dir_of(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(d, _)| d)
        .unwrap_or("")
        .to_string()
}

/// `a/b` + `../c` is `a/c`.
fn join(dir: &str, rel: &str) -> String {
    let mut parts: Vec<&str> = dir.split('/').filter(|p| !p.is_empty()).collect();
    for p in rel.split('/') {
        match p {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            x => parts.push(x),
        }
    }
    parts.join("/")
}

fn is_tf(path: &str) -> bool {
    path.ends_with(".tf") || path.ends_with(".tofu")
}

fn is_tfvars(path: &str) -> bool {
    path.ends_with(".tfvars")
}

fn dirs<'a>(files: &'a [SourceFile]) -> Vec<Dir<'a>> {
    let mut out: Vec<Dir<'a>> = Vec::new();
    for f in files.iter().filter(|f| is_tf(&f.path)) {
        let d = dir_of(&f.path);
        let blocks = hcl::parse(&f.content);
        match out.iter_mut().find(|x| x.repo == f.repo && x.dir == d) {
            Some(x) => x.files.push((&f.path, blocks)),
            None => out.push(Dir {
                repo: &f.repo,
                dir: d,
                files: vec![(&f.path, blocks)],
                tfvars: Vec::new(),
            }),
        }
    }
    // A variable file belongs to the nearest directory of `.tf` files at
    // or above it.
    for f in files.iter().filter(|f| is_tfvars(&f.path)) {
        let mut d = dir_of(&f.path);
        loop {
            if let Some(x) = out.iter_mut().find(|x| x.repo == f.repo && x.dir == d) {
                x.tfvars.push((&f.path, hcl::assignments(&f.content)));
                break;
            }
            if d.is_empty() {
                break;
            }
            d = dir_of(&d);
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn resolve(
    expr: &str,
    file: &str,
    line: u32,
    name: &str,
    via: &'static str,
    ctx: &Ctx,
    dir: &Dir,
    depth: u8,
) -> Val {
    if depth > 6 {
        return Val::Unknown(expr.to_string());
    }
    match hcl::value(expr) {
        Value::Lit(s) => Val::Known(
            s,
            Origin {
                file: file.to_string(),
                line,
                name: name.to_string(),
                via,
            },
        ),
        Value::Var(v) => match ctx.vars.get(&v) {
            Some(x) => x.clone(),
            None => match dir.variable_default(&v) {
                Some((p, a)) => resolve(
                    &a.expr,
                    p,
                    a.line,
                    &v,
                    "variable_default",
                    ctx,
                    dir,
                    depth + 1,
                ),
                None => Val::Unknown(expr.to_string()),
            },
        },
        Value::Local(v) => match dir.local(&v) {
            Some((p, a)) => resolve(&a.expr, p, a.line, &v, "local", ctx, dir, depth + 1),
            None => Val::Unknown(expr.to_string()),
        },
        Value::Template(parts) => {
            let mut text = String::new();
            let mut pattern = String::from("^");
            let mut known = true;
            for p in parts {
                match p {
                    Part::Text(t) => {
                        text.push_str(&t);
                        pattern.push_str(&regex::escape(&t));
                    }
                    Part::Ref(r) => match resolve(&r, file, line, name, via, ctx, dir, depth + 1) {
                        Val::Known(v, _) => {
                            text.push_str(&v);
                            pattern.push_str(&regex::escape(&v));
                        }
                        _ => {
                            known = false;
                            pattern.push_str(".+");
                        }
                    },
                }
            }
            if known {
                Val::Known(
                    text,
                    Origin {
                        file: file.to_string(),
                        line,
                        name: name.to_string(),
                        via,
                    },
                )
            } else {
                pattern.push('$');
                Val::Pattern(pattern, expr.to_string())
            }
        }
        Value::Each(t) | Value::Other(t) => Val::Unknown(t),
    }
}

const META: &[&str] = &[
    "source",
    "version",
    "count",
    "for_each",
    "providers",
    "depends_on",
];

/// Every way `dirs[at]` is used: per module call that points at it, or per
/// environment variable file when nothing calls it.
fn contexts(dirs: &[Dir], at: usize, depth: u8) -> Vec<Ctx> {
    let me = &dirs[at];
    let mut out = Vec::new();
    if depth < 4 {
        for (ci, c) in dirs.iter().enumerate().filter(|(_, c)| c.repo == me.repo) {
            for (file, m) in c.blocks().filter(|(_, b)| b.kind == "module") {
                let Some(Value::Lit(src)) = m.attr("source").map(|a| hcl::value(&a.expr)) else {
                    continue;
                };
                if !(src.starts_with("./") || src.starts_with("../"))
                    || join(&c.dir, &src) != me.dir
                {
                    continue;
                }
                if ci == at {
                    continue;
                }
                let many = m.attr("for_each").is_some() || m.attr("count").is_some();
                for cc in contexts(dirs, ci, depth + 1) {
                    let mut vars = BTreeMap::new();
                    for a in m.attrs.iter().filter(|a| !META.contains(&a.name.as_str())) {
                        vars.insert(
                            a.name.clone(),
                            resolve(&a.expr, file, a.line, &a.name, "module_input", &cc, c, 0),
                        );
                    }
                    let env = if cc.env.is_empty() {
                        environment_of(&[m.label(0), &c.dir])
                    } else {
                        cc.env.clone()
                    };
                    out.push(Ctx {
                        vars,
                        prefix: format!("{}module.{}.", cc.prefix, m.label(0)),
                        env,
                        label: if cc.label.is_empty() {
                            format!("module `{}` in {file}", m.label(0))
                        } else {
                            format!("module `{}` in {file}, with {}", m.label(0), cc.label)
                        },
                        repeated: many || cc.repeated,
                    });
                }
            }
        }
    }
    if !out.is_empty() {
        let n = out.len();
        if n > 1 {
            // Several calls of one module: an edit inside the module is
            // shared by all of them.
            for c in out.iter_mut() {
                c.repeated = true;
            }
        }
        return out;
    }
    // A root: what its variable files set.
    let set = |path: &str, attrs: &[Attr], into: &mut BTreeMap<String, Val>| {
        for a in attrs {
            let v = match hcl::value(&a.expr) {
                Value::Lit(s) => Val::Known(
                    s,
                    Origin {
                        file: path.to_string(),
                        line: a.line,
                        name: a.name.clone(),
                        via: "tfvars",
                    },
                ),
                _ => Val::Unknown(a.expr.clone()),
            };
            into.insert(a.name.clone(), v);
        }
    };
    let always = |p: &str| {
        dir_of(p) == me.dir && (p.ends_with("terraform.tfvars") || p.ends_with(".auto.tfvars"))
    };
    let mut base = BTreeMap::new();
    for (p, attrs) in me.tfvars.iter().filter(|(p, _)| always(p)) {
        set(p, attrs, &mut base);
    }
    let per_env: Vec<&(&str, Vec<Attr>)> = me.tfvars.iter().filter(|(p, _)| !always(p)).collect();
    if per_env.is_empty() {
        return vec![Ctx {
            vars: base,
            env: environment_of(&[&me.dir]),
            ..Default::default()
        }];
    }
    per_env
        .into_iter()
        .map(|(p, attrs)| {
            let mut vars = base.clone();
            set(p, attrs, &mut vars);
            Ctx {
                vars,
                env: environment_of(&[p]),
                label: (*p).to_string(),
                ..Default::default()
            }
        })
        .collect()
}

/// One place that could define the resource.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub score: i32,
    pub location: Location,
}

struct Scored {
    score: i32,
    evidence: Vec<String>,
    how: &'static str,
}

fn starts_with_version(live: &str, code: &str) -> bool {
    !code.is_empty()
        && (live == code || live.starts_with(&format!("{code}.")) || live.starts_with(code))
}

/// Scores the attributes `get` returns against the live resource. `None`
/// when a literal contradicts it.
#[allow(clippy::too_many_arguments)]
fn score(
    live: &Live,
    sp: &Spec,
    names: &[&str],
    prefixes: &[&str],
    get: &dyn Fn(&str) -> Option<Val>,
    has: &dyn Fn(&str) -> bool,
    repeated_block: bool,
    tags: &[(String, Val)],
) -> Option<Scored> {
    // An instance group's blocks (launch template, group) each carry a
    // name of their own: a name that differs rules nothing out there.
    let strict = !matches!(live.kind, Kind::AutoScalingGroup | Kind::Ec2Instance);
    let repeated_block = repeated_block || !strict;
    let mut s = Scored {
        score: 0,
        evidence: Vec::new(),
        how: "terraform_attributes",
    };
    let gen = live.names.iter().find_map(|n| generated_prefix(n));
    let mut named = false;
    for n in names {
        match get(n) {
            Some(Val::Known(v, o)) => {
                if live.names.iter().any(|x| x == &v) {
                    s.score += 100;
                    s.how = "terraform_name";
                    s.evidence.push(format!(
                        "`{n}` is `{v}`, the live name ({})",
                        if o.via == "resource" {
                            "written in the block".to_string()
                        } else {
                            format!("from `{}` in {}", o.name, o.file)
                        }
                    ));
                    named = true;
                } else if !repeated_block {
                    return None;
                }
            }
            Some(Val::Pattern(p, text)) => {
                let fits = Regex::new(&p)
                    .ok()
                    .is_some_and(|r| live.names.iter().any(|x| r.is_match(x)));
                if fits {
                    s.score += 40;
                    s.evidence.push(format!(
                        "`{n}` is written as `{text}`, which the live name fits; part of it is set somewhere CoderHelm did not read"
                    ));
                    named = true;
                } else if !repeated_block {
                    return None;
                }
            }
            Some(Val::Unknown(text)) => {
                if !strict {
                } else if text.starts_with("each.") || text.starts_with("count.") || repeated_block
                {
                    s.score += 15;
                    s.evidence.push(format!(
                        "`{n}` comes from `{text}`: the block creates several resources and CoderHelm cannot tell which one is this"
                    ));
                } else {
                    s.evidence
                        .push(format!("`{n}` is `{text}`, which CoderHelm could not read"));
                }
            }
            None => {}
        }
    }
    for n in prefixes {
        match (get(n), gen) {
            (Some(Val::Known(v, _)), Some(g)) if v == g => {
                s.score += 90;
                s.how = "terraform_generated_name";
                s.evidence.push(format!(
                    "`{n}` is `{v}`, and the live name is that prefix plus the suffix Terraform generates"
                ));
                named = true;
            }
            (Some(Val::Known(_, _)), _) if !repeated_block => return None,
            (Some(Val::Pattern(p, text)), Some(g))
                if Regex::new(&p).ok().is_some_and(|r| r.is_match(g)) =>
            {
                s.score += 35;
                s.how = "terraform_generated_name";
                s.evidence.push(format!(
                    "`{n}` is written as `{text}`, which the generated live name's prefix fits"
                ));
                named = true;
            }
            _ => {}
        }
    }
    let sets_name = names.iter().chain(prefixes.iter()).any(|n| has(n));
    if !named && !sets_name && gen == Some("terraform-") {
        s.score += 30;
        s.how = "terraform_generated_name";
        s.evidence.push(
            "the live name is one Terraform generated (`terraform-` and a timestamp), and this block sets no identifier".into(),
        );
    } else if !named && !sets_name && gen.is_none() && !repeated_block {
        // A name a person chose, and a block that would get a generated one.
        return None;
    }
    if !live.engine.is_empty() {
        if let Some(Val::Known(v, _)) = get("engine") {
            if v == live.engine {
                s.score += 15;
                s.evidence.push(format!("`engine` is `{v}`"));
            } else {
                return None;
            }
        }
    }
    if !live.engine_version.is_empty() && sp.version == "engine_version" {
        if let Some(Val::Known(v, _)) = get("engine_version") {
            if starts_with_version(&live.engine_version, &v) {
                s.score += 10;
                s.evidence.push(format!(
                    "`engine_version` is `{v}` (it runs {})",
                    live.engine_version
                ));
            } else {
                s.evidence.push(format!(
                    "`engine_version` is `{v}` while it runs {}",
                    live.engine_version
                ));
            }
        }
    }
    if !live.ami_id.is_empty() {
        if let Some(Val::Known(v, _)) = get("image_id") {
            if v == live.ami_id {
                s.score += 100;
                s.how = "terraform_attributes";
                s.evidence
                    .push(format!("`image_id` is `{v}`, the AMI that runs"));
            }
        }
    }
    for (fact, attr, points) in [
        ("subnet_group", "db_subnet_group_name", 15),
        ("subnet_group", "subnet_group_name", 15),
        ("parameter_group", "parameter_group_name", 10),
        ("parameter_group", "db_cluster_parameter_group_name", 10),
        ("instance_class", "instance_class", 5),
        ("cluster", "cluster_name", 15),
    ] {
        let Some(want) = live.facts.get(fact) else {
            continue;
        };
        if let Some(Val::Known(v, _)) = get(attr) {
            if &v == want {
                s.score += points;
                s.evidence.push(format!("`{attr}` is `{v}`"));
            } else if points >= 15 {
                s.score -= 20;
                s.evidence
                    .push(format!("`{attr}` is `{v}`, the live one is `{want}`"));
            }
        }
    }
    let mut tag_points = 0;
    for (k, v) in tags {
        let Some(want) = live.tags.get(k) else {
            continue;
        };
        if let Val::Known(v, _) = v {
            if v == want {
                if tag_points < 30 {
                    tag_points += 10;
                    s.evidence.push(format!("tag `{k}` is `{v}`"));
                }
            } else {
                s.score -= 20;
                s.evidence
                    .push(format!("tag `{k}` is `{v}`, the live tag is `{want}`"));
            }
        }
    }
    s.score += tag_points;
    (s.score >= 30).then_some(s)
}

fn edit_from(
    val: Option<Val>,
    file: &str,
    line: u32,
    attribute: &str,
    evidence: &mut Vec<String>,
) -> Option<Edit> {
    match val {
        Some(Val::Known(v, o)) => Some(Edit {
            file: o.file,
            line: o.line,
            attribute: o.name,
            value: v,
            via: o.via.to_string(),
            shared: String::new(),
        }),
        Some(Val::Pattern(_, text)) | Some(Val::Unknown(text)) => {
            evidence.push(format!(
                "`{attribute}` is `{text}`: CoderHelm could not follow it to the line that sets the value"
            ));
            None
        }
        None => Some(Edit {
            file: file.to_string(),
            line,
            attribute: attribute.to_string(),
            value: String::new(),
            via: "add".into(),
            shared: String::new(),
        }),
    }
}

fn tags_of(b: &Block, file: &str, ctx: &Ctx, dir: &Dir) -> Vec<(String, Val)> {
    let Some(t) = b.attr("tags") else {
        return Vec::new();
    };
    hcl::object(&t.expr, t.line)
        .into_iter()
        .map(|a| {
            let v = resolve(&a.expr, file, a.line, &a.name, "resource", ctx, dir, 0);
            (a.name, v)
        })
        .collect()
}

/// Every place in `files` that could define the live resource, best first.
pub fn candidates(live: &Live, files: &[SourceFile]) -> Vec<Candidate> {
    candidates_in(live, &index(files))
}

/// A team's Terraform files, parsed once for every resource placed
/// against them.
pub struct Index<'a> {
    dirs: Vec<Dir<'a>>,
}

/// Parses the Terraform and OpenTofu files among `files`.
pub fn index(files: &[SourceFile]) -> Index<'_> {
    Index { dirs: dirs(files) }
}

/// Every place in a parsed set of files that could define the live
/// resource, best first.
pub fn candidates_in(live: &Live, index: &Index) -> Vec<Candidate> {
    let Some(sp) = spec(live.kind) else {
        return Vec::new();
    };
    let dirs = &index.dirs;
    let mut out: Vec<Candidate> = Vec::new();
    for (at, d) in dirs.iter().enumerate() {
        let wanted: Vec<(&str, &Block)> = d
            .blocks()
            .filter(|(_, b)| b.kind == "resource" && sp.types.contains(&b.label(0)))
            .collect();
        let remote: Vec<(&str, &Block)> = d
            .blocks()
            .filter(|(_, b)| b.kind == "module")
            .filter(|(_, b)| {
                matches!(b.attr("source").map(|a| hcl::value(&a.expr)),
                    Some(Value::Lit(s)) if !(s.starts_with("./") || s.starts_with("../"))
                        && sp.module_words.iter().any(|w| s.to_ascii_lowercase().contains(w)))
            })
            .collect();
        if wanted.is_empty() && remote.is_empty() {
            continue;
        }
        let ctxs = contexts(dirs, at, 0);
        let opentofu = d.files.iter().any(|(p, _)| p.ends_with(".tofu"));
        for ctx in &ctxs {
            for (file, b) in &wanted {
                let many = b.attr("for_each").is_some() || b.attr("count").is_some();
                let get = |n: &str| {
                    b.attr(n)
                        .map(|a| resolve(&a.expr, file, a.line, n, "resource", ctx, d, 0))
                };
                let has = |n: &str| b.attr(n).is_some();
                let tags = tags_of(b, file, ctx, d);
                let Some(mut s) = score(live, &sp, sp.names, sp.prefixes, &get, &has, many, &tags)
                else {
                    continue;
                };
                let version = if b.label(0) == "aws_elasticsearch_domain" {
                    "elasticsearch_version"
                } else {
                    sp.version
                };
                let mut edit = edit_from(get(version), file, b.line, version, &mut s.evidence);
                if let Some(e) = edit.as_mut() {
                    e.shared = shared_note(e, b, many, ctx, d);
                }
                if !ctx.prefix.is_empty() {
                    s.how = "terraform_module";
                }
                if !ctx.label.is_empty() {
                    s.evidence.push(format!("read through {}", ctx.label));
                }
                out.push(Candidate {
                    score: s.score,
                    location: Location {
                        repo: d.repo.to_string(),
                        file: (*file).to_string(),
                        address: format!("{}{}.{}", ctx.prefix, b.label(0), b.label(1)),
                        line: b.line,
                        how: s.how.into(),
                        tool: if opentofu { "opentofu" } else { "terraform" }.into(),
                        confidence: Confidence::Low,
                        evidence: s.evidence,
                        edit,
                        environment: ctx.env.clone(),
                        ..Default::default()
                    },
                });
            }
            for (file, m) in &remote {
                let many = m.attr("for_each").is_some() || m.attr("count").is_some();
                let get = |n: &str| {
                    m.attr(n)
                        .map(|a| resolve(&a.expr, file, a.line, n, "module_input", ctx, d, 0))
                };
                let has = |n: &str| m.attr(n).is_some();
                let mut names: Vec<&str> = sp.names.to_vec();
                for n in ["name", "identifier", "cluster_name"] {
                    if !names.contains(&n) {
                        names.push(n);
                    }
                }
                let tags = tags_of(m, file, ctx, d);
                // A module may turn its name into a prefix; a call that
                // sets a name is never ruled out for lacking a prefix.
                let Some(mut s) = score(
                    live,
                    &sp,
                    &names,
                    &[],
                    &get,
                    &|n| has(n) || names.iter().any(|x| has(x)),
                    many,
                    &tags,
                )
                .or_else(|| prefix_through_module(live, &names, &get)) else {
                    continue;
                };
                let source = match m.attr("source").map(|a| hcl::value(&a.expr)) {
                    Some(Value::Lit(x)) => x,
                    _ => String::new(),
                };
                s.how = "terraform_module";
                s.evidence.push(format!(
                    "defined through the module `{source}`: the resource block is inside the module, so the edit is this call's input"
                ));
                let version = [
                    sp.version,
                    "engine_version",
                    "ami_release_version",
                    "cluster_version",
                ]
                .into_iter()
                .find(|v| m.attr(v).is_some())
                .unwrap_or(sp.version);
                let mut edit = edit_from(get(version), file, m.line, version, &mut s.evidence);
                if let Some(e) = edit.as_mut() {
                    if e.via == "add" {
                        e.via = "module_input".into();
                        s.evidence.push(format!(
                            "the call does not set `{version}`: the module's own default applies; check the module's inputs for the name it uses"
                        ));
                    }
                    e.shared = shared_note(e, m, many, ctx, d);
                }
                out.push(Candidate {
                    score: s.score.min(95),
                    location: Location {
                        repo: d.repo.to_string(),
                        file: (*file).to_string(),
                        address: format!("{}module.{}", ctx.prefix, m.label(0)),
                        line: m.line,
                        how: s.how.into(),
                        tool: if opentofu { "opentofu" } else { "terraform" }.into(),
                        confidence: Confidence::Low,
                        evidence: s.evidence,
                        edit,
                        environment: if ctx.env.is_empty() {
                            environment_of(&[m.label(0)])
                        } else {
                            ctx.env.clone()
                        },
                        ..Default::default()
                    },
                });
            }
        }
    }
    out.sort_by_key(|c| std::cmp::Reverse(c.score));
    out
}

/// A registry module that is given a name and makes it a prefix: the live
/// name is the call's name, a dash, and Terraform's generated suffix.
fn prefix_through_module(
    live: &Live,
    names: &[&str],
    get: &dyn Fn(&str) -> Option<Val>,
) -> Option<Scored> {
    let g = live.names.iter().find_map(|n| generated_prefix(n))?;
    for n in names {
        if let Some(Val::Known(v, _)) = get(n) {
            if g == v || g.trim_end_matches('-') == v {
                return Some(Scored {
                    score: 70,
                    evidence: vec![format!(
                        "the call's `{n}` is `{v}`, and the live name is that name plus the suffix Terraform generates"
                    )],
                    how: "terraform_module",
                });
            }
        }
    }
    None
}

fn shared_note(e: &Edit, block: &Block, many: bool, ctx: &Ctx, d: &Dir) -> String {
    match e.via.as_str() {
        "tfvars" | "variable_default" => {
            let n = d.readers(&e.attribute);
            let mut s = String::new();
            if n > 1 {
                s = format!("{n} blocks read `var.{}`: the change reaches all of them", e.attribute);
            }
            if e.via == "variable_default" {
                if !s.is_empty() {
                    s.push_str(". ");
                }
                s.push_str("A variable's default applies wherever nothing else sets it");
                if ctx.repeated {
                    s.push_str(", in every use of this module");
                }
            }
            s
        }
        "resource" if many => format!(
            "`{}.{}` creates several resources (`for_each` or `count`): a value written in the block applies to every one",
            block.label(0),
            block.label(1)
        ),
        "resource" | "add" if ctx.repeated => {
            "the block is inside a module used more than once: a value written here applies to every use".into()
        }
        "module_input" if many => {
            "the module call creates several copies (`for_each` or `count`): this input applies to every one".into()
        }
        _ => String::new(),
    }
}

/// The place that defines the live resource, graded. `None` when nothing
/// fits. A tie between places, or weak signs only, is `Low`, with the
/// other places listed.
pub fn locate(live: &Live, files: &[SourceFile]) -> Option<Location> {
    locate_in(live, &index(files))
}

/// [`locate`] against files parsed once ([`index`]).
pub fn locate_in(live: &Live, index: &Index) -> Option<Location> {
    let all = candidates_in(live, index);
    let best = all.first()?;
    let rivals: Vec<&Candidate> = all
        .iter()
        .skip(1)
        .filter(|c| c.score > best.score - 30)
        .collect();
    let mut loc = best.location.clone();
    loc.confidence = if best.score >= 90 && rivals.is_empty() {
        Confidence::High
    } else if best.score >= 50 && rivals.is_empty() {
        Confidence::Medium
    } else {
        Confidence::Low
    };
    if !rivals.is_empty() {
        loc.evidence.push(format!(
            "{} other place{} fit{} about as well; CoderHelm cannot tell them apart without Terraform's state",
            rivals.len(),
            if rivals.len() == 1 { "" } else { "s" },
            if rivals.len() == 1 { "s" } else { "" },
        ));
        loc.candidates = rivals
            .iter()
            .take(5)
            .map(|c| {
                format!(
                    "{}:{} {}{}",
                    c.location.repo,
                    c.location.file,
                    c.location.address,
                    if c.location.environment.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", c.location.environment)
                    }
                )
            })
            .collect();
    } else if loc.confidence == Confidence::Low {
        loc.evidence.push(
            "only the resource type and a few settings fit; the identifier is not written in the code CoderHelm read".into(),
        );
    }
    if loc.edit.is_none() {
        // Found, but not the line to change: not enough for a pull request.
        loc.confidence = loc.confidence.min(Confidence::Low);
    }
    Some(loc)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, content: &str) -> SourceFile {
        SourceFile {
            repo: "acme/infra".into(),
            path: path.into(),
            content: content.into(),
        }
    }

    fn db(name: &str, engine: &str, version: &str) -> Live {
        Live {
            kind: Kind::RdsInstance,
            names: vec![name.into()],
            engine: engine.into(),
            engine_version: version.into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_resource_written_with_its_identifier_is_found_with_the_line_to_edit() {
        let f = file(
            "db/main.tf",
            r#"
resource "aws_db_instance" "billing" {
  identifier     = "billing-prod"
  engine         = "mysql"
  engine_version = "8.0.36"
}

resource "aws_db_instance" "reports" {
  identifier     = "reports-prod"
  engine         = "mysql"
  engine_version = "8.0.36"
}
"#,
        );
        let l = locate(&db("billing-prod", "mysql", "8.0.36"), &[f]).unwrap();
        assert_eq!(l.address, "aws_db_instance.billing");
        assert_eq!(
            (l.how.as_str(), l.confidence),
            ("terraform_name", Confidence::High)
        );
        assert_eq!(l.tool, "terraform");
        let e = l.edit.unwrap();
        assert_eq!(
            (
                e.file.as_str(),
                e.line,
                e.attribute.as_str(),
                e.value.as_str(),
                e.via.as_str()
            ),
            ("db/main.tf", 5, "engine_version", "8.0.36", "resource")
        );
        assert!(
            l.candidates.is_empty(),
            "a block naming another database is ruled out"
        );
    }

    #[test]
    fn a_generated_identifier_is_matched_by_prefix_and_attributes_not_by_name() {
        let live_name = "orders-20240311094512345600000001";
        assert_eq!(generated_prefix(live_name), Some("orders-"));
        assert_eq!(
            generated_prefix("terraform-20240311094512345600000001"),
            Some("terraform-")
        );
        assert_eq!(generated_prefix("orders-prod"), None);
        let f = file(
            "main.tf",
            r#"
resource "aws_db_instance" "orders" {
  identifier_prefix = "orders-"
  engine            = "postgres"
  engine_version    = "16.4"
  tags = { Team = "payments" }
}
resource "aws_db_instance" "audit" {
  identifier_prefix = "audit-"
  engine            = "postgres"
  engine_version    = "16.4"
}
"#,
        );
        let mut live = db(live_name, "postgres", "16.4");
        live.tags.insert("Team".into(), "payments".into());
        let l = locate(&live, std::slice::from_ref(&f)).unwrap();
        assert_eq!(l.address, "aws_db_instance.orders");
        assert_eq!(
            (l.how.as_str(), l.confidence),
            ("terraform_generated_name", Confidence::High)
        );
        assert!(
            l.evidence
                .iter()
                .any(|e| e.contains("suffix Terraform generates")),
            "{:?}",
            l.evidence
        );
        // The literal live name is nowhere in the file.
        assert!(!f.content.contains(live_name));
    }

    #[test]
    fn a_default_generated_name_with_two_blocks_that_fit_is_low_and_says_why() {
        let f = file(
            "main.tf",
            r#"
resource "aws_db_instance" "a" {
  engine = "postgres"
  engine_version = "16.4"
}
resource "aws_db_instance" "b" {
  engine = "postgres"
  engine_version = "16.4"
}
"#,
        );
        let l = locate(
            &db("terraform-20240311094512345600000001", "postgres", "16.4"),
            &[f],
        )
        .unwrap();
        assert_eq!(l.confidence, Confidence::Low);
        assert!(!l.sure(), "never a pull request");
        assert_eq!(l.candidates.len(), 1);
        assert!(
            l.evidence
                .iter()
                .any(|e| e.contains("cannot tell them apart")),
            "{:?}",
            l.evidence
        );
        // One block that fits, with the engine and version too: medium.
        let one = file(
            "main.tf",
            "resource \"aws_db_instance\" \"a\" {\n  engine = \"postgres\"\n  engine_version = \"16.4\"\n}\n",
        );
        let l = locate(
            &db("terraform-20240311094512345600000001", "postgres", "16.4"),
            &[one],
        )
        .unwrap();
        assert_eq!(l.confidence, Confidence::Medium);
        // Another engine: ruled out.
        let other = file(
            "main.tf",
            "resource \"aws_db_instance\" \"a\" {\n  engine = \"mysql\"\n}\n",
        );
        assert!(locate(
            &db("terraform-20240311094512345600000001", "postgres", "16.4"),
            &[other]
        )
        .is_none());
    }

    const MODULE: &str = r#"
variable "name" {}
variable "engine_version" {
  default = "16.4"
}
resource "aws_rds_cluster" "this" {
  cluster_identifier = "${var.name}-aurora"
  engine             = "aurora-postgresql"
  engine_version     = var.engine_version
}
resource "aws_rds_cluster_instance" "this" {
  count              = 2
  cluster_identifier = aws_rds_cluster.this.id
  engine_version     = aws_rds_cluster.this.engine_version
}
"#;

    fn cluster(name: &str) -> Live {
        Live {
            kind: Kind::RdsCluster,
            names: vec![name.into()],
            engine: "aurora-postgresql".into(),
            engine_version: "16.4".into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_cluster_defined_through_a_local_module_edits_the_callers_input() {
        let files = vec![
            file("modules/aurora/main.tf", MODULE),
            file(
                "envs/prod/main.tf",
                "module \"ledger\" {\n  source         = \"../../modules/aurora\"\n  name           = \"ledger-prod\"\n  engine_version = \"16.4\"\n}\n",
            ),
            file(
                "envs/staging/main.tf",
                "module \"ledger\" {\n  source         = \"../../modules/aurora\"\n  name           = \"ledger-staging\"\n  engine_version = \"16.4\"\n}\n",
            ),
        ];
        let l = locate(&cluster("ledger-prod-aurora"), &files).unwrap();
        assert_eq!(l.address, "module.ledger.aws_rds_cluster.this");
        assert_eq!(
            (l.how.as_str(), l.confidence),
            ("terraform_module", Confidence::High)
        );
        assert_eq!(l.environment, "production");
        // The edit is the caller's input, not the module's resource block
        // and never the cluster instance.
        let e = l.edit.unwrap();
        assert_eq!(
            (
                e.file.as_str(),
                e.line,
                e.attribute.as_str(),
                e.via.as_str()
            ),
            ("envs/prod/main.tf", 4, "engine_version", "module_input")
        );
        let s = locate(&cluster("ledger-staging-aurora"), &files).unwrap();
        assert_eq!(s.environment, "staging");
        assert_eq!(s.edit.unwrap().file, "envs/staging/main.tf");
    }

    #[test]
    fn a_module_default_is_shared_and_the_note_says_so() {
        let files = vec![
            file("modules/aurora/main.tf", MODULE),
            file(
                "envs/prod/main.tf",
                "module \"ledger\" {\n  source = \"../../modules/aurora\"\n  name   = \"ledger-prod\"\n}\nmodule \"audit\" {\n  source = \"../../modules/aurora\"\n  name   = \"audit-prod\"\n}\n",
            ),
        ];
        let l = locate(&cluster("ledger-prod-aurora"), &files).unwrap();
        let e = l.edit.unwrap();
        assert_eq!(
            (e.file.as_str(), e.via.as_str()),
            ("modules/aurora/main.tf", "variable_default")
        );
        assert!(
            e.shared.contains("every use of this module"),
            "{}",
            e.shared
        );
    }

    #[test]
    fn a_registry_module_is_matched_on_the_calls_inputs() {
        let f = file(
            "main.tf",
            r#"
module "db" {
  source  = "terraform-aws-modules/rds/aws"
  version = "~> 6.0"

  identifier                     = "orders"
  instance_use_identifier_prefix = true
  engine                         = "postgres"
  engine_version                 = "16.4"
  family                         = "postgres16"
}
"#,
        );
        let l = locate(
            &db("orders-20240311094512345600000001", "postgres", "16.4"),
            &[f],
        )
        .unwrap();
        assert_eq!(l.address, "module.db");
        assert_eq!(l.how, "terraform_module");
        assert_eq!(
            l.confidence,
            Confidence::Medium,
            "a module's inside is not read"
        );
        let e = l.edit.unwrap();
        assert_eq!(
            (e.line, e.attribute.as_str(), e.via.as_str()),
            (9, "engine_version", "module_input")
        );
        assert!(
            l.evidence.iter().any(|e| e.contains("inside the module")),
            "{:?}",
            l.evidence
        );
    }

    const ROOT: &str = r#"
variable "env" {}
variable "db_engine_version" {}
resource "aws_db_instance" "app" {
  identifier     = "app-${var.env}"
  engine         = "postgres"
  engine_version = var.db_engine_version
}
"#;

    #[test]
    fn a_variable_file_per_environment_is_the_edit_target() {
        let files = vec![
            file("stack/main.tf", ROOT),
            file(
                "stack/envs/prod.tfvars",
                "env = \"prod\"\ndb_engine_version = \"16.4\"\n",
            ),
            file(
                "stack/envs/staging.tfvars",
                "env = \"staging\"\ndb_engine_version = \"16.6\"\n",
            ),
        ];
        let l = locate(&db("app-prod", "postgres", "16.4"), &files).unwrap();
        assert_eq!(l.confidence, Confidence::High);
        assert_eq!(l.environment, "production");
        let e = l.edit.unwrap();
        assert_eq!(
            (
                e.file.as_str(),
                e.line,
                e.attribute.as_str(),
                e.value.as_str(),
                e.via.as_str()
            ),
            (
                "stack/envs/prod.tfvars",
                2,
                "db_engine_version",
                "16.4",
                "tfvars"
            )
        );
        // Without the variable files the name only fits a pattern: medium,
        // and no line to edit means no pull request.
        let alone = locate(&db("app-prod", "postgres", "16.4"), &files[..1]).unwrap();
        assert!(!alone.sure());
        assert!(
            alone
                .evidence
                .iter()
                .any(|e| e.contains("could not follow")),
            "{:?}",
            alone.evidence
        );
    }

    #[test]
    fn for_each_and_count_say_the_value_is_shared() {
        let f = file(
            "main.tf",
            r#"
resource "aws_db_instance" "tenant" {
  for_each       = var.tenants
  identifier     = each.key
  engine         = "postgres"
  engine_version = "16.4"
}
"#,
        );
        let l = locate(&db("tenant-a", "postgres", "16.4"), &[f]).unwrap();
        assert_eq!(
            l.confidence,
            Confidence::Low,
            "which instance it is cannot be read"
        );
        assert!(l
            .edit
            .as_ref()
            .unwrap()
            .shared
            .contains("applies to every one"));
        assert!(
            l.evidence
                .iter()
                .any(|e| e.contains("creates several resources")),
            "{:?}",
            l.evidence
        );
    }

    #[test]
    fn paths_join_and_other_kinds_have_their_attribute() {
        assert_eq!(join("envs/prod", "../../modules/aurora"), "modules/aurora");
        assert_eq!(join("", "./db"), "db");
        assert_eq!(spec(Kind::EksNodegroup).unwrap().version, "release_version");
        assert_eq!(spec(Kind::LambdaFunction).unwrap().version, "runtime");
        assert!(spec(Kind::ContainerImage).is_none());
    }
}
