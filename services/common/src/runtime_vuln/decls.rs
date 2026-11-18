//! Where a repository's code says which runtime runs, and how precisely.
//!
//! | Where | Example | Precision |
//! |---|---|---|
//! | Dockerfile `FROM` | `eclipse-temurin:21.0.4_7-jre`, `amazoncorretto:21` | the tag's |
//! | Version file | `.java-version`, `.sdkmanrc`, `.tool-versions`, `.nvmrc`, `.python-version`, `go.mod` `toolchain` | the value's |
//! | Build toolchain | Gradle `JavaLanguageVersion.of(21)`, `jvmToolchain(21)` | a line |
//! | Build target | Maven `maven.compiler.release`, `java.version`; Gradle `sourceCompatibility` | a line |
//! | CI setup | `actions/setup-java` `java-version: '21'` (and node, python, go) | the value's |
//! | Lambda runtime | Terraform `runtime = "java21"`, SAM/CloudFormation `Runtime: java21`, Serverless `runtime: java21`, CDK `Runtime.JAVA_21` | a line AWS manages |
//!
//! A value with a patch number and a build (`21.0.4_7`, `21.0.4+7`) or
//! more is exact. A line (`21`, `21-jre`, a toolchain) takes whatever build
//! is newest when the image is built or the job runs: a rebuild takes a
//! fix. Values from variables (`${JAVA_VERSION}`, `${{ matrix.java }}`)
//! are not read.

use regex::Regex;
use serde::{Deserialize, Serialize};

use super::{java_numbers, Runtime};

/// What kind of place declares it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeclKind {
    #[default]
    Image,
    VersionFile,
    Toolchain,
    BuildTarget,
    Ci,
    Lambda,
}

impl DeclKind {
    pub fn label(self) -> &'static str {
        match self {
            DeclKind::Image => "base image",
            DeclKind::VersionFile => "version file",
            DeclKind::Toolchain => "build toolchain",
            DeclKind::BuildTarget => "build target",
            DeclKind::Ci => "CI setup step",
            DeclKind::Lambda => "Lambda runtime",
        }
    }
}

/// One declaration of a runtime in code.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Decl {
    pub runtime: Runtime,
    pub kind: DeclKind,
    pub file: String,
    /// 1-based.
    pub line: u32,
    /// The image's name as written, or the runtime's name.
    pub package: String,
    /// What a fix edits, as written: the tag, or the value.
    pub written: String,
    /// The runtime version it reads as (`21.0.4_7`, `21`).
    pub version: String,
    /// An exact build (else a line, or a tag the registry moves).
    pub exact: bool,
    /// `temurin`, `corretto`, `zulu`, ...; empty when not said.
    pub vendor: String,
    /// `linux/arm64` from `FROM --platform=`, empty when not said.
    pub platform: String,
}

impl Decl {
    /// The dependency row it is kept as (`security_fixes::Dep`), so the
    /// repository's inventory lists it.
    pub fn to_dep(&self) -> crate::security_fixes::Dep {
        crate::security_fixes::Dep {
            ecosystem: super::ECOSYSTEM.into(),
            name: self.package.clone(),
            version: self.written.clone(),
            manifest: self.file.clone(),
            scope: crate::security_fixes::Scope::Runtime,
            path: vec![format!(
                "{DEP_TAG}{}",
                serde_json::to_string(self).unwrap_or_default()
            )],
        }
    }

    pub fn from_dep(d: &crate::security_fixes::Dep) -> Option<Decl> {
        if d.ecosystem != super::ECOSYSTEM {
            return None;
        }
        d.path
            .first()
            .and_then(|p| p.strip_prefix(DEP_TAG))
            .and_then(|j| serde_json::from_str(j).ok())
    }

    /// Where it is, in words (`Dockerfile line 3, base image eclipse-temurin:21-jre`).
    pub fn describe(&self) -> String {
        let what = match self.kind {
            DeclKind::Image => format!("{}:{}", self.package, self.written),
            _ => self.written.clone(),
        };
        format!(
            "`{}` line {}, {} `{what}`",
            self.file,
            self.line,
            self.kind.label()
        )
    }
}

const DEP_TAG: &str = "runtime-decl:";

fn re(p: &str) -> Regex {
    Regex::new(p).expect("a valid pattern")
}

fn line_at(text: &str, byte: usize) -> u32 {
    text[..byte.min(text.len())].matches('\n').count() as u32 + 1
}

/// Exact when it has a patch number and a build, or more numbers
/// (`21.0.4_7`, `21.0.4+7`, `21.0.13.1`); `21.0.4` is exact too (a tag
/// that names a patch update), except where the findings say a registry
/// moves it.
fn java_exact(v: &str) -> bool {
    java_numbers(v).len() >= 3
}

fn plain_exact(v: &str) -> bool {
    v.trim_start_matches(|c: char| !c.is_ascii_digit())
        .split(|c: char| !(c.is_ascii_digit() || c == '.'))
        .next()
        .unwrap_or("")
        .split('.')
        .filter(|p| !p.is_empty())
        .count()
        >= 3
}

/// The runtime an image runs, its version as the tag or name says, and the
/// vendor: `(runtime, version, vendor)`.
pub fn image_runtime(name: &str, tag: &str) -> Option<(Runtime, String, String)> {
    let n = name.to_ascii_lowercase();
    let short = n.rsplit('/').next().unwrap_or(&n).to_string();
    let t = tag.to_ascii_lowercase();
    let lead = |s: &str| -> String {
        re(r"^(?:open-|certified-|jdk-?|jre-?)?(\d+(?:\.\d+)*(?:[_+]\d+)?)")
            .captures(s)
            .map(|c| c[1].to_string())
            .unwrap_or_default()
    };
    let java_vendor = match short.as_str() {
        "eclipse-temurin" => Some("temurin"),
        "openjdk" => Some("openjdk"),
        "amazoncorretto" => Some("corretto"),
        "ibm-semeru-runtimes" => Some("semeru"),
        "sapmachine" => Some("sapmachine"),
        s if s.starts_with("zulu-openjdk") => Some("zulu"),
        s if s.starts_with("liberica-") => Some("liberica"),
        "jdk" | "jre" if n.contains("microsoft") => Some("microsoft"),
        _ => None,
    };
    if let Some(vendor) = java_vendor {
        let v = lead(&t);
        return (!v.is_empty()).then(|| (Runtime::Java, v, vendor.to_string()));
    }
    // A line in the name: `ubi9/openjdk-21`, `distroless/java21-debian12`.
    if let Some(c) = re(r"(?:^|/)(?:openjdk-|java)(\d{1,2})(?:-|$)").captures(&n) {
        return Some((Runtime::Java, c[1].to_string(), String::new()));
    }
    // Build and server images that carry a JDK line in the tag:
    // `gradle:8.10-jdk21`, `maven:3.9-eclipse-temurin-21`, `tomcat:10.1-jdk21-temurin`.
    if matches!(
        short.as_str(),
        "gradle" | "maven" | "tomcat" | "jetty" | "sbt" | "clojure" | "groovy"
    ) {
        if let Some(c) = re(
            r"(?:jdk|jre|temurin-|corretto-|openjdk-|zulu-|sapmachine-|graalvm-community-)(\d{1,2})(?:\b|$|-)",
        )
        .captures(&t)
        {
            return Some((Runtime::Java, c[1].to_string(), String::new()));
        }
        return None;
    }
    let other = match short.as_str() {
        "node" => Some(Runtime::Node),
        "python" => Some(Runtime::Python),
        "golang" => Some(Runtime::Go),
        _ => None,
    }?;
    let v = lead(&t);
    (!v.is_empty()).then(|| (other, v, String::new()))
}

/// The runtimes a Dockerfile's `FROM` lines name.
pub fn from_dockerfile(path: &str, text: &str) -> Vec<Decl> {
    let from = re(r"(?im)^\s*FROM\s+((?:--\S+\s+)*)(\S+)");
    let mut out = Vec::new();
    for c in from.captures_iter(text) {
        let image = &c[2];
        if image.contains('$') {
            continue;
        }
        let platform = re(r"--platform=(\S+)")
            .captures(&c[1])
            .map(|p| p[1].to_string())
            .unwrap_or_default();
        let (name, tag) = match image.split_once('@') {
            Some((n, _)) => match n.rsplit_once(':') {
                Some((nn, t)) if !t.contains('/') => (nn.to_string(), t.to_string()),
                _ => (n.to_string(), "latest".to_string()),
            },
            None => match image.rsplit_once(':') {
                Some((n, t)) if !t.contains('/') => (n.to_string(), t.to_string()),
                _ => (image.to_string(), "latest".to_string()),
            },
        };
        let Some((runtime, version, vendor)) = image_runtime(&name, &tag) else {
            continue;
        };
        let line = line_at(text, c.get(2).map(|m| m.start()).unwrap_or(0));
        let exact = match runtime {
            Runtime::Java => java_exact(&version) && !tag_names_line_only(&tag, &version),
            _ => plain_exact(&version),
        };
        out.push(Decl {
            runtime,
            kind: DeclKind::Image,
            file: path.into(),
            line,
            package: name,
            written: tag.clone(),
            version,
            exact,
            vendor,
            platform,
        });
    }
    out
}

/// A tag whose version came from a JDK line in a build image's tag
/// (`gradle:8.10-jdk21`): never exact.
fn tag_names_line_only(tag: &str, version: &str) -> bool {
    !tag.to_ascii_lowercase()
        .starts_with(|c: char| c.is_ascii_digit())
        && !tag.to_ascii_lowercase().starts_with("open-")
        && !tag.to_ascii_lowercase().starts_with("certified-")
        && version.len() <= 2
}

fn runtime_of_tool(tool: &str) -> Option<Runtime> {
    match tool.to_ascii_lowercase().as_str() {
        "java" => Some(Runtime::Java),
        "nodejs" | "node" => Some(Runtime::Node),
        "python" => Some(Runtime::Python),
        "golang" | "go" => Some(Runtime::Go),
        _ => None,
    }
}

fn vendor_of(v: &str) -> String {
    let l = v.to_ascii_lowercase();
    for (w, name) in [
        ("temurin", "temurin"),
        ("-tem", "temurin"),
        ("corretto", "corretto"),
        ("-amzn", "corretto"),
        ("zulu", "zulu"),
        ("-zulu", "zulu"),
        ("liberica", "liberica"),
        ("-librca", "liberica"),
        ("semeru", "semeru"),
        ("-sem", "semeru"),
        ("sapmachine", "sapmachine"),
        ("-sapmchn", "sapmachine"),
        ("graalvm", "graalvm"),
        ("-graal", "graalvm"),
        ("oracle", "oracle"),
        ("-oracle", "oracle"),
        ("-ms", "microsoft"),
        ("microsoft", "microsoft"),
    ] {
        if l.contains(w) {
            return name.to_string();
        }
    }
    String::new()
}

fn value_decl(runtime: Runtime, kind: DeclKind, path: &str, line: u32, value: &str) -> Decl {
    let exact = match runtime {
        Runtime::Java => java_exact(value),
        _ => plain_exact(value),
    };
    Decl {
        runtime,
        kind,
        file: path.into(),
        line,
        package: runtime.name().into(),
        written: value.to_string(),
        version: value.to_string(),
        exact,
        vendor: if runtime == Runtime::Java {
            vendor_of(value)
        } else {
            String::new()
        },
        platform: String::new(),
    }
}

fn base(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase()
}

/// Whether a file may declare a runtime (Dockerfiles are read for their
/// base images already).
pub fn is_runtime_file(path: &str) -> bool {
    let b = base(path);
    let p = path.to_ascii_lowercase();
    matches!(
        b.as_str(),
        ".java-version"
            | ".sdkmanrc"
            | ".tool-versions"
            | ".nvmrc"
            | ".node-version"
            | ".python-version"
            | "go.mod"
            | "pom.xml"
            | "build.gradle"
            | "build.gradle.kts"
            | "template.yaml"
            | "template.yml"
            | "serverless.yml"
            | "serverless.yaml"
    ) || (p.starts_with(".github/workflows/") && (b.ends_with(".yml") || b.ends_with(".yaml")))
        || b.ends_with(".tf")
        || b.ends_with(".template.json")
        || b.ends_with(".template.yaml")
}

/// The runtimes one file declares (not a Dockerfile's: [`from_dockerfile`]).
pub fn from_file(path: &str, text: &str) -> Vec<Decl> {
    let b = base(path);
    let mut out = Vec::new();
    let first_value = || -> Option<(u32, String)> {
        text.lines()
            .enumerate()
            .map(|(i, l)| (i as u32 + 1, l.trim()))
            .find(|(_, l)| !l.is_empty() && !l.starts_with('#'))
            .map(|(i, l)| (i, l.split_whitespace().next().unwrap_or("").to_string()))
    };
    match b.as_str() {
        ".java-version" | ".nvmrc" | ".node-version" | ".python-version" => {
            let rt = match b.as_str() {
                ".java-version" => Runtime::Java,
                ".python-version" => Runtime::Python,
                _ => Runtime::Node,
            };
            if let Some((line, v)) = first_value() {
                if v.chars().any(|c| c.is_ascii_digit()) {
                    out.push(value_decl(rt, DeclKind::VersionFile, path, line, &v));
                }
            }
        }
        ".sdkmanrc" => {
            for (i, l) in text.lines().enumerate() {
                if let Some(v) = l.trim().strip_prefix("java=") {
                    out.push(value_decl(
                        Runtime::Java,
                        DeclKind::VersionFile,
                        path,
                        i as u32 + 1,
                        v.trim(),
                    ));
                }
            }
        }
        ".tool-versions" => {
            for (i, l) in text.lines().enumerate() {
                let w: Vec<&str> = l.split_whitespace().collect();
                if let (Some(tool), Some(v)) = (w.first(), w.get(1)) {
                    if let Some(rt) = runtime_of_tool(tool) {
                        out.push(value_decl(rt, DeclKind::VersionFile, path, i as u32 + 1, v));
                    }
                }
            }
        }
        "go.mod" => {
            for (i, l) in text.lines().enumerate() {
                if let Some(v) = l.trim().strip_prefix("toolchain go") {
                    out.push(value_decl(
                        Runtime::Go,
                        DeclKind::Toolchain,
                        path,
                        i as u32 + 1,
                        v.trim(),
                    ));
                }
            }
        }
        "pom.xml" => {
            let r = re(
                r"<(maven\.compiler\.release|java\.version|maven\.compiler\.target|release)>\s*(\d{1,2}|1\.8)\s*</",
            );
            let mut seen = Vec::new();
            for c in r.captures_iter(text) {
                let v = c[2].replace("1.8", "8");
                if seen.contains(&v) {
                    continue;
                }
                seen.push(v.clone());
                let line = line_at(text, c.get(2).unwrap().start());
                out.push(value_decl(
                    Runtime::Java,
                    DeclKind::BuildTarget,
                    path,
                    line,
                    &v,
                ));
            }
        }
        "build.gradle" | "build.gradle.kts" => {
            let tool = re(r"(?:JavaLanguageVersion\.of|jvmToolchain)\s*\(\s*(\d{1,2})\s*\)");
            for c in tool.captures_iter(text) {
                let line = line_at(text, c.get(1).unwrap().start());
                out.push(value_decl(
                    Runtime::Java,
                    DeclKind::Toolchain,
                    path,
                    line,
                    &c[1],
                ));
            }
            if out.is_empty() {
                let target = re(
                    r#"(?:source|target)Compatibility\s*=?\s*(?:JavaVersion\.VERSION_(\d{1,2})|['"]?(\d{1,2})['"]?)"#,
                );
                if let Some(c) = target.captures(text) {
                    let v = c.get(1).or(c.get(2)).unwrap();
                    let line = line_at(text, v.start());
                    out.push(value_decl(
                        Runtime::Java,
                        DeclKind::BuildTarget,
                        path,
                        line,
                        v.as_str(),
                    ));
                }
            }
        }
        _ => {}
    }
    let p = path.to_ascii_lowercase();
    if p.starts_with(".github/workflows/") {
        let r = re(
            r#"(?m)^\s*(java|node|python|go)-version:\s*(\[[^\]\n]*\]|['"]?[0-9][0-9A-Za-z.+_-]*['"]?)"#,
        );
        for c in r.captures_iter(text) {
            let rt = runtime_of_tool(&c[1]).unwrap_or(Runtime::Java);
            let raw = c.get(2).unwrap();
            for v in raw
                .as_str()
                .trim_matches(|c| c == '[' || c == ']')
                .split(',')
                .map(|v| v.trim().trim_matches(|c| c == '\'' || c == '"'))
                .filter(|v| v.starts_with(|c: char| c.is_ascii_digit()))
            {
                let line = line_at(text, raw.start());
                out.push(value_decl(rt, DeclKind::Ci, path, line, v));
            }
        }
    }
    out.extend(lambda_runtimes(path, text));
    out
}

/// Lambda runtimes in infrastructure code, as lines AWS manages.
pub fn lambda_runtimes(path: &str, text: &str) -> Vec<Decl> {
    let b = base(path);
    let iac = b.ends_with(".tf")
        || b.ends_with(".yml")
        || b.ends_with(".yaml")
        || b.ends_with(".json")
        || b.ends_with(".ts")
        || b.ends_with(".py");
    if !iac {
        return Vec::new();
    }
    let r = re(
        r#"(?m)(?:\bruntime\s*=\s*"|\bRuntime"?\s*:\s*"?|\bruntime:\s*"?|Runtime\.)(java\d+(?:\.al2)?|JAVA_\d+|nodejs\d+\.x|NODEJS_\d+_X|python3\.\d+|PYTHON_3_\d+)"#,
    );
    let mut out = Vec::new();
    for c in r.captures_iter(text) {
        let m = c.get(1).unwrap();
        let id = lambda_id(m.as_str());
        let (rt, line) = match id.as_str() {
            s if s.starts_with("java") => (
                Runtime::Java,
                s.trim_start_matches("java")
                    .trim_end_matches(".al2")
                    .to_string(),
            ),
            s if s.starts_with("nodejs") => (
                Runtime::Node,
                s.trim_start_matches("nodejs")
                    .trim_end_matches(".x")
                    .to_string(),
            ),
            s => (Runtime::Python, s.trim_start_matches("python").to_string()),
        };
        out.push(Decl {
            runtime: rt,
            kind: DeclKind::Lambda,
            file: path.into(),
            line: line_at(text, m.start()),
            package: id.clone(),
            written: m.as_str().to_string(),
            version: line,
            exact: false,
            vendor: if rt == Runtime::Java {
                "corretto".into()
            } else {
                String::new()
            },
            platform: String::new(),
        });
    }
    out
}

/// A CDK runtime constant as Lambda's id (`JAVA_21` is `java21`).
fn lambda_id(s: &str) -> String {
    let l = s.to_ascii_lowercase();
    if let Some(n) = l.strip_prefix("java_") {
        return format!("java{n}");
    }
    if let Some(n) = l.strip_prefix("nodejs_") {
        return format!("nodejs{}.x", n.trim_end_matches("_x"));
    }
    if let Some(n) = l.strip_prefix("python_3_") {
        return format!("python3.{n}");
    }
    l
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jdk_images_say_their_build_or_line() {
        let df = "FROM eclipse-temurin:21.0.4_7-jre AS run\nFROM --platform=linux/arm64 amazoncorretto:21-al2023-headless\nFROM gradle:8.10-jdk21 AS build\nFROM maven:3.9-eclipse-temurin-17\nFROM gcr.io/distroless/java21-debian12\nFROM node:20.11.0-alpine\nFROM debian:12-slim\nFROM ${BASE}\n";
        let d = from_dockerfile("Dockerfile", df);
        let got: Vec<(&str, &str, bool, u32)> = d
            .iter()
            .map(|x| (x.package.as_str(), x.version.as_str(), x.exact, x.line))
            .collect();
        assert_eq!(
            got,
            vec![
                ("eclipse-temurin", "21.0.4_7", true, 1),
                ("amazoncorretto", "21", false, 2),
                ("gradle", "21", false, 3),
                ("maven", "17", false, 4),
                ("gcr.io/distroless/java21-debian12", "21", false, 5),
                ("node", "20.11.0", true, 6),
            ]
        );
        assert_eq!(d[1].platform, "linux/arm64");
        assert_eq!(d[0].vendor, "temurin");
        assert_eq!(d[0].written, "21.0.4_7-jre");
        assert_eq!(d[0].runtime, Runtime::Java);
        assert_eq!(d[5].runtime, Runtime::Node);
    }

    #[test]
    fn version_files_toolchains_and_ci_steps() {
        let j = from_file(".java-version", "17.0.15\n");
        assert_eq!((j[0].version.as_str(), j[0].exact), ("17.0.15", true));
        let s = from_file(".sdkmanrc", "# sdk\njava=21.0.4-tem\n");
        assert_eq!((s[0].line, s[0].vendor.as_str()), (2, "temurin"));
        let t = from_file(
            ".tool-versions",
            "java temurin-21.0.4+7.0.LTS\nnodejs 20.11.0\n",
        );
        assert_eq!(t.len(), 2);
        assert_eq!(t[1].runtime, Runtime::Node);
        let g = from_file(
            "build.gradle.kts",
            "java {\n    toolchain {\n        languageVersion = JavaLanguageVersion.of(21)\n    }\n}\n",
        );
        assert_eq!(
            (g[0].kind, g[0].version.as_str(), g[0].exact, g[0].line),
            (DeclKind::Toolchain, "21", false, 3)
        );
        let pom = from_file(
            "pom.xml",
            "<properties>\n  <maven.compiler.release>21</maven.compiler.release>\n</properties>",
        );
        assert_eq!((pom[0].kind, pom[0].line), (DeclKind::BuildTarget, 2));
        let ci = from_file(
            ".github/workflows/ci.yml",
            "steps:\n  - uses: actions/setup-java@v4\n    with:\n      distribution: temurin\n      java-version: '21'\n  - uses: actions/setup-node@v4\n    with:\n      node-version: [18, 20]\n",
        );
        let got: Vec<(Runtime, &str, u32)> = ci
            .iter()
            .map(|x| (x.runtime, x.version.as_str(), x.line))
            .collect();
        assert_eq!(
            got,
            vec![
                (Runtime::Java, "21", 5),
                (Runtime::Node, "18", 8),
                (Runtime::Node, "20", 8)
            ]
        );
        assert!(from_file(
            ".github/workflows/ci.yml",
            "java-version: ${{ matrix.java }}\n"
        )
        .is_empty());
    }

    #[test]
    fn lambda_runtimes_in_every_tool() {
        let tf = from_file(
            "infra/lambda.tf",
            "resource \"aws_lambda_function\" \"x\" {\n  runtime = \"java21\"\n}\n",
        );
        assert_eq!(
            (
                tf[0].kind,
                tf[0].package.as_str(),
                tf[0].version.as_str(),
                tf[0].line
            ),
            (DeclKind::Lambda, "java21", "21", 2)
        );
        let sam = from_file("template.yaml", "Resources:\n  Fn:\n    Type: AWS::Serverless::Function\n    Properties:\n      Runtime: java17\n");
        assert_eq!(sam[0].package, "java17");
        let cdk = lambda_runtimes("infra/stack.ts", "runtime: lambda.Runtime.JAVA_21,");
        assert_eq!(cdk[0].package, "java21");
        let node = from_file("serverless.yml", "provider:\n  runtime: nodejs20.x\n");
        assert_eq!(
            (node[0].runtime, node[0].version.as_str()),
            (Runtime::Node, "20")
        );
    }

    #[test]
    fn a_declaration_round_trips_through_the_dependency_list() {
        let d = &from_dockerfile("svc/Dockerfile", "FROM eclipse-temurin:21-jre\n")[0];
        let dep = d.to_dep();
        assert_eq!(dep.ecosystem, "runtime");
        assert_eq!(
            (dep.name.as_str(), dep.version.as_str()),
            ("eclipse-temurin", "21-jre")
        );
        assert_eq!(Decl::from_dep(&dep).as_ref(), Some(d));
        assert!(is_runtime_file(".github/workflows/build.yaml"));
        assert!(is_runtime_file("api/.java-version"));
        assert!(!is_runtime_file("src/main/App.java"));
    }
}
