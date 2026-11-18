//! CVE-2026-114938 end to end, on the AWS side: SSM inventory's packages
//! on instances of several CPU generations and a Lambda function on
//! `java21`, matched against the live Amazon Linux feeds and the OpenJDK
//! advisory of 2026-11-17 (saved as they were read on 2026-11-18).

use std::collections::BTreeMap;

use crate::infra_vuln::feeds::{self, Advisory, Distro};
use crate::infra_vuln::inventory::{self, Kind};
use crate::infra_vuln::matching::{findings, Feeds, FixKind, InfraFinding};
use crate::infra_vuln::raw::{RawAccount, RawAsg, RawInstance, RawPackages};
use crate::runtime_vuln::advisories::tests::java4shell;
use crate::runtime_vuln::cpu::Holds;
use crate::security_fixes::Tier;

const RSS23: &str = include_str!("testdata/alas2023-java-rss.xml");
const RSS2: &str = include_str!("testdata/alas2-java-rss.xml");
const PAGE23: &str = include_str!("testdata/alas2023-2026-3479.html");
const PAGE2: &str = include_str!("testdata/alas2-2026-4123.html");

/// The two feeds as the daily read stores them: every RSS row, and the
/// pages read for the java advisories.
fn alas() -> Vec<Advisory> {
    let mut out = feeds::parse_alas_rss(RSS23, Distro::Al2023).unwrap();
    out.extend(feeds::parse_alas_rss(RSS2, Distro::Al2).unwrap());
    for page in [PAGE23, PAGE2] {
        let p = feeds::parse_alas_page(page).unwrap();
        for a in out.iter_mut().filter(|a| a.id == p.id) {
            a.apply_page(&p);
        }
    }
    out
}

fn instance(id: &str, ty: &str, arch: &str, asg: Option<&str>) -> RawInstance {
    let mut tags = vec![("Name".to_string(), id.to_string())];
    if let Some(g) = asg {
        tags.push(("aws:autoscaling:groupName".into(), g.into()));
    }
    RawInstance {
        id: id.into(),
        image_id: "ami-0abc".into(),
        instance_type: ty.into(),
        architecture: arch.into(),
        tags,
        ..Default::default()
    }
}

fn pkgs(id: &str, list: &[(&str, &str)]) -> RawPackages {
    RawPackages {
        instance: id.into(),
        packages: list
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect(),
    }
}

/// (e) instances whose SSM inventory has an affected java package, on
/// Sapphire Rapids (VBMI), Graviton (arm64) and Skylake (no VBMI), one
/// already on the fixed build, and (d) a Lambda function on java21.
pub fn replay() -> Vec<InfraFinding> {
    replay_with(&alas(), &[java4shell()])
}

/// The same account against any feeds (the live ones in [`live`]).
pub fn replay_with(
    adv: &[Advisory],
    runtime: &[crate::runtime_vuln::advisories::RuntimeAdvisory],
) -> Vec<InfraFinding> {
    let raw = RawAccount {
        account: "111122223333".into(),
        region: "us-east-1".into(),
        read_at: "2026-11-18T12:00:00Z".into(),
        instances: vec![
            instance("i-0a00000000000001", "m7i.large", "x86_64", Some("api")),
            instance("i-0a00000000000002", "m7i.large", "x86_64", Some("api")),
            instance("i-0b00000000000001", "m6g.large", "arm64", None),
            instance("i-0c00000000000001", "m5.xlarge", "x86_64", None),
            instance("i-0d00000000000001", "c7i.large", "x86_64", None),
        ],
        asgs: vec![RawAsg {
            name: "api".into(),
            arn: "arn:aws:autoscaling:us-east-1:111122223333:autoScalingGroup:x:autoScalingGroupName/api".into(),
            desired: 2,
            ..Default::default()
        }],
        packages: vec![
            pkgs(
                "i-0a00000000000001",
                &[
                    ("java-21-amazon-corretto-headless", "21.0.13.0.11-1.amzn2023.1"),
                    ("bash", "5.2.15-1.amzn2023.0.2"),
                ],
            ),
            pkgs(
                "i-0a00000000000002",
                &[("java-21-amazon-corretto-headless", "21.0.12.7.1-1.amzn2023.1")],
            ),
            pkgs(
                "i-0b00000000000001",
                &[("java-17-amazon-corretto-headless", "1:17.0.15.6.1-1.amzn2.1")],
            ),
            pkgs(
                "i-0c00000000000001",
                &[("java-21-amazon-corretto-headless", "21.0.13.0.11-1.amzn2023.1")],
            ),
            pkgs(
                "i-0d00000000000001",
                &[("java-21-amazon-corretto-headless", "21.0.13.1.2-1.amzn2023.1")],
            ),
        ],
        ..Default::default()
    };
    let mut resources = raw.resources();
    resources.extend(inventory::lambdas_from_terraform(
        "acme/infra",
        "lambda.tf",
        "resource \"aws_lambda_function\" \"auth\" {\n  function_name = \"auth\"\n  runtime       = \"java21\"\n}\nresource \"aws_lambda_function\" \"edge\" {\n  function_name = \"edge\"\n  runtime       = \"java17\"\n  architectures = [\"arm64\"]\n}\nresource \"aws_lambda_function\" \"old\" {\n  function_name = \"old\"\n  runtime       = \"java11\"\n}\n",
        "111122223333",
        "us-east-1",
        "2026-11-18T12:00:00Z",
    ));
    resources.extend(inventory::lambdas_from_templates(
        "acme/infra",
        "template.yaml",
        "Resources:\n  OrdersFn:\n    Type: AWS::Serverless::Function\n    Properties:\n      Runtime: java21\n",
        "111122223333",
        "us-east-1",
        "2026-11-18T12:00:00Z",
    ));
    let kev = BTreeMap::new();
    let latest = BTreeMap::new();
    findings(
        &resources,
        &Feeds {
            advisories: adv,
            bulletins: &[],
            runtimes: &[],
            latest: &[],
            kev: &kev,
            latest_al2023: &latest,
            runtime,
            today: "2026-11-18",
        },
    )
}

fn by_name<'a>(f: &'a [InfraFinding], name: &str) -> Vec<&'a InfraFinding> {
    f.iter().filter(|x| x.name == name).collect()
}

#[test]
fn an_affected_java_package_on_a_vbmi_instance_is_the_top_tier() {
    let f = replay();
    let api = by_name(&f, "api");
    assert_eq!(api.len(), 1, "one finding for the group's package");
    let a = api[0];
    assert_eq!(a.kind, Some(Kind::AutoScalingGroup));
    assert_eq!(a.cves, vec!["CVE-2026-114938"]);
    assert!(a.sources.iter().any(|s| s.id == "ALAS2023-2026-3479"));
    assert_eq!(a.fix.kind, Some(FixKind::Runbook));
    assert!(
        a.fix.from.contains("21.0.12.7.1"),
        "the oldest copy in the group: {}",
        a.fix.from
    );
    assert_eq!(a.fix.to, "21.0.13.1.2-1.amzn2023.1");
    assert!(
        a.fix.cli[0].contains("--advisory ALAS2023-2026-3479"),
        "{:?}",
        a.fix.cli
    );
    assert!(
        a.fix.reason.contains("-XX:-UseBASE64Intrinsics"),
        "{}",
        a.fix.reason
    );
    let h = a.hardware.as_ref().expect("hardware facts");
    assert_eq!(h.holds, Holds::Yes);
    assert!(h.why.contains("Sapphire Rapids"), "{}", h.why);
    assert_eq!(a.tier, Some(Tier::P1), "VBMI, not internet-facing");
}

#[test]
fn arm64_and_pre_vbmi_instances_are_findings_at_the_lowest_tier_with_the_reason() {
    let f = replay();
    let arm = by_name(&f, "i-0b00000000000001");
    assert_eq!(arm.len(), 1, "never dropped");
    let h = arm[0].hardware.as_ref().unwrap();
    assert_eq!(h.holds, Holds::No);
    assert!(h.why.contains("arm64"), "{}", h.why);
    assert!(h.if_moved.contains("m7i"), "{}", h.if_moved);
    assert_eq!(arm[0].tier, Some(Tier::P3));
    assert!(
        arm[0].sources.iter().any(|s| s.id == "ALAS2-2026-4123"),
        "AL2 too"
    );
    let sky = by_name(&f, "i-0c00000000000001");
    assert_eq!(sky.len(), 1);
    let h = sky[0].hardware.as_ref().unwrap();
    assert_eq!(h.holds, Holds::No);
    assert!(h.why.contains("no AVX-512 VBMI"), "{}", h.why);
    assert_eq!(sky[0].tier, Some(Tier::P3));
}

#[test]
fn a_fixed_build_and_unrelated_packages_are_not_findings() {
    let f = replay();
    assert!(
        by_name(&f, "i-0d00000000000001").is_empty(),
        "21.0.13.1.2 is fixed"
    );
    assert!(!f.iter().any(|x| x.title.contains("bash")));
}

#[test]
fn lambda_java_runtimes_are_aws_managed_rows_and_java_11_is_not_affected() {
    let f = replay();
    let auth = by_name(&f, "auth");
    assert_eq!(auth.len(), 1);
    assert_eq!(auth[0].fix.kind, Some(FixKind::Aws));
    assert!(auth[0].cves.contains(&"CVE-2026-114938".to_string()));
    assert!(
        auth[0].fix.reason.contains("java21"),
        "{}",
        auth[0].fix.reason
    );
    assert!(
        auth[0].fix.reason.contains("corretto 21.0.13.1.2"),
        "{}",
        auth[0].fix.reason
    );
    assert!(!auth[0].is_open());
    let edge = by_name(&f, "edge");
    assert!(
        edge[0].fix.reason.contains("not affected"),
        "{}",
        edge[0].fix.reason
    );
    assert!(by_name(&f, "old").is_empty(), "java11 is not affected");
    let sam = by_name(&f, "OrdersFn");
    assert_eq!(sam.len(), 1, "SAM templates too");
    assert_eq!(sam[0].fix.kind, Some(FixKind::Aws));
}

#[test]
fn package_versions_compare_like_rpm() {
    use crate::infra_vuln::matching::rpm_lt;
    assert!(rpm_lt(
        "21.0.13.0.11-1.amzn2023.1",
        "21.0.13.1.2-1.amzn2023.1"
    ));
    assert!(rpm_lt("1:17.0.15.6.1-1.amzn2.1", "17.0.21.1.2-1.amzn2.1"));
    assert!(!rpm_lt(
        "21.0.13.1.2-1.amzn2023.1",
        "21.0.13.1.2-1.amzn2023.1"
    ));
    assert!(rpm_lt("2.36-9", "2.36-10"));
}

/// The live sources, read now: the OpenJDK index and its newest page, the
/// ALAS feeds and the page of every java advisory naming the CVE; then
/// every fixture of this module and of `findings::tests` against them.
/// Run by hand: `cargo test -p common runtime_vuln::replay::live -- --ignored --nocapture`.
#[tokio::test]
#[ignore]
async fn live() {
    use crate::runtime_vuln::advisories as ra;
    use crate::runtime_vuln::decls::{from_dockerfile, from_file};
    let http = reqwest::Client::new();
    let get = |u: String| {
        let http = http.clone();
        async move {
            http.get(&u)
                .header("user-agent", "CoderHelm security feeds")
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        }
    };
    let dates = ra::parse_openjdk_index(&get(ra::OPENJDK_INDEX.to_string()).await).unwrap();
    println!("OpenJDK index: newest advisory {}", dates[0]);
    let url = ra::openjdk_page(&dates[0]);
    let mut runtime = ra::parse_openjdk_page(&get(url.clone()).await, &url).unwrap();
    let mut alas: Vec<Advisory> = Vec::new();
    for (rss, distro) in [
        (feeds::ALAS2023_RSS, Distro::Al2023),
        (feeds::ALAS2_RSS, Distro::Al2),
    ] {
        let mut rows = feeds::parse_alas_rss(&get(rss.to_string()).await, distro).unwrap();
        for a in rows.iter_mut().filter(|a| {
            a.package.starts_with("java-") && a.cves.iter().any(|c| c == "CVE-2026-114938")
        }) {
            let p = feeds::parse_alas_page(&get(a.link.clone()).await).unwrap();
            a.apply_page(&p);
            println!(
                "ALAS {} {}: fixed {:?}",
                a.id,
                a.package,
                a.fixed
                    .iter()
                    .map(|f| format!("{}-{}", f.version, f.release))
                    .collect::<Vec<_>>()
                    .first()
            );
        }
        alas.extend(rows);
    }
    ra::enrich(&mut runtime, &alas, &ra::notes());
    let j = runtime
        .iter()
        .find(|a| a.id == "CVE-2026-114938")
        .expect("the live page lists it");
    println!(
        "\n{} {} cvss {:?} published {}",
        j.id, j.component, j.cvss, j.published
    );
    for l in &j.lines {
        println!(
            "  line {}: from {:?} through {} fixed {:?} vendor {:?}",
            l.line, l.introduced, l.last_affected, l.fixed, l.vendor_fixed
        );
    }
    println!(
        "  hardware {:?}\n  mitigations {:?}\n  sources {:?}",
        j.hardware, j.mitigations, j.sources
    );
    // The repository fixtures.
    let repos: &[(&str, &str, &str)] = &[
        (
            "a pinned image",
            "Dockerfile",
            "FROM eclipse-temurin:21.0.12_7-jre\n",
        ),
        (
            "a floating image",
            "Dockerfile",
            "FROM eclipse-temurin:21-jre\n",
        ),
        ("version file", ".java-version", "17.0.15\n"),
        (
            "gradle toolchain",
            "build.gradle.kts",
            "languageVersion = JavaLanguageVersion.of(21)\n",
        ),
        (
            "maven release",
            "pom.xml",
            "<maven.compiler.release>21</maven.compiler.release>\n",
        ),
        (
            "setup-java",
            ".github/workflows/ci.yml",
            "          java-version: '21'\n",
        ),
        (
            "arm64 image",
            "Dockerfile",
            "FROM --platform=linux/arm64 eclipse-temurin:21-jre\n",
        ),
        (
            "java 11",
            "Dockerfile",
            "FROM eclipse-temurin:11.0.25_9-jre\n",
        ),
        ("java 8", "pom.xml", "<java.version>1.8</java.version>\n"),
        ("fixed build", ".java-version", "21.0.13.1\n"),
    ];
    println!("\nRepositories:");
    for (what, path, text) in repos {
        let decls = if path.ends_with("Dockerfile") {
            from_dockerfile(path, text)
        } else {
            from_file(path, text)
        };
        let files = std::collections::BTreeSet::from([path.to_string()]);
        let (obs, _) =
            super::findings::observations("acme/svc", &decls, &files, &runtime, &BTreeMap::new());
        let hit: Vec<&crate::security_fixes::Observation> = obs
            .iter()
            .filter(|o| o.advisory.ids[0] == "CVE-2026-114938")
            .collect();
        match hit.first() {
            None => println!("- {what}: no finding"),
            Some(o) => println!(
                "- {what}: {} ({} {}), fix {:?}\n    {}",
                o.advisory.ids[0],
                o.package,
                o.version,
                o.advisory.fixed.first(),
                o.path.join("\n    ")
            ),
        }
    }
    println!("\nAWS:");
    for f in replay_with(&alas, &runtime) {
        println!(
            "- {} {} [{:?} {:?}] {} | hardware {:?}",
            f.kind.map(|k| k.label()).unwrap_or(""),
            f.name,
            f.tier,
            f.fix.kind,
            f.title,
            f.hardware.as_ref().map(|h| (h.holds, h.why.clone()))
        );
    }
}
