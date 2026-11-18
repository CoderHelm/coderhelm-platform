# Runtime vulnerabilities

A flaw in a language runtime (a JDK, Node.js, Python, Go) is neither a library nor an operating system package. A JDK is not a Maven artifact, and a JDK unpacked into an image is not a Debian or Alpine package. CoderHelm finds the runtimes a team runs from its code and its AWS accounts, and matches them by version line against the runtime projects' own advisories. No model is called: every match is a comparison.

The code is in `services/common/src/runtime_vuln/` (the rules) and in the security and infrastructure passes of the worker (the reads).

## What runs, from the code

Read with each repository's dependency list (`refresh_inventory`), and listed in it under the `runtime` ecosystem:

| Where | Example | Precision |
|---|---|---|
| Dockerfile `FROM` | `eclipse-temurin:21.0.4_7-jre`, `amazoncorretto:21`, `gradle:8.10-jdk21`, `gcr.io/distroless/java21-debian12`, `node:20.11.0`, `python:3.12`, `golang:1.22.3` | the tag's |
| Version files | `.java-version`, `.sdkmanrc` (`java=21.0.4-tem`), `.tool-versions`, `.nvmrc`, `.node-version`, `.python-version`, `go.mod` `toolchain` | the value's |
| Build toolchains | Gradle `JavaLanguageVersion.of(21)`, `jvmToolchain(21)` | a line |
| Build targets | Maven `maven.compiler.release`, `java.version`, `release`; Gradle `sourceCompatibility` | a line |
| CI setup steps | `java-version`, `node-version`, `python-version`, `go-version` in `.github/workflows` | the value's |
| Lambda runtimes | Terraform `runtime = "java21"`, SAM and CloudFormation `Runtime: java21`, Serverless `runtime: java21`, CDK `Runtime.JAVA_21` | a line AWS manages |

A value with a patch number (`21.0.4`, `21.0.4_7`) is an exact build. A line (`21`, `21-jre`, a toolchain) takes whatever build is newest when the image is built or the job runs. A tag a registry republishes for every build of an update (`amazoncorretto:21.0.13` while the advisory's last affected build is `21.0.13.0.3`) is treated as a line. Values from variables are not read. `FROM --platform=linux/arm64` is kept as the image's architecture.

## The advisories

Read once a day with the infrastructure feeds (`infra_feeds`), each kept as `RUNTIME_ADV#<CVE>` under `SECURITY#GLOBAL`:

1. The OpenJDK Vulnerability Group's index and each advisory page (the newest three again every day, older ones once). A page gives the lines each CVE affects (its risk matrix) and the last affected build of each line ("The affected versions are 27.0.1.0.3, 25.0.5.0.3, 21.0.13.0.3, 17.0.21.0.3, 11.0.33, 8u512, and earlier").
2. The Amazon Linux advisories for the `java-*-amazon-corretto` packages (ALAS, already read for the infrastructure findings), for what the OpenJDK page does not say: where a line starts being affected ("17.0.6 and later"), the hardware ("on x86-64 systems", "ARM64 systems are not affected"), JVM flags, and Corretto's fixed builds (and from them the upstream fix: Corretto `21.0.13.1.2` is upstream `21.0.13.1`).
3. `runtime_vuln/notes.json`: what no feed carries in a form that can be read (a CPU feature, a JVM flag), per CVE, dated and with its source. A feed's own value wins.

A page that can't be fetched or no longer parses keeps the last good copy, and the feed's status row (`INFRA_FEED#openjdk`, on the Sources panel) carries the error.

OSV was checked first: it has no JDK ecosystem. It lists a JDK CVE only under distributions' `openjdk-*` source packages (Debian, Ubuntu, Alpine), which the base image findings already cover for images whose registry publishes a package list.

## Findings in repositories

One finding per declaration and CVE, recorded like a dependency finding (package = the image's name or the runtime's, version = the tag or value as written, manifest = the file), so tiers, cards, fix runs and pull requests work the same way.

- An exact build that is affected gets a pull request when a fixed build is known in the same form: for an image, the lowest published tag of the same variant past the last affected build (from the registry's tag list: `21.0.4_7-jre` moves to `21.0.13.1_2-jre`, never to a `-jdk` or another vendor); for a version file or CI pin, the upstream fix, or the vendor's build when the source names the vendor's.
- A line, a floating tag or a republished tag is a finding without a pull request: rebuild and redeploy (or pin a fixed build).
- An exact build with no fixed build published yet for its vendor is a finding without a pull request, until one is.
- Every finding names the advisory's hardware condition and its JVM flag for the time until the fix runs.
- An image built only for an architecture the advisory says is not affected stays a finding, at the lowest tier, with the reason.
- A finding closes when a fresh read of the repository no longer declares an affected build there (a merged pull request alone does not close it).

## Findings on AWS

- SSM inventory: every installed package of up to 100 SSM-managed instances per account and region (the first of each Auto Scaling group and node group first), not only the kernel. Each is matched against every Amazon Linux advisory for a package other than the kernel (the kernel decides the AMI finding), by the package's own release (`amzn2023` or `amzn2`). One finding per affected source package and group, a runbook (`dnf upgrade --advisory ...`, then restart the JVMs), with the JVM flag when the advisory names one.
- Lambda functions on a managed Java runtime whose line is affected are AWS-managed rows: AWS ships the fixed build in the managed runtime; the row says how each runtime update mode takes it.
- The hardware: EC2 instance types (and node group instance types) map to their architecture and CPU generation (`runtime_vuln::cpu`). When every CVE behind a finding names a hardware condition:
  - it holds (for AVX-512 VBMI: Intel Ice Lake, Sapphire Rapids, Granite Rapids; AMD Zen 4, Zen 5): P0 when internet-facing, else at least P1;
  - it does not (Graviton, or an x86 family without the feature): P3, with the reason and what moving to an affected family would mean. A later read on an affected family raises the tier, which posts a card for P0 and P1;
  - not known (a family not in the table): the tier stays as matched, and the finding says the CPU is not known.

No new AWS permission is needed: `ec2:DescribeInstances` already returns the instance type and architecture, `eks:DescribeNodegroup` the node group's instance types, and `ssm:ListInventoryEntries` every package.

## Worked example: CVE-2026-114938

A heap overflow in the `java.util.Base64` decoder's AVX-512 VBMI intrinsic (OpenJDK advisory 2026-11-17). The OpenJDK page lists lines 17, 21, 25 and 27 (not 8 or 11), last affected 17.0.21.0.3, 21.0.13.0.3, 25.0.5.0.3, 27.0.1.0.3. ALAS2023-2026-3478 to 3481 and ALAS2-2026-4123 add "17.0.6 and later on x86-64 ... ARM64 systems are not affected" and Corretto 17.0.21.1.2 and 21.0.13.1.2; the notes add AVX-512 VBMI and `-XX:-UseBASE64Intrinsics`.

| What the team runs | What CoderHelm says |
|---|---|
| `FROM eclipse-temurin:21.0.4_7-jre` | P1, pull request to the lowest fixed `_N-jre` tag once the registry has one |
| `FROM eclipse-temurin:21-jre` | P1, rebuild and redeploy to take 21.0.13.1 |
| `.java-version` `17.0.15` | P1, pull request to `17.0.21.1` |
| Gradle toolchain 21, `maven.compiler.release` 21, `setup-java` `21` | P1 each, rebuild (the line takes the fix) |
| `FROM --platform=linux/arm64 eclipse-temurin:21-jre` | P3, ARM64 is not affected |
| Lambda `runtime = "java21"` | AWS-managed row |
| `java-21-amazon-corretto-headless` 21.0.13.0.11 on `m7i` | P1 (P0 when internet-facing), `dnf upgrade --advisory ALAS2023-2026-3479` |
| the same on `m6g` or `m5` | P3, with why and what moving to `m7i` would mean |
| Java 8, Java 11, 17.0.5, 21.0.13.1 | nothing |

The replay tests are `runtime_vuln::findings::tests` and `runtime_vuln::replay`, on the pages as they were read on 2026-11-18 (`runtime_vuln/testdata`).

## Not covered yet

- Node.js, Python and Go advisories: their declarations are read and listed, but only the OpenJDK feed is matched. Go's (`stdlib` in OSV's Go ecosystem) and Node.js's (the security working group's `vuln/core` index) are the next sources.
- A JDK inside a container is the repository finding of the Dockerfile that builds it; the hardware it runs on is not tied to that finding yet (ECS services and EKS pods are not mapped back to their image's Dockerfile). What CPU runs a Fargate task or an x86 Lambda function AWS does not say, so those stay "not known".
- A vendor whose fixed build no source names (an `.sdkmanrc` on Temurin, say) gets a finding without a pull request until the source names it.
