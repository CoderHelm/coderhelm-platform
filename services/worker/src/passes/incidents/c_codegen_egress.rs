//! c. The studio's typegen ran in the sandbox with the team's network
//! allowlist on. It needed the CMS API, which wasn't on the list; the proxy
//! printed `CODERHELM_EGRESS_BLOCKED <host>`, the generator failed, and the
//! run ended "its generated files were not regenerated", blaming the change.
//!
//! Guarantee: a build that runs a generator may reach the hosts that
//! generator needs. If a host is still refused, the run stops as a tooling
//! failure that names the host, never as the change's failure.

use super::fixture::*;
use crate::clients::sandbox::{self, EgressBlocks};
use crate::passes::generators::{self, GateStop, Pass, Read};

const OLD: &str = "its generated files were not regenerated";

#[test]
fn a_generator_build_may_reach_the_hosts_its_generator_needs() {
    let repo = monorepo();
    let hosts = generators::required_hosts(&repo, &codegen_env());
    assert!(
        common::agent_policy::sandbox::domain_allowed(&hosts, CMS_HOST),
        "{hosts:?}"
    );
    let cmds = generators::generator_commands(&repo);
    // The team limits the network to the package registries.
    let team = "registry.npmjs.org,github.com";
    let codegen = sandbox::build_allowlist(Some(team), &hosts, true).unwrap();
    let allowed: Vec<String> = codegen.split(',').map(str::to_string).collect();
    assert!(common::agent_policy::sandbox::domain_allowed(
        &allowed, CMS_HOST
    ));
    // A check build that runs the typegen gets them too; one that doesn't,
    // doesn't: a limited network stays limited.
    assert!(sandbox::runs_generator(
        &format!("cd apps/studio && {} && pnpm run lint", cmds[0]),
        &cmds
    ));
    let lint = sandbox::build_allowlist(
        Some(team),
        &hosts,
        sandbox::runs_generator("pnpm run lint", &cmds),
    );
    assert_eq!(lint.as_deref(), Some(team));
}

/// The generator gate's ending for one codegen build: the sandbox's
/// archive and log read by the real `codegen_outcome`, the real
/// `read_outcome`, and the pipeline's stop.
async fn gate_ending(run_id: &str, sb: &FakeSandbox) -> Ending {
    let mut end = Ending::default();
    let cmd = "cd apps/studio && pnpm run typegen";
    let o = sandbox::codegen_outcome(sb, "/aws/codebuild/sandbox", "s1", "out.tgz", false).await;
    let pass = match generators::read_outcome(o, cmd, || true) {
        Read::Refused(f) => {
            EgressBlocks::record(run_id, &f);
            Pass::Blocked(f)
        }
        other => panic!("not read as refused: {other:?}"),
    };
    match generators::stop_of(pass) {
        Err(GateStop::Blocked(why)) => {
            end.said(super::super::stopped_keeping(&why, BRANCH));
            end.status = common::run_status::FAILED.into();
        }
        Err(GateStop::NeedsTime) => end.status = "continues".into(),
        Ok(_) => end.status = "checks".into(),
    }
    end
}

#[tokio::test]
async fn a_refused_host_is_a_tooling_failure_naming_it() {
    let run_id = "01JRUNEGRESS00000000000000";
    let log = codebuild_log(&[
        "Error: getaddrinfo ENOTFOUND proj9.api.sanity.io",
        &format!("CODERHELM_EGRESS_BLOCKED {CMS_HOST}"),
        "CODERHELM_CODEGEN_END exit=1",
    ]);
    let end = gate_ending(run_id, &FakeSandbox::new(vec![], log)).await;
    assert_eq!(end.status, common::run_status::FAILED);
    assert!(end.says(CMS_HOST), "{:?}", end.words);
    assert!(end.says("The work so far is kept on branch"));
    end.never_says(OLD);
    end.never_says("the change failed");
    // A later step of the run that can't build still reports the host.
    let kept = EgressBlocks::last(run_id).expect("the refusal is kept for the run");
    assert!(kept.reason().contains(CMS_HOST));
    EgressBlocks::clear(run_id);
}

#[tokio::test]
async fn the_echoed_grep_of_the_egress_log_is_not_a_refusal() {
    // The buildspec's own `grep CODERHELM_EGRESS_BLOCKED` line is echoed in
    // every log; only the proxy's line of its own names a refused host.
    let log = codebuild_log(&["CODERHELM_CODEGEN_END exit=1"]);
    let o = sandbox::codegen_outcome(
        &FakeSandbox::new(vec![], log),
        "/aws/codebuild/sandbox",
        "s1",
        "out.tgz",
        false,
    )
    .await;
    assert!(matches!(
        generators::read_outcome(o, "pnpm run typegen", || true),
        Read::Failed(..)
    ));
}

#[test]
fn the_pipeline_stops_a_blocked_gate_with_the_gates_reason() {
    let rp = run_passes_src();
    let at = rp
        .find("Err(generators::GateStop::Blocked(why)) => {")
        .unwrap();
    assert!(rp[at..at + 400].contains("stopped_keeping(&why, &kept)"));
}
