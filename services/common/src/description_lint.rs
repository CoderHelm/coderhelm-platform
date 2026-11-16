//! The last check on a pull request description before it is written.
//!
//! A web pull request's description carried "History of the touched files"
//! three times, one section per fix round, and a list of deploy workflows
//! for services the change never touched. Whatever section writer produced
//! it, the description is held here at the one place it is written
//! (`GitHubClient::create_pull_request` and `update_pr_body`):
//!
//! - a section said twice keeps only its newest (last) copy;
//! - a line that lists several deploy workflows (a deploy map) goes: the
//!   release section names only the deploy the change waits on;
//! - "who this affects" with no use found goes: a search that found nothing
//!   tells a reviewer nothing.
//!
//! [`defects`] is the check; [`clean`] applies it.

/// A section heading: `## X`, `### X`, or a line that is only a bold label
/// (`**History of the touched files**`).
fn heading(l: &str) -> Option<String> {
    let t = l.trim();
    let name = if let Some(h) = t.strip_prefix("### ").or_else(|| t.strip_prefix("## ")) {
        h
    } else if t.starts_with("**")
        && t.ends_with("**")
        && t.len() > 4
        && !t[2..t.len() - 2].contains("**")
    {
        &t[2..t.len() - 2]
    } else {
        return None;
    };
    let n = name.trim().trim_end_matches(':').trim().to_lowercase();
    (!n.is_empty()).then_some(n)
}

/// A line that ends a section without being one (a collapsed block's
/// edge, a hidden marker).
fn boundary(l: &str) -> bool {
    let t = l.trim_start();
    t.starts_with("<!--") || t.starts_with("<details") || t.starts_with("</details")
}

/// A line listing more than one deploy workflow.
fn deploy_map(l: &str) -> bool {
    l.matches(".github/workflows/").count() > 1
        || (l.contains("How it deploys") && l.matches("deploys on").count() > 1)
}

/// "Who this affects" saying nothing was found.
fn empty_affects(l: &str) -> bool {
    let t = l.replace("**", "").to_lowercase();
    t.trim_start().starts_with("who this affects:") && t.contains("nothing outside")
}

/// What is wrong with a description; empty when it holds.
pub fn defects(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    for l in body.lines() {
        if let Some(h) = heading(l) {
            if seen.contains(&h)
                && !out
                    .iter()
                    .any(|o: &String| o.ends_with(&format!("\"{h}\"")))
            {
                out.push(format!("section repeated: \"{h}\""));
            }
            seen.push(h);
        }
        if deploy_map(l) {
            out.push("lists several deploy workflows".to_string());
        }
        if empty_affects(l) {
            out.push("says who it affects with no use found".to_string());
        }
    }
    out
}

/// The description with [`defects`] taken out: an earlier copy of a
/// repeated section, deploy maps, an empty "who this affects".
pub fn clean(body: &str) -> String {
    let lines: Vec<&str> = body.lines().collect();
    // Where each section starts and ends.
    let mut sections: Vec<(String, usize, usize)> = Vec::new();
    for (i, l) in lines.iter().enumerate() {
        if let Some(h) = heading(l) {
            let end = lines[i + 1..]
                .iter()
                .position(|n| heading(n).is_some() || boundary(n))
                .map_or(lines.len(), |p| i + 1 + p);
            sections.push((h, i, end));
        }
    }
    let mut drop = vec![false; lines.len()];
    for (k, (h, start, end)) in sections.iter().enumerate() {
        if sections[k + 1..].iter().any(|(n, _, _)| n == h) {
            for d in drop.iter_mut().take(*end).skip(*start) {
                *d = true;
            }
        }
    }
    for (i, l) in lines.iter().enumerate() {
        if deploy_map(l) || empty_affects(l) {
            drop[i] = true;
        }
    }
    let mut out: Vec<&str> = Vec::new();
    for (i, l) in lines.iter().enumerate() {
        if drop[i] {
            continue;
        }
        // No run of blank lines left behind.
        if l.trim().is_empty() && out.last().is_some_and(|p| p.trim().is_empty()) {
            continue;
        }
        out.push(l);
    }
    let mut s = out.join("\n");
    if body.ends_with('\n') && !s.ends_with('\n') {
        s.push('\n');
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    const BEFORE: &str = "### Summary\n\nAdds the sign-up page.\n\n**History of the touched files**\n- `app/page.tsx`: round one.\n\n**History of the touched files**\n- `app/page.tsx`: round two.\n\n**How it deploys:** Staging deploys on merge (`.github/workflows/deploy-a.yml`); Staging deploys on merge (`.github/workflows/deploy-b.yml`).\n\n**Who this affects:** nothing outside this repository (searched the team's repositories).\n\n**History of the touched files**\n- `app/page.tsx`: round three.\n\n### Verification\n\nTests pass.\n";

    #[test]
    fn a_repeated_section_keeps_its_newest_copy() {
        let d = defects(BEFORE);
        assert!(
            d.iter().any(|x| x.contains("history of the touched files")),
            "{d:?}"
        );
        assert!(d.iter().any(|x| x.contains("deploy workflows")), "{d:?}");
        assert!(d.iter().any(|x| x.contains("no use found")), "{d:?}");
        let after = clean(BEFORE);
        assert_eq!(
            after.matches("History of the touched files").count(),
            1,
            "{after}"
        );
        assert!(
            after.contains("round three") && !after.contains("round one"),
            "{after}"
        );
        assert!(!after.contains(".github/workflows/"), "{after}");
        assert!(!after.contains("Who this affects"), "{after}");
        assert!(after.contains("### Verification\n\nTests pass."), "{after}");
        assert!(defects(&after).is_empty(), "{:?}", defects(&after));
        assert!(!after.contains("\n\n\n"), "{after}");
    }

    #[test]
    fn a_clean_description_is_left_alone() {
        let body = "### Summary\n\nFixes the total.\n\n**Who this affects**\n- `billing` (uses `total()`): updated in this pull request\n\n<!-- coderhelm:x -->\n";
        assert_eq!(clean(body), body);
        assert!(defects(body).is_empty());
    }
}
