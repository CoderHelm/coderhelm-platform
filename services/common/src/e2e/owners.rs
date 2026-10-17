//! Who owns a test, from the repo's CODEOWNERS file.

use super::coverage::matches;

pub const CODEOWNERS_PATHS: &[&str] = &[".github/CODEOWNERS", "CODEOWNERS", "docs/CODEOWNERS"];

/// The owners of `path`: the last matching CODEOWNERS line wins, as on GitHub.
pub fn owners_of(codeowners: &str, path: &str) -> Option<String> {
    let mut found: Option<String> = None;
    for line in codeowners.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        let mut parts = line.split_whitespace();
        let (Some(pattern), owners) = (parts.next(), parts.collect::<Vec<_>>()) else {
            continue;
        };
        if owners.is_empty() {
            continue;
        }
        if pattern_matches(pattern, path) {
            found = Some(owners.join(" "));
        }
    }
    found
}

fn pattern_matches(pattern: &str, path: &str) -> bool {
    let anchored = pattern.starts_with('/');
    let mut p = pattern.trim_start_matches('/').to_string();
    if p.ends_with('/') {
        p.push_str("**");
    }
    if p == "*" {
        return true;
    }
    // A pattern without a slash (besides a trailing one) matches at any depth.
    let glob = if anchored || p.trim_end_matches("/**").contains('/') {
        p.clone()
    } else {
        format!("**/{p}")
    };
    matches(&glob, path) || matches(&format!("{glob}/**"), path)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILE: &str = "# owners\n* @acme/everyone\n/e2e/ @acme/qa\n*.feature @acme/mobile\napps/web/src/features/checkout/ @acme/payments  # pay\n";

    #[test]
    fn the_last_matching_line_wins() {
        assert_eq!(
            owners_of(FILE, "README.md").as_deref(),
            Some("@acme/everyone")
        );
        assert_eq!(
            owners_of(FILE, "e2e/specs/pay.spec.ts").as_deref(),
            Some("@acme/qa")
        );
        assert_eq!(
            owners_of(FILE, "test/features/login.feature").as_deref(),
            Some("@acme/mobile")
        );
        assert_eq!(
            owners_of(FILE, "apps/web/src/features/checkout/Pay.tsx").as_deref(),
            Some("@acme/payments")
        );
        assert_eq!(owners_of("", "a"), None);
    }
}
