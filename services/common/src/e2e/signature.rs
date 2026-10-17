//! Groups failures by what went wrong, not by the noise around it.
//!
//! Two failures share a signature when their messages match once ids,
//! numbers, timings and addresses are taken out, they fail in the same place in
//! the code, and they wait on the same element.

use std::sync::LazyLock;

use regex::Regex;

static ANSI: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\x1b\[[0-9;]*[A-Za-z]").expect("valid"));
static URL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"https?://[^\s'")\]]+"#).expect("valid"));
static UUID: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
        .expect("valid")
});
static HEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(0x)?[0-9a-f]{8,}\b").expect("valid"));
static NUMBER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\d+(\.\d+)?").expect("valid"));
static SPACES: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s+").expect("valid"));
static FRAME: LazyLock<Regex> = LazyLock::new(|| {
    // `at fn (path/to/file.ts:12:5)` or `at path/to/file.ts:12:5`
    Regex::new(r"^\s*at\s+(?:(?P<func>[^\s(]+)\s+\()?(?P<file>[^\s():]+):\d+(?::\d+)?\)?")
        .expect("valid")
});
/// A quoted string in any of JavaScript's three quotes, captured under `name`
/// with a suffix per quote kind (the regex engine has no backreferences).
fn quoted(name: &str, max: usize) -> String {
    format!(
        r#"(?:'(?P<{name}1>[^']{{1,{max}}})'|"(?P<{name}2>[^"]{{1,{max}}})"|`(?P<{name}3>[^`]{{1,{max}}})`)"#
    )
}

static SELECTORS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        format!(
            r"(?:locator|getByTestId|getByText|getByLabel|getByPlaceholder|getByTitle|getByAltText)\(\s*{}",
            quoted("sel", 200)
        ),
        format!(
            r"getByRole\(\s*{}(?:,\s*\{{\s*name:\s*{})?",
            quoted("sel", 80),
            quoted("name", 120)
        ),
        format!(r"\$\$?\(\s*{}", quoted("sel", 200)),
        // WebdriverIO: `element ("~Login") still not displayed`
        format!(r"element \(\s*{}\s*\)", quoted("sel", 200)),
        r#"(?P<sel1>\[data-testid=['"]?[^\]'"]{1,120}['"]?\])"#.to_string(),
    ]
    .into_iter()
    .map(|p| Regex::new(&p).expect("valid selector pattern"))
    .collect()
});

fn any_group<'t>(c: &regex::Captures<'t>, name: &str) -> Option<&'t str> {
    (1..=3).find_map(|i| c.name(&format!("{name}{i}")).map(|m| m.as_str()))
}

/// The message with the noise taken out: one line, at most 300 characters.
pub fn normalize_message(message: &str) -> String {
    let text = ANSI.replace_all(message, "");
    let first: String = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .take(3)
        .collect::<Vec<_>>()
        .join(" ");
    let t = URL.replace_all(&first, "<url>");
    let t = UUID.replace_all(&t, "<id>");
    let t = HEX.replace_all(&t, "<id>");
    let t = NUMBER.replace_all(&t, "N");
    let t = SPACES.replace_all(&t, " ");
    crate::truncate_str(t.trim(), 300).to_string()
}

/// The first few places in the code the failure passed through, without line
/// numbers or machine-specific folders, skipping framework internals.
pub fn top_frames(stack: &str, n: usize) -> Vec<String> {
    stack
        .lines()
        .filter_map(|l| FRAME.captures(l))
        .filter_map(|c| {
            let file = c.name("file")?.as_str();
            if file.contains("node_modules")
                || file.starts_with("node:")
                || file.starts_with("internal/")
            {
                return None;
            }
            let file = file
                .rsplit_once("/e2e/")
                .map(|(_, f)| format!("e2e/{f}"))
                .unwrap_or_else(|| {
                    // Keep the path from the first folder that looks like source.
                    ["/src/", "/test/", "/tests/", "/apps/", "/packages/"]
                        .iter()
                        .filter_map(|m| file.find(m))
                        .min()
                        .map(|i| file[i + 1..].to_string())
                        .unwrap_or_else(|| file.rsplit('/').next().unwrap_or(file).to_string())
                });
            Some(match c.name("func") {
                Some(f) => format!("{} {}", f.as_str(), file),
                None => file,
            })
        })
        .take(n)
        .collect()
}

/// The element a failure was waiting on, when the message names one.
pub fn selector(message: &str) -> Option<String> {
    let text = ANSI.replace_all(message, "");
    SELECTORS.iter().find_map(|re| {
        re.captures(&text).map(|c| {
            let sel = any_group(&c, "sel").unwrap_or("");
            match any_group(&c, "name") {
                Some(n) => format!("{sel} \"{n}\""),
                None => sel.to_string(),
            }
        })
    })
}

/// A short stable key for a failure.
pub fn signature(message: &str, stack: &str) -> String {
    let key = format!(
        "{}\n{}\n{}",
        normalize_message(message),
        top_frames(stack, 3).join("\n"),
        selector(message).unwrap_or_default()
    );
    crate::content_hash(&key)[..16].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noise_is_removed_from_messages() {
        let a = normalize_message(
            "\x1b[31mError: Timed out 15000ms waiting for expect(locator).toBeVisible()\x1b[39m\n\nCall log: order 7f3a9c2e1b4d at https://staging.example.com/a?x=1",
        );
        assert_eq!(
            a,
            "Error: Timed out Nms waiting for expect(locator).toBeVisible() Call log: order <id> at <url>"
        );
        assert_eq!(
            normalize_message("member 550e8400-e29b-41d4-a716-446655440000 missing"),
            "member <id> missing"
        );
    }

    #[test]
    fn frames_skip_internals_and_line_numbers() {
        let stack = "Error: x\n    at Object.<anonymous> (/home/runner/work/web/web/e2e/specs/join.spec.ts:42:11)\n    at /home/runner/work/web/web/node_modules/playwright/lib/x.js:1:1\n    at checkout (/home/runner/work/web/web/apps/web/src/lib/pay.ts:9:3)";
        assert_eq!(
            top_frames(stack, 3),
            vec![
                "Object.<anonymous> e2e/specs/join.spec.ts",
                "checkout apps/web/src/lib/pay.ts"
            ]
        );
    }

    #[test]
    fn selectors_are_pulled_out() {
        assert_eq!(
            selector("waiting for locator('[data-testid=fg-loader]')").as_deref(),
            Some("[data-testid=fg-loader]")
        );
        assert_eq!(
            selector("getByTestId('plan-card') resolved to 0 elements").as_deref(),
            Some("plan-card")
        );
        assert_eq!(
            selector("waiting for getByRole('button', { name: 'Continue' })").as_deref(),
            Some("button \"Continue\"")
        );
        assert_eq!(
            selector("element ('~LoginButton') still not displayed after $('~LoginButton')")
                .as_deref(),
            Some("~LoginButton")
        );
        assert_eq!(
            selector("element (\"~Home\") still not displayed after 15000ms").as_deref(),
            Some("~Home")
        );
        assert_eq!(selector("expected 3 to equal 4"), None);
    }

    #[test]
    fn the_same_failure_on_different_runs_shares_a_signature() {
        let stack = "    at t (/home/runner/work/a/a/e2e/x.spec.ts:1:1)";
        let s1 = signature(
            "Timed out 15000ms waiting for locator('#pay') on run 1234",
            stack,
        );
        let s2 = signature(
            "Timed out 30000ms waiting for locator('#pay') on run 98",
            stack,
        );
        assert_eq!(s1, s2);
        let other = signature("Timed out 15000ms waiting for locator('#cancel')", stack);
        assert_ne!(s1, other);
        assert_eq!(s1.len(), 16);
    }
}
