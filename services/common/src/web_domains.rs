//! The team's own public domains, which agents may check pages on with
//! `check_url` (see the worker's `passes::web_check`). Admins list them in
//! the team's review settings (`REVIEW_CONFIG#GLOBAL`, attribute
//! [`ATTR`]); a listed domain covers its subdomains.

/// Settings attribute holding the list.
pub const ATTR: &str = "web_domains";
/// Most domains a team may list.
pub const MAX: usize = 50;

/// A domain as an admin may list it: a bare lowercase host name with at
/// least two labels (a scheme, `*.` or a trailing slash are dropped). `None`
/// when it isn't one, including IP addresses.
pub fn clean(raw: &str) -> Option<String> {
    let d = raw
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_start_matches("*.")
        .trim_end_matches('/')
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let ok = d.len() <= 253
        && d.contains('.')
        && d.split('.').all(|l| {
            !l.is_empty()
                && l.len() <= 63
                && !l.starts_with('-')
                && !l.ends_with('-')
                && l.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        })
        && !d.split('.').all(|l| l.chars().all(|c| c.is_ascii_digit()));
    ok.then_some(d)
}

/// A list, cleaned, deduplicated, at most [`MAX`]. Entries that aren't
/// domains are dropped.
pub fn clean_all<S: AsRef<str>>(raw: &[S]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for d in raw.iter().filter_map(|d| clean(d.as_ref())) {
        if !out.contains(&d) {
            out.push(d);
        }
        if out.len() == MAX {
            break;
        }
    }
    out
}

/// The entries of `raw` that aren't domains, to tell the admin.
pub fn rejected<S: AsRef<str>>(raw: &[S]) -> Vec<String> {
    raw.iter()
        .map(|d| d.as_ref().trim())
        .filter(|d| !d.is_empty() && clean(d).is_none())
        .map(String::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_domains_are_cleaned() {
        let raw = [
            " https://Example.com/ ",
            "*.example.org",
            "example.com",
            "localhost",
            "10.0.0.1",
            "bad_domain.com",
        ];
        assert_eq!(clean_all(&raw), vec!["example.com", "example.org"]);
        assert_eq!(
            rejected(&raw),
            vec!["localhost", "10.0.0.1", "bad_domain.com"]
        );
    }

    #[test]
    fn at_most_fifty() {
        let raw: Vec<String> = (0..80).map(|i| format!("s{i}.example.com")).collect();
        assert_eq!(clean_all(&raw).len(), MAX);
    }
}
