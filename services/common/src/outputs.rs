//! Files a run keeps as its outputs: the reports, tables and snapshots a
//! skill would have written to a folder on someone's computer.
//!
//! Kept in the data bucket under `teams/<team>/outputs/<run>/<name>`, read
//! back by the run (`read_output`) and listed and downloaded on the run's
//! page (`GET /api/runs/:run_id/outputs`). Names are relative paths of
//! letters, digits, `.`, `-`, `_` and `/`, never `..` or absolute, so an
//! output can't land outside its run's folder. Text only, bounded in count
//! and size.

/// Most files one run keeps.
pub const MAX_FILES: usize = 50;
/// Largest file kept.
pub const MAX_BYTES: usize = 5 * 1024 * 1024;

/// The folder of one run's outputs.
pub fn prefix(team_id: &str, run_id: &str) -> String {
    format!("teams/{team_id}/outputs/{}/", safe_id(run_id))
}

/// The key of one output.
pub fn key(team_id: &str, run_id: &str, name: &str) -> String {
    format!("{}{name}", prefix(team_id, run_id))
}

fn safe_id(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// A clean output name, or why it isn't one.
pub fn clean_name(name: &str) -> Result<String, String> {
    let n = name.trim().trim_start_matches("./").replace('\\', "/");
    if n.is_empty() || n.len() > 200 {
        return Err("Give the output a name of 1 to 200 characters, like report.html.".into());
    }
    if n.starts_with('/') || n.split('/').any(|p| p.is_empty() || p == "." || p == "..") {
        return Err(format!(
            "'{name}' isn't a plain relative name: use something like tables/summary.csv."
        ));
    }
    if !n
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '/'))
    {
        return Err(format!(
            "'{name}' has characters outputs can't keep: use letters, digits, '.', '-', '_' and '/'."
        ));
    }
    Ok(n)
}

/// The content type an output is served with, by its extension. Anything
/// that could run in a browser is served as plain text or as a download
/// (see [`disposition`]), never inline as a page of the dashboard's site.
pub fn content_type(name: &str) -> &'static str {
    match name
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_lowercase()
        .as_str()
    {
        "json" => "application/json",
        "csv" => "text/csv; charset=utf-8",
        "md" => "text/markdown; charset=utf-8",
        "html" | "htm" => "text/html; charset=utf-8",
        _ => "text/plain; charset=utf-8",
    }
}

/// How an output is served: HTML always as an attachment (it is the
/// team's script's page, not the dashboard's), the rest inline.
pub fn disposition(name: &str) -> String {
    let file = name.rsplit('/').next().unwrap_or(name);
    let html = matches!(
        name.rsplit('.')
            .next()
            .unwrap_or("")
            .to_lowercase()
            .as_str(),
        "html" | "htm"
    );
    format!(
        "{}; filename=\"{}\"",
        if html { "attachment" } else { "inline" },
        file.replace('"', "")
    )
}

/// Where a person finds a run's outputs: the run's page, Outputs.
pub fn page_link(base: &str, run_id: &str, name: &str) -> String {
    format!(
        "{}/runs/detail?id={run_id}&output={}#outputs",
        base.trim_end_matches('/'),
        urlencoding::encode(name)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_stay_inside_their_runs_folder() {
        assert_eq!(clean_name("./report.html").unwrap(), "report.html");
        assert_eq!(
            clean_name("tables/by_author.csv").unwrap(),
            "tables/by_author.csv"
        );
        for bad in [
            "",
            "../x",
            "/etc/passwd",
            "a//b",
            "a/../b",
            "a b.txt",
            "x;rm",
        ] {
            assert!(clean_name(bad).is_err(), "{bad}");
        }
        assert_eq!(key("t1", "r/1", "a.csv"), "teams/t1/outputs/r-1/a.csv");
    }

    #[test]
    fn html_is_a_download_and_the_rest_reads_in_place() {
        assert!(disposition("out/report.html").starts_with("attachment;"));
        assert!(disposition("summary.json").starts_with("inline;"));
        assert_eq!(content_type("t.csv"), "text/csv; charset=utf-8");
        assert_eq!(content_type("x.bin"), "text/plain; charset=utf-8");
        assert_eq!(
            page_link("https://app.example.com/", "r1", "a b.csv"),
            "https://app.example.com/runs/detail?id=r1&output=a%20b.csv#outputs"
        );
    }
}
