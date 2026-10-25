//! Turning a provider's content into sections for indexing.

use serde_json::Value;

use super::super::chunk::Section;
use super::super::extract::{html_text, markdown_sections};

/// Markdown split at its headings, with the title as the first heading so
/// a passage's place reads "Title > Heading".
pub fn markdown(title: &str, md: &str) -> Vec<Section> {
    let body = if md.trim_start().starts_with("# ") || title.trim().is_empty() {
        md.to_string()
    } else {
        format!("# {}\n\n{md}", title.trim())
    };
    markdown_sections(&body)
        .into_iter()
        .filter(|s| !s.text.trim().is_empty())
        .collect()
}

/// An HTML page's readable text as one section.
pub fn html(title: &str, page: &str) -> Vec<Section> {
    let text = html_text(page);
    if text.trim().is_empty() {
        return Vec::new();
    }
    vec![Section {
        loc: title.trim().to_string(),
        text,
        header: None,
    }]
}

/// The `<title>` of an HTML page.
pub fn html_title(page: &str) -> Option<String> {
    let lower = page.to_ascii_lowercase();
    let start = lower.find("<title")?;
    let open_end = lower[start..].find('>')? + start + 1;
    let end = lower[open_end..].find("</title>")? + open_end;
    let t = html_text(&page[open_end..end]);
    let t = t.trim();
    (!t.is_empty()).then(|| crate::truncate_str(t, 300).to_string())
}

/// Sanity Portable Text (an array of blocks) as plain text: each block one
/// paragraph, headings marked, list items bulleted.
pub fn portable_text(v: &Value) -> String {
    let Some(blocks) = v.as_array() else {
        return String::new();
    };
    let mut out = Vec::new();
    for b in blocks {
        if b["_type"] != "block" {
            // A custom block (an image, a code block): keep any text it has.
            let t = loose_text(b, 0);
            if !t.trim().is_empty() {
                out.push(t);
            }
            continue;
        }
        let text: String = b["children"]
            .as_array()
            .map(|c| c.iter().filter_map(|s| s["text"].as_str()).collect())
            .unwrap_or_default();
        if text.trim().is_empty() {
            continue;
        }
        let style = b["style"].as_str().unwrap_or("normal");
        let line = match style {
            "h1" => format!("# {text}"),
            "h2" => format!("## {text}"),
            "h3" => format!("### {text}"),
            "h4" | "h5" | "h6" => format!("#### {text}"),
            "blockquote" => format!("> {text}"),
            _ if b["listItem"].is_string() => format!("- {text}"),
            _ => text,
        };
        out.push(line);
    }
    out.join("\n\n")
}

/// Every string in a value, depth-first, skipping Sanity's own fields
/// (`_id`, `_type`, `_ref`, ...) and anything that looks like an id or a
/// URL of an asset.
pub fn loose_text(v: &Value, depth: usize) -> String {
    if depth > 8 {
        return String::new();
    }
    match v {
        Value::String(s) => s.clone(),
        Value::Array(a) => {
            if a.iter().any(|x| x["_type"] == "block") {
                return portable_text(v);
            }
            a.iter()
                .map(|x| loose_text(x, depth + 1))
                .filter(|t| !t.trim().is_empty())
                .collect::<Vec<_>>()
                .join("\n")
        }
        Value::Object(m) => m
            .iter()
            .filter(|(k, _)| !k.starts_with('_') && k.as_str() != "asset")
            .map(|(_, x)| loose_text(x, depth + 1))
            .filter(|t| !t.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Number(n) => n.to_string(),
        _ => String::new(),
    }
}

/// A value at a dotted path (`slug.current`).
pub fn at_path<'a>(v: &'a Value, path: &str) -> &'a Value {
    path.split('.')
        .filter(|p| !p.is_empty())
        .fold(v, |cur, key| &cur[key])
}

/// `template` with each `{field.path}` replaced by that field's text,
/// percent-encoded where it goes into a URL path. `None` when a field is
/// missing.
pub fn fill_template(template: &str, doc: &Value) -> Option<String> {
    let mut out = String::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let close = rest[open..].find('}')? + open;
        let key = &rest[open + 1..close];
        let v = at_path(doc, key);
        let s = v
            .as_str()
            .map(str::to_string)
            .or_else(|| v.as_i64().map(|n| n.to_string()))?;
        if s.is_empty() {
            return None;
        }
        let encoded: String = s
            .split('/')
            .map(|seg| {
                url::form_urlencoded::byte_serialize(seg.as_bytes())
                    .collect::<String>()
                    .replace('+', "%20")
            })
            .collect::<Vec<_>>()
            .join("/");
        out.push_str(&encoded);
        rest = &rest[close + 1..];
    }
    out.push_str(rest);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn markdown_gets_the_title_as_its_top_heading() {
        let s = markdown("Runbook", "Intro\n\n## Deploy\nRun it.");
        assert_eq!(s[0].loc, "Runbook");
        assert_eq!(s[1].loc, "Runbook > Deploy");
        let own = markdown("Runbook", "# Own title\nText");
        assert_eq!(own[0].loc, "Own title");
    }

    #[test]
    fn portable_text_reads_as_paragraphs() {
        let pt = json!([
            {"_type":"block","style":"h2","children":[{"_type":"span","text":"Returns"}]},
            {"_type":"block","style":"normal","children":[{"text":"Within "},{"text":"30 days."}]},
            {"_type":"block","listItem":"bullet","children":[{"text":"Keep the receipt"}]},
            {"_type":"image","asset":{"_ref":"image-abc"},"alt":"A box"}
        ]);
        assert_eq!(
            portable_text(&pt),
            "## Returns\n\nWithin 30 days.\n\n- Keep the receipt\n\nA box"
        );
    }

    #[test]
    fn loose_text_skips_system_fields() {
        let v = json!({"_id":"x","_type":"faq","q":"Why?","a":[{"_type":"block","children":[{"text":"Because."}]}]});
        let t = loose_text(&v, 0);
        assert!(t.contains("Why?") && t.contains("Because.") && !t.contains("faq"));
    }

    #[test]
    fn templates_fill_from_fields() {
        let d = json!({"slug":{"current":"a b/c"},"lang":"en"});
        assert_eq!(
            fill_template("https://ex.com/{lang}/blog/{slug.current}", &d).as_deref(),
            Some("https://ex.com/en/blog/a%20b/c")
        );
        assert_eq!(fill_template("https://ex.com/{missing}", &d), None);
    }

    #[test]
    fn html_titles_are_read() {
        assert_eq!(
            html_title("<html><head><title> Pricing &amp; plans </title></head></html>").as_deref(),
            Some("Pricing & plans")
        );
        assert_eq!(html_title("<p>none</p>"), None);
    }
}
