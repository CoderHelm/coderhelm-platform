//! Plain text from Jira's document format (ADF): ticket descriptions and
//! comments arrive as a JSON tree, sometimes inside a string.

use serde_json::Value;

/// A ticket or comment body's words: a document (or a document held in a
/// string) as plain text; any other string as it is.
pub fn body_text(body: &Value) -> String {
    match body {
        Value::String(s) => text_of(s),
        Value::Null => String::new(),
        other => adf_text(other),
    }
}

/// Stored text that may be a document saved as JSON: its words, else the
/// text unchanged.
pub fn text_of(raw: &str) -> String {
    let t = raw.trim_start();
    if t.starts_with('{') {
        if let Ok(v @ Value::Object(_)) = serde_json::from_str::<Value>(t) {
            if v.get("type").and_then(|x| x.as_str()) == Some("doc") {
                return adf_text(&v);
            }
        }
    }
    raw.to_string()
}

/// Extract plain text from Jira Atlassian Document Format (ADF).
/// ADF is a nested JSON tree with `content` arrays and `text` leaf nodes.
/// Falls back to raw JSON if the structure is unexpected.
pub fn adf_text(value: &Value) -> String {
    let mut parts = Vec::new();
    adf_text_inner(value, &mut parts);
    let result = parts.join("");
    if result.trim().is_empty() {
        // Fallback: serialize as JSON so we don't lose data entirely
        serde_json::to_string(value).unwrap_or_default()
    } else {
        result.trim().to_string()
    }
}

fn adf_text_inner(value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            // Text leaf node
            if let Some(Value::String(text)) = map.get("text") {
                parts.push(text.clone());
                return;
            }
            // Block nodes that should produce line breaks
            let node_type = map.get("type").and_then(|v| v.as_str()).unwrap_or("");
            let is_block = matches!(
                node_type,
                "paragraph"
                    | "heading"
                    | "bulletList"
                    | "orderedList"
                    | "listItem"
                    | "blockquote"
                    | "codeBlock"
                    | "rule"
                    | "table"
                    | "tableRow"
                    | "tableCell"
            );
            // Hard break inline node
            if node_type == "hardBreak" {
                parts.push("\n".to_string());
                return;
            }
            // Media nodes (images/attachments embedded in the description)
            if node_type == "media" || node_type == "mediaSingle" || node_type == "mediaInline" {
                let filename = map
                    .get("attrs")
                    .and_then(|a| a.get("alt"))
                    .and_then(|v| v.as_str())
                    .or_else(|| {
                        map.get("attrs")
                            .and_then(|a| a.get("id"))
                            .and_then(|v| v.as_str())
                    })
                    .unwrap_or("image");
                parts.push(format!("[Attached image: {filename}]"));
                // Recurse into children (mediaSingle wraps a media node)
                if let Some(Value::Array(children)) = map.get("content") {
                    for child in children {
                        adf_text_inner(child, parts);
                    }
                }
                return;
            }
            // Inline card (pasted URLs like Notion links)
            if node_type == "inlineCard" || node_type == "blockCard" {
                if let Some(url) = map
                    .get("attrs")
                    .and_then(|a| a.get("url"))
                    .and_then(|v| v.as_str())
                {
                    parts.push(url.to_string());
                }
                return;
            }
            if let Some(Value::Array(children)) = map.get("content") {
                for child in children {
                    adf_text_inner(child, parts);
                }
            }
            if is_block && !parts.last().is_none_or(|s| s.ends_with('\n')) {
                parts.push("\n".to_string());
            }
        }
        Value::Array(arr) => {
            for item in arr {
                adf_text_inner(item, parts);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc() -> Value {
        json!({"type":"doc","version":1,"content":[
            {"type":"paragraph","content":[{"type":"text","text":"Root cause confirmed.","marks":[{"type":"strong"}]}]},
            {"type":"paragraph","content":[{"type":"text","text":"Two "},{"type":"text","text":"schedule_group_class","marks":[{"type":"code"}]},{"type":"text","text":" rows are wrong."}]}
        ]})
    }

    #[test]
    fn a_document_in_a_string_becomes_its_words() {
        let raw = doc().to_string();
        let t = text_of(&raw);
        assert_eq!(
            t,
            "Root cause confirmed.\nTwo schedule_group_class rows are wrong."
        );
        assert_eq!(body_text(&Value::String(raw)), t);
        assert_eq!(body_text(&doc()), t);
    }

    #[test]
    fn plain_text_is_kept() {
        assert_eq!(
            text_of("fixed in the booking system"),
            "fixed in the booking system"
        );
        assert_eq!(text_of("{not json"), "{not json");
        assert_eq!(text_of(r#"{"a":1}"#), r#"{"a":1}"#);
        assert_eq!(body_text(&Value::Null), "");
    }
}
