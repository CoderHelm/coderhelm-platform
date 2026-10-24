//! Reading an uploaded file's text, in sections that citations can name.
//!
//! PDF (per page), Word `.docx` (split at headings), Markdown (split at
//! headings), plain text, CSV and Excel `.xlsx` (one section per sheet, the
//! header row kept), and HTML (tags, scripts and styles removed). Office
//! files and PDFs also give their own last-modified date, which citations
//! show as the "as of" date.

use std::io::Read;

use quick_xml::events::Event;
use quick_xml::Reader;

use super::chunk::Section;

/// A file kind the knowledge base reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Pdf,
    Docx,
    Markdown,
    Text,
    Csv,
    Xlsx,
    Html,
}

/// Every accepted extension, for the page and the upload check.
pub const EXTENSIONS: &[&str] = &[
    "pdf", "docx", "md", "markdown", "txt", "text", "csv", "xlsx", "html", "htm",
];

impl Kind {
    /// The kind of a file by its name's extension.
    pub fn of(name: &str) -> Option<Kind> {
        let ext = name.rsplit_once('.')?.1.to_ascii_lowercase();
        Some(match ext.as_str() {
            "pdf" => Kind::Pdf,
            "docx" => Kind::Docx,
            "md" | "markdown" => Kind::Markdown,
            "txt" | "text" => Kind::Text,
            "csv" => Kind::Csv,
            "xlsx" => Kind::Xlsx,
            "html" | "htm" => Kind::Html,
            _ => return None,
        })
    }

    /// The stored name of the kind.
    pub fn key(self) -> &'static str {
        match self {
            Kind::Pdf => "pdf",
            Kind::Docx => "docx",
            Kind::Markdown => "markdown",
            Kind::Text => "text",
            Kind::Csv => "csv",
            Kind::Xlsx => "xlsx",
            Kind::Html => "html",
        }
    }

    /// The content type the upload is signed with.
    pub fn content_type(self) -> &'static str {
        match self {
            Kind::Pdf => "application/pdf",
            Kind::Docx => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            Kind::Markdown => "text/markdown",
            Kind::Text => "text/plain",
            Kind::Csv => "text/csv",
            Kind::Xlsx => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            Kind::Html => "text/html",
        }
    }
}

/// What a file holds.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Extracted {
    pub sections: Vec<Section>,
    /// The document's own last-modified date (`YYYY-MM-DD`), when it has one.
    pub as_of: Option<String>,
}

/// Rows read from one sheet at most.
const MAX_ROWS: usize = 20_000;
/// Uncompressed bytes read from one part of an Office file at most.
const MAX_PART_BYTES: u64 = 64 * 1024 * 1024;

/// The text of a file, or why it can't be read (in plain words).
pub fn extract(kind: Kind, bytes: &[u8]) -> Result<Extracted, String> {
    let out = match kind {
        Kind::Text => Extracted {
            sections: vec![Section {
                text: text_of(bytes),
                ..Default::default()
            }],
            as_of: None,
        },
        Kind::Markdown => Extracted {
            sections: markdown_sections(&text_of(bytes)),
            as_of: None,
        },
        Kind::Csv => Extracted {
            sections: csv_sections(&text_of(bytes)),
            as_of: None,
        },
        Kind::Html => Extracted {
            sections: vec![Section {
                text: html_text(&text_of(bytes)),
                ..Default::default()
            }],
            as_of: None,
        },
        Kind::Docx => docx(bytes)?,
        Kind::Xlsx => xlsx(bytes)?,
        Kind::Pdf => pdf(bytes)?,
    };
    if out.sections.iter().all(|s| s.text.trim().is_empty()) {
        return Err(match kind {
            Kind::Pdf => "No text found in this PDF. A scanned PDF needs text recognition, which isn't supported yet.".into(),
            _ => "The file has no text to read.".into(),
        });
    }
    Ok(out)
}

/// Bytes as text: UTF-8 (a BOM dropped), invalid bytes replaced.
pub fn text_of(bytes: &[u8]) -> String {
    let b = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes);
    String::from_utf8_lossy(b).into_owned()
}

/// Markdown split at headings; each part is named by its heading trail
/// ("Setup > Install").
fn markdown_sections(text: &str) -> Vec<Section> {
    let mut out = Vec::new();
    let mut trail: Vec<(usize, String)> = Vec::new();
    let mut cur = String::new();
    let mut loc = String::new();
    let mut in_fence = false;
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
        }
        let level = if in_fence {
            0
        } else {
            line.chars().take_while(|c| *c == '#').count()
        };
        if (1..=6).contains(&level) && line[level..].starts_with(' ') {
            if !cur.trim().is_empty() {
                out.push(Section {
                    loc: loc.clone(),
                    text: std::mem::take(&mut cur),
                    header: None,
                });
            }
            cur.clear();
            let title = line[level..].trim().to_string();
            trail.retain(|(l, _)| *l < level);
            trail.push((level, title));
            loc = trail
                .iter()
                .map(|(_, t)| t.as_str())
                .collect::<Vec<_>>()
                .join(" > ");
        }
        cur.push_str(line);
        cur.push('\n');
    }
    if !cur.trim().is_empty() {
        out.push(Section {
            loc,
            text: cur,
            header: None,
        });
    }
    out
}

/// A CSV as one section: the header row kept, the rows as lines.
fn csv_sections(text: &str) -> Vec<Section> {
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let Some(header) = lines.next() else {
        return Vec::new();
    };
    let rows: Vec<&str> = lines.take(MAX_ROWS).collect();
    vec![Section {
        loc: "Rows".into(),
        text: rows.join("\n"),
        header: Some(header.to_string()),
    }]
}

/// The readable text of an HTML page: scripts, styles and tags removed,
/// blocks on their own lines, common entities decoded.
pub fn html_text(html: &str) -> String {
    let lower = html.to_ascii_lowercase();
    let mut out = String::with_capacity(html.len() / 2);
    let bytes = html.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'<' {
            // Skip a script or style element whole.
            let rest = &lower[i..];
            let skip_to = ["script", "style", "noscript", "template"]
                .iter()
                .find(|t| rest.starts_with(&format!("<{t}")))
                .and_then(|t| rest.find(&format!("</{t}")).map(|e| e + t.len() + 2));
            if let Some(n) = skip_to {
                i += n;
                if let Some(e) = lower[i..].find('>') {
                    i += e + 1;
                }
                continue;
            }
            let end = match lower[i..].find('>') {
                Some(e) => i + e + 1,
                None => bytes.len(),
            };
            let inner = &lower[i + 1..end.saturating_sub(1).max(i + 1)];
            let closing = inner.starts_with('/');
            let tag = inner
                .trim_start_matches('/')
                .split(|c: char| c.is_whitespace() || c == '/')
                .next()
                .unwrap_or("")
                .to_string();
            match tag.as_str() {
                "p" | "div" | "section" | "article" | "h1" | "h2" | "h3" | "h4" | "h5" | "h6"
                | "ul" | "ol" | "table" | "blockquote" | "pre" | "header" | "footer" | "main" => {
                    out.push_str("\n\n")
                }
                "br" => out.push('\n'),
                "li" | "tr" | "dt" | "dd" if !closing => out.push('\n'),
                "td" | "th" if !closing => out.push('\t'),
                _ => {}
            }
            i = end;
            continue;
        }
        let next = lower[i..].find('<').map(|n| i + n).unwrap_or(bytes.len());
        out.push_str(&decode_entities(&html[i..next]));
        i = next;
    }
    // Collapse runs of spaces and more than one blank line.
    let mut cleaned = String::with_capacity(out.len());
    let mut blank = 0;
    for line in out.lines() {
        let l = line.split_whitespace().collect::<Vec<_>>().join(" ");
        if l.is_empty() {
            blank += 1;
            if blank == 1 && !cleaned.is_empty() {
                cleaned.push('\n');
            }
        } else {
            blank = 0;
            cleaned.push_str(&l);
            cleaned.push('\n');
        }
    }
    cleaned.trim().to_string()
}

fn decode_entities(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        rest = &rest[i..];
        let Some(end) = rest[..rest.len().min(12)].find(';') else {
            out.push('&');
            rest = &rest[1..];
            continue;
        };
        let name = &rest[1..end];
        let ch = match name {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" | "#39" => Some('\''),
            "nbsp" => Some(' '),
            n if n.starts_with("#x") || n.starts_with("#X") => u32::from_str_radix(&n[2..], 16)
                .ok()
                .and_then(char::from_u32),
            n if n.starts_with('#') => n[1..].parse::<u32>().ok().and_then(char::from_u32),
            _ => None,
        };
        match ch {
            Some(c) => {
                out.push(c);
                rest = &rest[end + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

// ── Office files ────────────────────────────────────────────────────────────

fn open_zip(bytes: &[u8]) -> Result<zip::ZipArchive<std::io::Cursor<&[u8]>>, String> {
    zip::ZipArchive::new(std::io::Cursor::new(bytes))
        .map_err(|_| "The file isn't a valid Office document (it couldn't be opened).".to_string())
}

fn read_part(zip: &mut zip::ZipArchive<std::io::Cursor<&[u8]>>, name: &str) -> Option<String> {
    let f = zip.by_name(name).ok()?;
    let mut s = String::new();
    f.take(MAX_PART_BYTES).read_to_string(&mut s).ok()?;
    Some(s)
}

/// `docProps/core.xml`'s modified date, as `YYYY-MM-DD`.
fn office_modified(zip: &mut zip::ZipArchive<std::io::Cursor<&[u8]>>) -> Option<String> {
    let core = read_part(zip, "docProps/core.xml")?;
    let start = core.find("<dcterms:modified")?;
    let open_end = core[start..].find('>')? + start + 1;
    let date = core[open_end..].get(..10)?;
    valid_date(date).then(|| date.to_string())
}

fn valid_date(d: &str) -> bool {
    chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").is_ok()
}

fn local(name: &[u8]) -> &[u8] {
    match name.iter().rposition(|b| *b == b':') {
        Some(i) => &name[i + 1..],
        None => name,
    }
}

fn attr(e: &quick_xml::events::BytesStart, key: &[u8]) -> Option<String> {
    e.attributes().flatten().find_map(|a| {
        (local(a.key.as_ref()) == key).then(|| String::from_utf8_lossy(&a.value).into_owned())
    })
}

/// A Word document: paragraphs, a section per heading.
fn docx(bytes: &[u8]) -> Result<Extracted, String> {
    let mut zip = open_zip(bytes)?;
    let xml = read_part(&mut zip, "word/document.xml")
        .ok_or("The Word document has no body (word/document.xml is missing).")?;
    let as_of = office_modified(&mut zip);
    let mut reader = Reader::from_str(&xml);
    let mut sections: Vec<Section> = Vec::new();
    let mut cur = Section::default();
    let mut para = String::new();
    let mut heading = false;
    let mut in_text = false;
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => match local(e.name().as_ref()) {
                b"p" => {
                    para.clear();
                    heading = false;
                }
                b"t" => in_text = true,
                _ => {}
            },
            Ok(Event::Empty(e)) => match local(e.name().as_ref()) {
                b"pStyle" => {
                    let v = attr(&e, b"val").unwrap_or_default().to_ascii_lowercase();
                    heading = v.starts_with("heading") || v == "title";
                }
                b"tab" => para.push('\t'),
                b"br" | b"cr" => para.push('\n'),
                _ => {}
            },
            Ok(Event::Text(t)) if in_text => {
                if let Ok(s) = t.unescape() {
                    para.push_str(&s);
                }
            }
            Ok(Event::End(e)) => match local(e.name().as_ref()) {
                b"t" => in_text = false,
                b"p" => {
                    let p = para.trim().to_string();
                    if heading && !p.is_empty() {
                        if !cur.text.trim().is_empty() {
                            sections.push(std::mem::take(&mut cur));
                        }
                        cur.loc = crate::truncate_str(&p, 120).to_string();
                    }
                    if !p.is_empty() {
                        cur.text.push_str(&p);
                        cur.text.push_str("\n\n");
                    }
                    para.clear();
                }
                b"tc" => para.push('\t'),
                _ => {}
            },
            Ok(Event::Eof) => break,
            Err(_) => return Err("The Word document couldn't be read (its XML is damaged).".into()),
            _ => {}
        }
    }
    if !cur.text.trim().is_empty() {
        sections.push(cur);
    }
    Ok(Extracted { sections, as_of })
}

/// An Excel workbook: a section per sheet, rows as ` | `-separated cells,
/// the first row kept as the header.
fn xlsx(bytes: &[u8]) -> Result<Extracted, String> {
    let mut zip = open_zip(bytes)?;
    let as_of = office_modified(&mut zip);
    let shared = read_part(&mut zip, "xl/sharedStrings.xml")
        .map(|x| shared_strings(&x))
        .unwrap_or_default();
    let workbook = read_part(&mut zip, "xl/workbook.xml")
        .ok_or("The workbook has no sheets (xl/workbook.xml is missing).")?;
    let rels = read_part(&mut zip, "xl/_rels/workbook.xml.rels").unwrap_or_default();
    let targets = relationships(&rels);
    let mut sections = Vec::new();
    for (i, (name, rid)) in sheet_list(&workbook).into_iter().enumerate() {
        let target = targets
            .iter()
            .find(|(id, _)| *id == rid)
            .map(|(_, t)| t.clone())
            .unwrap_or_else(|| format!("worksheets/sheet{}.xml", i + 1));
        let path = match target.strip_prefix('/') {
            Some(abs) => abs.to_string(),
            None => format!("xl/{target}"),
        };
        let Some(xml) = read_part(&mut zip, &path) else {
            continue;
        };
        let rows = sheet_rows(&xml, &shared);
        let mut rows = rows.into_iter().filter(|r| !r.trim().is_empty());
        let Some(header) = rows.next() else {
            continue;
        };
        sections.push(Section {
            loc: format!("Sheet: {name}"),
            text: rows.take(MAX_ROWS).collect::<Vec<_>>().join("\n"),
            header: Some(header),
        });
    }
    Ok(Extracted { sections, as_of })
}

fn shared_strings(xml: &str) -> Vec<String> {
    let mut reader = Reader::from_str(xml);
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_t = false;
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => match local(e.name().as_ref()) {
                b"si" => cur.clear(),
                b"t" => in_t = true,
                _ => {}
            },
            Ok(Event::Text(t)) if in_t => {
                if let Ok(s) = t.unescape() {
                    cur.push_str(&s);
                }
            }
            Ok(Event::End(e)) => match local(e.name().as_ref()) {
                b"t" => in_t = false,
                b"si" => out.push(std::mem::take(&mut cur)),
                _ => {}
            },
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
    }
    out
}

/// `(sheet name, relationship id)` in workbook order.
fn sheet_list(xml: &str) -> Vec<(String, String)> {
    let mut reader = Reader::from_str(xml);
    let mut out = Vec::new();
    loop {
        match reader.read_event() {
            Ok(Event::Empty(e)) | Ok(Event::Start(e)) if local(e.name().as_ref()) == b"sheet" => {
                out.push((
                    attr(&e, b"name").unwrap_or_default(),
                    attr(&e, b"id").unwrap_or_default(),
                ));
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
    }
    out
}

/// `(id, target)` of each relationship.
fn relationships(xml: &str) -> Vec<(String, String)> {
    let mut reader = Reader::from_str(xml);
    let mut out = Vec::new();
    loop {
        match reader.read_event() {
            Ok(Event::Empty(e)) | Ok(Event::Start(e))
                if local(e.name().as_ref()) == b"Relationship" =>
            {
                out.push((
                    attr(&e, b"Id").unwrap_or_default(),
                    attr(&e, b"Target").unwrap_or_default(),
                ));
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
    }
    out
}

fn sheet_rows(xml: &str, shared: &[String]) -> Vec<String> {
    let mut reader = Reader::from_str(xml);
    let mut rows = Vec::new();
    let mut row: Vec<String> = Vec::new();
    let mut cell_type = String::new();
    let mut cell = String::new();
    let mut in_value = false;
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => match local(e.name().as_ref()) {
                b"row" => row.clear(),
                b"c" => {
                    cell_type = attr(&e, b"t").unwrap_or_default();
                    cell.clear();
                }
                b"v" | b"t" => in_value = true,
                _ => {}
            },
            Ok(Event::Text(t)) if in_value => {
                if let Ok(s) = t.unescape() {
                    cell.push_str(&s);
                }
            }
            Ok(Event::End(e)) => match local(e.name().as_ref()) {
                b"v" | b"t" => in_value = false,
                b"c" => {
                    let v = if cell_type == "s" {
                        cell.trim()
                            .parse::<usize>()
                            .ok()
                            .and_then(|i| shared.get(i).cloned())
                            .unwrap_or_default()
                    } else {
                        cell.clone()
                    };
                    row.push(v.replace(['\n', '\r'], " ").trim().to_string());
                }
                b"row" => {
                    while row.last().is_some_and(|c| c.is_empty()) {
                        row.pop();
                    }
                    rows.push(row.join(" | "));
                    if rows.len() > MAX_ROWS {
                        break;
                    }
                }
                _ => {}
            },
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
    }
    rows
}

// ── PDF ─────────────────────────────────────────────────────────────────────

/// A PDF: one section per page.
fn pdf(bytes: &[u8]) -> Result<Extracted, String> {
    let doc = lopdf::Document::load_mem(bytes)
        .map_err(|_| "The PDF couldn't be opened (it may be damaged).".to_string())?;
    if doc.is_encrypted() {
        return Err("The PDF is password protected. Upload a copy without a password.".into());
    }
    let mut sections = Vec::new();
    for (num, _) in doc.get_pages() {
        let text = doc.extract_text(&[num]).unwrap_or_default();
        if !text.trim().is_empty() {
            sections.push(Section {
                loc: format!("Page {num}"),
                text,
                header: None,
            });
        }
    }
    Ok(Extracted {
        sections,
        as_of: pdf_modified(&doc),
    })
}

/// The PDF's `/ModDate` (or `/CreationDate`) as `YYYY-MM-DD`.
fn pdf_modified(doc: &lopdf::Document) -> Option<String> {
    let info = doc.trailer.get(b"Info").ok()?;
    let dict = match info {
        lopdf::Object::Reference(id) => doc.get_dictionary(*id).ok()?,
        lopdf::Object::Dictionary(d) => d,
        _ => return None,
    };
    for key in [&b"ModDate"[..], &b"CreationDate"[..]] {
        if let Ok(lopdf::Object::String(raw, _)) = dict.get(key) {
            if let Some(d) = pdf_date(&String::from_utf8_lossy(raw)) {
                return Some(d);
            }
        }
    }
    None
}

/// `D:20240131120000Z` → `2024-01-31`.
pub fn pdf_date(raw: &str) -> Option<String> {
    let s = raw.trim().trim_start_matches("D:");
    if s.len() < 8 || !s[..8].bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let d = format!("{}-{}-{}", &s[..4], &s[4..6], &s[6..8]);
    valid_date(&d).then_some(d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn zip_of(parts: &[(&str, &str)]) -> Vec<u8> {
        let mut buf = std::io::Cursor::new(Vec::new());
        {
            let mut w = zip::ZipWriter::new(&mut buf);
            let opts = zip::write::SimpleFileOptions::default();
            for (name, body) in parts {
                w.start_file(*name, opts).unwrap();
                w.write_all(body.as_bytes()).unwrap();
            }
            w.finish().unwrap();
        }
        buf.into_inner()
    }

    #[test]
    fn kinds_come_from_the_extension() {
        assert_eq!(Kind::of("Q3 Plan.PDF"), Some(Kind::Pdf));
        assert_eq!(Kind::of("notes.md"), Some(Kind::Markdown));
        assert_eq!(Kind::of("prices.xlsx"), Some(Kind::Xlsx));
        assert_eq!(Kind::of("page.htm"), Some(Kind::Html));
        assert_eq!(Kind::of("run.exe"), None);
        assert_eq!(Kind::of("noext"), None);
        for e in EXTENSIONS {
            assert!(Kind::of(&format!("f.{e}")).is_some(), "{e}");
        }
    }

    #[test]
    fn markdown_is_split_at_headings_with_a_trail() {
        let md = "Intro line.\n\n# Setup\nInstall it.\n## Install\nRun make.\n```\n# not a heading\n```\n# Usage\nCall it.";
        let x = extract(Kind::Markdown, md.as_bytes()).unwrap();
        let locs: Vec<&str> = x.sections.iter().map(|s| s.loc.as_str()).collect();
        assert_eq!(locs, ["", "Setup", "Setup > Install", "Usage"]);
        assert!(x.sections[2].text.contains("# not a heading"));
    }

    #[test]
    fn csv_keeps_its_header() {
        let x = extract(Kind::Csv, b"\xEF\xBB\xBFsku,price\na,1\nb,2\n").unwrap();
        assert_eq!(x.sections[0].header.as_deref(), Some("sku,price"));
        assert_eq!(x.sections[0].text, "a,1\nb,2");
    }

    #[test]
    fn html_loses_tags_scripts_and_entities() {
        let html = "<html><head><style>p{color:red}</style><script>alert('x')</script></head>\
                    <body><h1>Refunds</h1><p>Within&nbsp;5 days &amp; free.</p><ul><li>One</li><li>Two</li></ul>\
                    <table><tr><td>a</td><td>b</td></tr></table></body></html>";
        let t = html_text(html);
        assert!(t.contains("Refunds"));
        assert!(t.contains("Within 5 days & free."));
        assert!(t.contains("One\nTwo"));
        assert!(!t.contains("alert") && !t.contains("color:red") && !t.contains('<'));
    }

    #[test]
    fn docx_paragraphs_headings_and_date() {
        let doc = r#"<?xml version="1.0"?><w:document xmlns:w="w"><w:body>
            <w:p><w:r><w:t>Preface text.</w:t></w:r></w:p>
            <w:p><w:pPr><w:pStyle w:val="Heading1"/></w:pPr><w:r><w:t>Refund policy</w:t></w:r></w:p>
            <w:p><w:r><w:t xml:space="preserve">Refunds take </w:t></w:r><w:r><w:t>5 days &amp; more.</w:t></w:r></w:p>
            </w:body></w:document>"#;
        let core = r#"<cp:coreProperties><dcterms:modified xsi:type="dcterms:W3CDTF">2026-03-14T10:00:00Z</dcterms:modified></cp:coreProperties>"#;
        let bytes = zip_of(&[("word/document.xml", doc), ("docProps/core.xml", core)]);
        let x = extract(Kind::Docx, &bytes).unwrap();
        assert_eq!(x.as_of.as_deref(), Some("2026-03-14"));
        assert_eq!(x.sections.len(), 2);
        assert_eq!(x.sections[1].loc, "Refund policy");
        assert!(x.sections[1].text.contains("Refunds take 5 days & more."));
    }

    #[test]
    fn xlsx_sheets_rows_and_shared_strings() {
        let wb = r#"<workbook xmlns:r="r"><sheets><sheet name="Prices" sheetId="1" r:id="rId1"/></sheets></workbook>"#;
        let rels = r#"<Relationships><Relationship Id="rId1" Target="worksheets/sheet1.xml"/></Relationships>"#;
        let ss = r#"<sst><si><t>sku</t></si><si><t>price</t></si><si><r><t>wid</t></r><r><t>get</t></r></si></sst>"#;
        let sheet = r#"<worksheet><sheetData>
            <row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>
            <row r="2"><c r="A2" t="s"><v>2</v></c><c r="B2"><v>9.5</v></c></row>
            <row r="3"><c r="A3" t="inlineStr"><is><t>gizmo</t></is></c><c r="B3"><v>3</v></c></row>
            </sheetData></worksheet>"#;
        let bytes = zip_of(&[
            ("xl/workbook.xml", wb),
            ("xl/_rels/workbook.xml.rels", rels),
            ("xl/sharedStrings.xml", ss),
            ("xl/worksheets/sheet1.xml", sheet),
        ]);
        let x = extract(Kind::Xlsx, &bytes).unwrap();
        assert_eq!(x.sections.len(), 1);
        assert_eq!(x.sections[0].loc, "Sheet: Prices");
        assert_eq!(x.sections[0].header.as_deref(), Some("sku | price"));
        assert_eq!(x.sections[0].text, "widget | 9.5\ngizmo | 3");
    }

    #[test]
    fn damaged_or_empty_files_fail_with_a_reason() {
        assert!(extract(Kind::Docx, b"not a zip")
            .unwrap_err()
            .contains("Office"));
        assert!(extract(Kind::Pdf, b"%PDF-garbage")
            .unwrap_err()
            .contains("PDF"));
        assert!(extract(Kind::Text, b"   \n")
            .unwrap_err()
            .contains("no text"));
    }

    #[test]
    fn pdf_dates_read_as_days() {
        assert_eq!(pdf_date("D:20240131120000Z").as_deref(), Some("2024-01-31"));
        assert_eq!(pdf_date("D:2024"), None);
        assert_eq!(pdf_date("D:20241399"), None);
    }

    #[test]
    fn a_real_pdf_reads_per_page() {
        // A minimal PDF built with lopdf: one page that says "Refunds take five days".
        use lopdf::content::{Content, Operation};
        use lopdf::{dictionary, Object, Stream};
        let mut doc = lopdf::Document::with_version("1.5");
        let pages_id = doc.new_object_id();
        let font_id = doc.add_object(dictionary! {
            "Type" => "Font", "Subtype" => "Type1", "BaseFont" => "Courier",
        });
        let resources_id = doc.add_object(dictionary! {
            "Font" => dictionary! { "F1" => font_id },
        });
        let content = Content {
            operations: vec![
                Operation::new("BT", vec![]),
                Operation::new("Tf", vec!["F1".into(), 24.into()]),
                Operation::new("Td", vec![100.into(), 600.into()]),
                Operation::new("Tj", vec![Object::string_literal("Refunds take five days")]),
                Operation::new("ET", vec![]),
            ],
        };
        let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode().unwrap()));
        let page_id = doc.add_object(dictionary! {
            "Type" => "Page", "Parent" => pages_id, "Contents" => content_id,
        });
        doc.objects.insert(
            pages_id,
            Object::Dictionary(dictionary! {
                "Type" => "Pages", "Kids" => vec![page_id.into()], "Count" => 1,
                "Resources" => resources_id, "MediaBox" => vec![0.into(), 0.into(), 595.into(), 842.into()],
            }),
        );
        let catalog_id = doc.add_object(dictionary! { "Type" => "Catalog", "Pages" => pages_id });
        doc.trailer.set("Root", catalog_id);
        let mut bytes = Vec::new();
        doc.save_to(&mut bytes).unwrap();
        let x = extract(Kind::Pdf, &bytes).unwrap();
        assert_eq!(x.sections[0].loc, "Page 1");
        assert!(
            x.sections[0].text.contains("Refunds take five days"),
            "{:?}",
            x.sections
        );
    }
}
