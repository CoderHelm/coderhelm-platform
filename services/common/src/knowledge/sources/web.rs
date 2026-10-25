//! Public web pages: single addresses, or every page a sitemap lists.
//!
//! No sign-in. Every request goes through the guarded HTTPS client: public
//! addresses only (checked after DNS and on every redirect), no proxy, size
//! and time caps. A sitemap's pages must be on the sitemap's own host. A
//! sitemap's `<lastmod>` is a page's marker; a page without one is fetched
//! each sync and its text hash decides whether anything is re-indexed. A
//! page that disappears from the sitemap, or answers 404 or 410, is removed.

use super::connector::{send, Connector, ConnectorError, Document, Listed, Listing, Node};
use super::{text, Kind, Source};
use crate::mcp_remote::http::{check_url, Http, HttpRequest, Limit};

/// Sitemaps read per source at most (an index and its children).
const MAX_SITEMAPS: usize = 20;

pub struct Web<'a> {
    pub http: &'a dyn Http,
}

/// The `<loc>` (and `<lastmod>`) of each `<url>` or `<sitemap>` entry.
pub fn parse_sitemap(xml: &str) -> (Vec<(String, String)>, Vec<String>) {
    let mut pages = Vec::new();
    let mut maps = Vec::new();
    let tag = |block: &str, name: &str| -> Option<String> {
        let open = format!("<{name}>");
        let close = format!("</{name}>");
        let s = block.find(&open)? + open.len();
        let e = block[s..].find(&close)? + s;
        Some(
            block[s..e]
                .trim()
                .trim_start_matches("<![CDATA[")
                .trim_end_matches("]]>")
                .replace("&amp;", "&")
                .trim()
                .to_string(),
        )
    };
    for (entry, is_map) in [("url", false), ("sitemap", true)] {
        let open = format!("<{entry}>");
        let close = format!("</{entry}>");
        let mut rest = xml;
        while let Some(s) = rest.find(&open) {
            let after = &rest[s + open.len()..];
            let Some(e) = after.find(&close) else { break };
            let block = &after[..e];
            if let Some(loc) = tag(block, "loc") {
                if is_map {
                    maps.push(loc);
                } else {
                    pages.push((loc, tag(block, "lastmod").unwrap_or_default()));
                }
            }
            rest = &after[e + close.len()..];
        }
    }
    (pages, maps)
}

fn host_of(u: &str) -> String {
    reqwest::Url::parse(u)
        .ok()
        .and_then(|u| u.host_str().map(str::to_ascii_lowercase))
        .unwrap_or_default()
}

/// `url:<address>` / `sitemap:<address>` as (kind, address), the address
/// checked to be public HTTPS.
pub fn split_id(raw: &str) -> Option<(&str, &str)> {
    let (k, u) = raw.split_once(':')?;
    (matches!(k, "url" | "sitemap") && check_url(u).is_ok()).then_some((k, u))
}

impl Web<'_> {
    fn get(&self, url: &str) -> HttpRequest {
        let mut r = HttpRequest::get(url);
        r.headers.retain(|(k, _)| k != "Accept");
        r.headers.push((
            "Accept".into(),
            "text/html,application/xhtml+xml,text/plain,text/markdown,application/xml;q=0.9,*/*;q=0.5".into(),
        ));
        r.limit = Limit::Document;
        r
    }

    async fn sitemap_pages(
        &self,
        root: &str,
    ) -> Result<(Vec<(String, String)>, bool), ConnectorError> {
        let host = host_of(root);
        let mut out = Vec::new();
        let mut queue = vec![root.to_string()];
        let mut read = 0;
        let mut complete = true;
        while let Some(map) = queue.pop() {
            read += 1;
            if read > MAX_SITEMAPS {
                complete = false;
                break;
            }
            let r = send(self.http, self.get(&map), "The website").await?;
            let (pages, maps) = parse_sitemap(&r.text());
            out.extend(
                pages
                    .into_iter()
                    .filter(|(u, _)| host_of(u) == host && check_url(u).is_ok()),
            );
            queue.extend(maps.into_iter().filter(|u| host_of(u) == host));
        }
        Ok((out, complete))
    }
}

#[async_trait::async_trait]
impl Connector for Web<'_> {
    fn kind(&self) -> Kind {
        Kind::Web
    }

    /// The typed address, as a page or (when it looks like one) a sitemap.
    async fn browse(
        &self,
        _parent: Option<&str>,
        query: &str,
    ) -> Result<Vec<Node>, ConnectorError> {
        let q = query.trim();
        if q.is_empty() {
            return Ok(Vec::new());
        }
        let url = if q.starts_with("https://") {
            q.to_string()
        } else if q.contains("://") {
            return Err(ConnectorError::Config(
                "Use the page's https:// address.".into(),
            ));
        } else {
            format!("https://{q}")
        };
        check_url(&url).map_err(|e| ConnectorError::Config(e.to_string()))?;
        let sitemap = url.to_ascii_lowercase().contains("sitemap")
            && url.to_ascii_lowercase().ends_with(".xml");
        Ok(vec![Node {
            id: format!("{}:{url}", if sitemap { "sitemap" } else { "url" }),
            title: url.clone(),
            kind: if sitemap { "sitemap" } else { "url" }.into(),
            has_children: false,
            selectable: true,
            url,
            detail: if sitemap {
                "Every page in this sitemap".into()
            } else {
                "This page".into()
            },
            ..Default::default()
        }])
    }

    async fn list(&self, source: &Source) -> Result<Listing, ConnectorError> {
        let mut items: Vec<Listed> = Vec::new();
        let mut complete = true;
        for s in &source.selection {
            let (kind, url) = split_id(&s.id).ok_or_else(|| {
                ConnectorError::Config(format!("\"{}\" isn't a public https address.", s.title))
            })?;
            if kind == "url" {
                items.push(Listed {
                    external_id: url.to_string(),
                    title: s.title.clone(),
                    url: url.to_string(),
                    ..Default::default()
                });
                continue;
            }
            let (pages, all) = self.sitemap_pages(url).await?;
            complete &= all;
            let host = host_of(url);
            for (page, lastmod) in pages {
                items.push(Listed {
                    title: page.clone(),
                    url: page.clone(),
                    updated_at: lastmod.clone(),
                    version: lastmod,
                    path: host.clone(),
                    external_id: page,
                    ..Default::default()
                });
            }
        }
        let truncated = items.len() > super::MAX_ITEMS_PER_SOURCE;
        items.truncate(super::MAX_ITEMS_PER_SOURCE);
        Ok(Listing {
            items,
            deleted: Vec::new(),
            complete: complete && !truncated,
            cursor: String::new(),
            truncated,
        })
    }

    async fn fetch(&self, _source: &Source, item: &Listed) -> Result<Document, ConnectorError> {
        check_url(&item.url)
            .map_err(|_| ConnectorError::NotFound("Not a public https address.".into()))?;
        let r = send(self.http, self.get(&item.url), "The website").await?;
        let ctype = r
            .header("content-type")
            .unwrap_or("text/html")
            .to_ascii_lowercase();
        let body = r.text();
        let (title, sections) = if ctype.contains("markdown") || ctype.starts_with("text/plain") {
            let t = item.title.clone();
            let s = text::markdown(&t, &body);
            (t, s)
        } else if ctype.contains("html") || ctype.contains("xml") {
            let t = text::html_title(&body).unwrap_or_else(|| item.title.clone());
            let s = text::html(&t, &body);
            (t, s)
        } else {
            return Err(ConnectorError::Config(format!(
                "{} isn't a web page.",
                item.url
            )));
        };
        Ok(Document {
            title,
            url: item.url.clone(),
            updated_at: r
                .header("last-modified")
                .and_then(|d| chrono::DateTime::parse_from_rfc2822(d).ok())
                .map(|d| d.with_timezone(&chrono::Utc).to_rfc3339())
                .unwrap_or_else(|| item.updated_at.clone()),
            sections,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knowledge::sources::Selected;
    use crate::mcp_remote::http::fake::{resp, Fake};

    #[test]
    fn sitemaps_and_indexes_parse() {
        let (pages, maps) = parse_sitemap(
            "<urlset><url><loc>https://docs.example.com/a?x=1&amp;y=2</loc><lastmod>2026-10-01</lastmod></url>\
             <url><loc> https://docs.example.com/b </loc></url></urlset>\
             <sitemapindex><sitemap><loc>https://docs.example.com/s2.xml</loc></sitemap></sitemapindex>",
        );
        assert_eq!(
            pages,
            [
                (
                    "https://docs.example.com/a?x=1&y=2".to_string(),
                    "2026-10-01".to_string()
                ),
                ("https://docs.example.com/b".to_string(), String::new())
            ]
        );
        assert_eq!(maps, ["https://docs.example.com/s2.xml"]);
    }

    #[tokio::test]
    async fn a_sitemap_keeps_its_own_host_and_follows_its_index() {
        let http = Fake::default();
        http.with(|r| {
            r.url.ends_with("/sitemap.xml").then(|| {
                resp(
                    200,
                    &[],
                    "<sitemapindex><sitemap><loc>https://docs.example.com/s2.xml</loc></sitemap>\
                    <sitemap><loc>https://evil.example.net/s.xml</loc></sitemap></sitemapindex>",
                )
            })
        });
        http.with(|r| {
            r.url.ends_with("/s2.xml").then(|| {
                resp(200, &[], "<urlset><url><loc>https://docs.example.com/a</loc><lastmod>2026-10-01</lastmod></url>\
                    <url><loc>https://other.example.org/x</loc></url></urlset>")
            })
        });
        let w = Web { http: &http };
        let src = Source {
            selection: vec![Selected {
                id: "sitemap:https://docs.example.com/sitemap.xml".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let l = w.list(&src).await.unwrap();
        assert_eq!(l.items.len(), 1);
        assert_eq!(l.items[0].external_id, "https://docs.example.com/a");
        assert_eq!(l.items[0].version, "2026-10-01");
        assert!(http.sent().iter().all(|r| !r.url.contains("evil")));
    }

    #[tokio::test]
    async fn a_page_reads_with_its_title() {
        let http = Fake::default();
        http.with(|r| {
            r.url.ends_with("/pricing").then(|| {
                resp(200, &[("content-type", "text/html; charset=utf-8")],
                    "<html><head><title>Pricing</title><script>x()</script></head><body><h1>Plans</h1><p>Free tier.</p></body></html>")
            })
        });
        let w = Web { http: &http };
        let d = w
            .fetch(
                &Source::default(),
                &Listed {
                    url: "https://example.com/pricing".into(),
                    title: "x".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(d.title, "Pricing");
        assert!(d.sections[0].text.contains("Free tier.") && !d.sections[0].text.contains("x()"));
    }

    #[tokio::test]
    async fn private_addresses_are_refused() {
        let w = Web {
            http: &Fake::default(),
        };
        assert!(w.browse(None, "http://localhost/x").await.is_err());
        assert!(w
            .browse(None, "https://169.254.169.254/latest")
            .await
            .is_err());
        assert_eq!(split_id("url:https://10.0.0.1/"), None);
        let ok = w
            .browse(None, "docs.example.com/sitemap.xml")
            .await
            .unwrap();
        assert_eq!(ok[0].kind, "sitemap");
    }
}
