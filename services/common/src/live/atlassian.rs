//! Confluence with the Atlassian sign-in the team made for Knowledge.
//!
//! A team that signed in to Atlassian so Knowledge can sync its pages has
//! already granted everything agents need to read those pages during runs
//! and reviews (pages, spaces, search: `knowledge::sources::oauth::provider`).
//! An admin turns that on with one switch, with no second sign-in and no API
//! token: the team's `CONNECTOR#confluence` record then says
//! [`VIA_SIGN_IN`](super::connectors::VIA_SIGN_IN), and every read opens the
//! same sealed sign-in (renewed when it is about to expire) for that one
//! call. It never writes: the sign-in has no write scope.

use crate::knowledge::sources::confluence::{accessible_sites, oauth_base};
use crate::knowledge::sources::connector::ConnectorError;
use crate::knowledge::sources::oauth::connection_id;
use crate::knowledge::sources::Kind;
use crate::mcp_remote::runtime::{ConnError, Runtime};
use crate::mcp_remote::store::Owner;

use super::StepError;

/// The team's Atlassian sign-in, opened for Confluence.
#[derive(Clone, PartialEq, Eq)]
pub struct SignedIn {
    /// Where the REST API is: `https://api.atlassian.com/ex/confluence/<cloud id>`.
    pub base: String,
    /// The site people open pages on: `https://x.atlassian.net`.
    pub site: String,
    /// The `Authorization` value (`Bearer ...`).
    pub bearer: String,
}

// Written by hand so the token never reaches a log.
impl std::fmt::Debug for SignedIn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignedIn")
            .field("site", &self.site)
            .finish_non_exhaustive()
    }
}

const WHERE: &str = "Sign in to Confluence again in Settings, Integrations, Confluence.";

fn gone(message: impl Into<String>) -> StepError {
    StepError::new("sign_in", message, WHERE)
}

/// Opens the team's Atlassian sign-in for the Confluence site `want` (the
/// connection's saved site; empty takes the first site the sign-in
/// reaches). An error says what to do: sign in again, or try later.
pub async fn confluence(rt: &Runtime, want: &str) -> Result<SignedIn, StepError> {
    let rec = rt
        .store
        .get_server(&rt.team, &connection_id(Kind::Confluence))
        .await
        .map_err(|e| StepError::new("reach", e, "Try again in a minute."))?
        .ok_or_else(|| gone("The team isn't signed in to Confluence any more."))?;
    let bearer = match rt.auth_header(&rec, &Owner::Team, false).await {
        Ok(Some(h)) => h.1,
        Ok(None) | Err(ConnError::NotConnected) => {
            return Err(gone("The team isn't signed in to Confluence any more."))
        }
        Err(ConnError::NeedsReconnect(why)) | Err(ConnError::Failed(why)) => {
            return Err(gone(if why.is_empty() {
                "The team's Atlassian sign-in has to be renewed.".to_string()
            } else {
                why
            }))
        }
        Err(e) => {
            return Err(StepError::new(
                "reach",
                format!("The Atlassian sign-in couldn't be opened: {e:?}"),
                "Try again in a minute.",
            ))
        }
    };
    let sites = accessible_sites(rt.http.as_ref(), &bearer)
        .await
        .map_err(|e| match e {
            ConnectorError::Reconnect(m) | ConnectorError::Forbidden(m) => gone(m),
            other => StepError::new(
                "reach",
                format!("Atlassian didn't say which sites the sign-in reaches: {other:?}"),
                "Try again in a minute.",
            ),
        })?;
    pick_site(sites, want)
        .map(|(cloud, site)| SignedIn {
            base: oauth_base(&cloud),
            site,
            bearer,
        })
        .ok_or_else(|| {
            StepError::new(
                "read",
                if want.trim().is_empty() {
                    "The Atlassian sign-in can't reach any Confluence site.".to_string()
                } else {
                    format!("The Atlassian sign-in can't reach {want} any more.")
                },
                WHERE,
            )
        })
}

/// The (cloud id, site address) to use: the wanted site when the sign-in
/// still reaches it, the first one when none is wanted. A wanted site the
/// sign-in doesn't reach is never swapped for another.
pub fn pick_site(sites: Vec<(String, String, String)>, want: &str) -> Option<(String, String)> {
    let want = want.trim().trim_end_matches('/');
    let ok = |id: &str| !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-');
    sites
        .into_iter()
        .filter(|(id, _, _)| ok(id))
        .find(|(_, url, _)| want.is_empty() || url.eq_ignore_ascii_case(want))
        .map(|(id, url, _)| (id, url))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::knowledge::sources::oauth::{connection_record, provider};
    use crate::mcp_remote::http::fake::{json as fjson, Fake};
    use crate::mcp_remote::runtime::testing::{sign_in, FakeKms};
    use crate::mcp_remote::store::fake::MemStore;
    use crate::mcp_remote::store::Status;
    use serde_json::json;
    use std::sync::Arc;

    fn rt(http: Arc<Fake>) -> Runtime {
        Runtime {
            store: Arc::new(MemStore::default()),
            cipher: Arc::new(FakeKms),
            http,
            team: "TEAM#a".into(),
            clock: Arc::new(|| 1_000_000),
        }
    }

    fn sites() -> Vec<(String, String, String)> {
        vec![
            (
                "cloud-1".into(),
                "https://one.atlassian.net".into(),
                "One".into(),
            ),
            (
                "cloud-2".into(),
                "https://two.atlassian.net".into(),
                "Two".into(),
            ),
        ]
    }

    #[test]
    fn the_saved_site_is_used_and_never_swapped() {
        assert_eq!(
            pick_site(sites(), ""),
            Some(("cloud-1".into(), "https://one.atlassian.net".into()))
        );
        assert_eq!(
            pick_site(sites(), "https://TWO.atlassian.net/"),
            Some(("cloud-2".into(), "https://two.atlassian.net".into()))
        );
        assert_eq!(pick_site(sites(), "https://three.atlassian.net"), None);
        // An id that isn't one is never put in an address.
        let bad = vec![(
            "a/b".to_string(),
            "https://x.atlassian.net".to_string(),
            String::new(),
        )];
        assert_eq!(pick_site(bad, ""), None);
    }

    #[tokio::test]
    async fn the_knowledge_sign_in_opens_confluence_for_runs() {
        let http = Arc::new(Fake::default());
        http.with(|r| {
            r.url
                .ends_with("/oauth/token/accessible-resources")
                .then(|| {
                    fjson(
                        200,
                        json!([{ "id": "cloud-1", "url": "https://one.atlassian.net", "name": "One",
                             "scopes": ["read:page:confluence"] }]),
                    )
                })
        });
        let rt = rt(http);
        // Nobody signed in: it says where to sign in.
        let e = confluence(&rt, "").await.unwrap_err();
        assert_eq!(e.step, "sign_in");
        assert!(
            e.fix.contains("Settings, Integrations, Confluence"),
            "{e:?}"
        );

        let p = provider(Kind::Confluence).unwrap();
        let rec = connection_record(Kind::Confluence, &p, "cid", "sealed");
        rt.store.put_server("TEAM#a", &rec).await.unwrap();
        sign_in(
            &rt,
            &rec.id,
            &Owner::Team,
            "at-1",
            None,
            None,
            "ada@example.com",
        )
        .await;
        let s = confluence(&rt, "").await.unwrap();
        assert_eq!(s.base, "https://api.atlassian.com/ex/confluence/cloud-1");
        assert_eq!(s.site, "https://one.atlassian.net");
        assert_eq!(s.bearer, "Bearer at-1");
        assert!(!format!("{s:?}").contains("at-1"));
        // A site the sign-in no longer reaches is an error, not another site.
        let e = confluence(&rt, "https://two.atlassian.net")
            .await
            .unwrap_err();
        assert!(e.message.contains("two.atlassian.net"), "{e:?}");

        // A grant that was revoked says to sign in again.
        rt.store
            .set_token_status(
                "TEAM#a",
                &rec.id,
                &Owner::Team,
                Status::NeedsReconnect,
                "Atlassian ended the sign-in.",
                &[],
            )
            .await
            .unwrap();
        let e = confluence(&rt, "").await.unwrap_err();
        assert_eq!(e.step, "sign_in");
    }
}
