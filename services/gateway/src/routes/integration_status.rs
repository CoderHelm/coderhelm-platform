//! One status per product on the Integrations page.
//!
//! A team can reach the same product in several ways, each kept in its own
//! record: a connection with a key (`CONNECTOR#<id>`), the team's Jira app,
//! a sign-in made for Knowledge (`MCPSERVER#ks-<id>`), the vendor's remote
//! server (`MCPSERVER#<id>`), a sign-in with an app (`MCPSERVER#app-<id>`)
//! and a key for the vendor's package (`PLUGIN#<id>`). The page used to
//! read one of them per card, so a team signed in to Confluence for
//! Knowledge saw "Not connected" on Confluence's card.
//!
//! Here every way the team has is a [`Path`], and [`product_state`] works
//! out the card from all of them:
//!
//! - **What it is used for.** Each use the product has ([`Use`]: runs and
//!   reviews, Knowledge) is decided by the first path, in the order given,
//!   that isn't "not connected": working, or needing attention.
//! - **A use nothing serves yet** is `ready` when a working path can serve
//!   it with no second sign-in ([`Extend`]: a switch, or nothing at all),
//!   and `needs` when a different grant really is required ([`Need`] says
//!   exactly what, with one action).
//! - **The card** is "Connected" when any use works, and says which. It
//!   needs attention when a use that was set up is broken. It is "Not
//!   connected" only when no path works at all, so a product the team has
//!   a working sign-in to never reads "Not connected".

use serde_json::{json, Value};

use super::integrations::CardState;

/// What a team uses a product for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Use {
    /// Agents read it (or use its tools) during runs, reviews and chat.
    Runs,
    /// Its content is synced into Knowledge.
    Knowledge,
}

impl Use {
    pub fn key(self) -> &'static str {
        match self {
            Use::Runs => "runs",
            Use::Knowledge => "knowledge",
        }
    }

    /// The name a card shows.
    pub fn label(self) -> &'static str {
        match self {
            Use::Runs => "Runs and reviews",
            Use::Knowledge => "Knowledge",
        }
    }

    /// The name inside a sentence.
    fn words(self) -> &'static str {
        match self {
            Use::Runs => "runs and reviews",
            Use::Knowledge => "Knowledge",
        }
    }
}

/// The one thing a person does next: `kind` is what the page does
/// (`use_in_runs`, `connect`, `reconnect`, `check_again`, `add_credentials`,
/// `link`), `label` the words on the control, `href` where a `link` goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Action {
    pub kind: &'static str,
    pub label: String,
    pub href: String,
}

impl Action {
    pub fn new(kind: &'static str, label: &str) -> Self {
        Action {
            kind,
            label: label.to_string(),
            href: String::new(),
        }
    }

    pub fn link(label: &str, href: &str) -> Self {
        Action {
            kind: "link",
            label: label.to_string(),
            href: href.to_string(),
        }
    }

    fn json(&self) -> Value {
        json!({ "kind": self.kind, "label": self.label, "href": self.href })
    }
}

/// A use a working path can also serve, with no second sign-in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extend {
    pub to: Use,
    /// What it takes, in a sentence ("The same sign-in can read pages
    /// during runs and reviews. Turn it on: no second sign-in.").
    pub detail: String,
    pub action: Option<Action>,
}

/// One way the team reaches a product.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Path {
    /// What it is, in a card's words: "an API token", "the Jira app",
    /// "the team's Atlassian sign-in", "Coderhelm's app".
    pub how: String,
    /// The path's own state, from its own record.
    pub state: CardState,
    /// What it is used for now.
    pub serves: Vec<Use>,
    pub extends: Vec<Extend>,
    /// Who connected it and when, when the record says.
    pub by: String,
    pub at: String,
}

impl Path {
    pub fn new(how: &str, state: CardState, serves: &[Use]) -> Self {
        Path {
            how: how.to_string(),
            state,
            serves: serves.to_vec(),
            extends: Vec::new(),
            by: String::new(),
            at: String::new(),
        }
    }

    pub fn by(mut self, by: &str, at: &str) -> Self {
        self.by = by.to_string();
        self.at = at.to_string();
        self
    }

    pub fn extend(mut self, to: Use, detail: &str, action: Option<Action>) -> Self {
        self.extends.push(Extend {
            to,
            detail: detail.to_string(),
            action,
        });
        self
    }

    fn works(&self) -> bool {
        self.state.status == "connected"
    }

    fn set_up(&self) -> bool {
        self.state.status != "not_connected"
    }
}

/// What a use needs when no working path can serve it: a different grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Need {
    pub for_use: Use,
    /// "Reading pages during runs needs an API token or the Jira app."
    pub detail: String,
    pub action: Option<Action>,
}

/// How one use stands. `state` is `on` (works), `attention` (set up, but
/// broken), `ready` (a working sign-in can serve it), `needs` (a different
/// grant is required) or `off`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UseState {
    pub of: Use,
    pub state: &'static str,
    pub how: String,
    pub detail: String,
    pub action: Option<Action>,
}

impl UseState {
    fn json(&self) -> Value {
        json!({
            "id": self.of.key(),
            "label": self.of.label(),
            "state": self.state,
            "how": self.how,
            "detail": self.detail,
            "action": self.action.as_ref().map(Action::json),
        })
    }
}

/// A product's card, worked out from every way the team reaches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductState {
    pub card: CardState,
    pub uses: Vec<UseState>,
    /// The way that works (the first one), who made it and when.
    pub how: String,
    pub connected_by: String,
    pub connected_at: String,
}

impl ProductState {
    /// The fields a card carries besides its status, reason and action.
    pub fn fields(&self) -> Value {
        json!({
            "uses": self.uses.iter().map(UseState::json).collect::<Vec<_>>(),
            "how": self.how,
            "connected_by": self.connected_by,
            "connected_at": self.connected_at,
        })
    }
}

fn action_of(st: &CardState) -> Option<Action> {
    let label = match st.action {
        "reconnect" => "Reconnect",
        "check_again" => "Check again",
        "add_credentials" => "Add credentials",
        _ => return None,
    };
    Some(Action::new(st.action, label))
}

fn sentence(s: &str) -> String {
    let s = s.trim();
    if s.is_empty() || s.ends_with(['.', '!', '?']) {
        s.to_string()
    } else {
        format!("{s}.")
    }
}

fn upper_first(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

fn join(parts: &[String]) -> String {
    parts
        .iter()
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// The card for a product with `uses`, reached by `paths` (most preferred
/// first: the one agents or a sync take when several work).
pub fn product_state(uses: &[Use], paths: &[Path], needs: &[Need]) -> ProductState {
    let any_works = paths.iter().any(Path::works);
    // Each use is decided by the first path serving it that is set up.
    let decided: Vec<(UseState, Option<&Path>)> = uses
        .iter()
        .map(|&u| {
            let decider = paths.iter().find(|p| p.set_up() && p.serves.contains(&u));
            match decider {
                Some(p) if p.works() => (
                    UseState {
                        of: u,
                        state: "on",
                        how: p.how.clone(),
                        detail: p.state.reason.clone(),
                        action: None,
                    },
                    Some(p),
                ),
                Some(p) => (
                    UseState {
                        of: u,
                        state: "attention",
                        how: p.how.clone(),
                        detail: p.state.reason.clone(),
                        action: action_of(&p.state),
                    },
                    Some(p),
                ),
                None => {
                    let ready = paths.iter().filter(|p| p.works()).find_map(|p| {
                        p.extends.iter().find(|e| e.to == u).map(|e| UseState {
                            of: u,
                            state: "ready",
                            how: p.how.clone(),
                            detail: e.detail.clone(),
                            action: e.action.clone(),
                        })
                    });
                    let need = || {
                        needs
                            .iter()
                            .find(|n| n.for_use == u)
                            .filter(|_| any_works)
                            .map(|n| UseState {
                                of: u,
                                state: "needs",
                                how: String::new(),
                                detail: n.detail.clone(),
                                action: n.action.clone(),
                            })
                    };
                    (
                        ready.or_else(need).unwrap_or(UseState {
                            of: u,
                            state: "off",
                            how: String::new(),
                            detail: String::new(),
                            action: None,
                        }),
                        None,
                    )
                }
            }
        })
        .collect();

    let on: Vec<&(UseState, Option<&Path>)> =
        decided.iter().filter(|(s, _)| s.state == "on").collect();
    let broken = decided.iter().find(|(s, _)| s.state == "attention");
    let first = on.first().and_then(|(_, p)| *p);
    let (how, connected_by, connected_at) = match first {
        Some(p) => (p.how.clone(), p.by.clone(), p.at.clone()),
        None => Default::default(),
    };

    let card = match (on.is_empty(), broken) {
        // Nothing works and something set up is broken: that path's state.
        (true, Some((_, Some(p)))) => p.state.clone(),
        // Nothing is set up: the first path's own words, if it has any
        // ("Each person connects their own account. You haven't yet.").
        (true, _) => CardState {
            status: "not_connected",
            reason: paths
                .iter()
                .map(|p| p.state.reason.as_str())
                .find(|r| !r.is_empty())
                .unwrap_or("")
                .to_string(),
            action: "connect",
        },
        // One use works and another that was set up is broken: the card
        // needs attention and says which part still works.
        (false, Some((b, Some(p)))) => CardState {
            status: p.state.status,
            reason: join(&[
                sentence(&format!(
                    "{} still work{}",
                    upper_first(&list(
                        &on.iter().map(|(s, _)| s.of.words()).collect::<Vec<_>>()
                    )),
                    if on.len() == 1 && on[0].0.of == Use::Knowledge {
                        "s"
                    } else {
                        ""
                    },
                )),
                format!(
                    "{}: {}",
                    b.of.label(),
                    sentence(&nonempty(&b.detail, "it has to be connected again"))
                ),
            ]),
            action: p.state.action,
        },
        (false, _) => CardState {
            status: "connected",
            reason: connected_reason(uses, &decided),
            action: "manage",
        },
    };

    ProductState {
        card,
        uses: decided.into_iter().map(|(s, _)| s).collect(),
        how,
        connected_by,
        connected_at,
    }
}

fn nonempty(s: &str, fallback: &str) -> String {
    if s.trim().is_empty() {
        fallback.to_string()
    } else {
        s.trim().to_string()
    }
}

fn list(words: &[&str]) -> String {
    match words {
        [] => String::new(),
        [one] => one.to_string(),
        [a, b] => format!("{a} and {b}"),
        many => format!(
            "{}, and {}",
            many[..many.len() - 1].join(", "),
            many[many.len() - 1]
        ),
    }
}

/// What a connected card says. A product with one use keeps its path's own
/// words ("Signed in with Coderhelm's app."). One with several says what it
/// is used for, then what each other use takes.
fn connected_reason(uses: &[Use], decided: &[(UseState, Option<&Path>)]) -> String {
    let on: Vec<&UseState> = decided
        .iter()
        .map(|(s, _)| s)
        .filter(|s| s.state == "on")
        .collect();
    let notes: Vec<String> = {
        let mut seen: Vec<String> = Vec::new();
        for s in &on {
            let d = sentence(&s.detail);
            if !d.is_empty() && !seen.contains(&d) {
                seen.push(d);
            }
        }
        seen
    };
    if uses.len() < 2 {
        return join(&notes);
    }
    let lead = if on.len() == uses.len() {
        // Every use works: "Used in runs and reviews, and in Knowledge."
        let parts: Vec<String> = on.iter().map(|s| format!("in {}", s.of.words())).collect();
        format!("Used {}.", parts.join(", and "))
    } else {
        format!(
            "Connected for {}.",
            list(&on.iter().map(|s| s.of.words()).collect::<Vec<_>>())
        )
    };
    let rest: Vec<String> = decided
        .iter()
        .map(|(s, _)| s)
        .filter(|s| matches!(s.state, "ready" | "needs"))
        .map(|s| sentence(&s.detail))
        .collect();
    join(&[vec![lead], notes, rest].concat())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(status: &'static str, reason: &str, action: &'static str) -> CardState {
        CardState {
            status,
            reason: reason.to_string(),
            action,
        }
    }

    fn ok(reason: &str) -> CardState {
        st("connected", reason, "manage")
    }

    fn off() -> CardState {
        st("not_connected", "", "connect")
    }

    const BOTH: &[Use] = &[Use::Runs, Use::Knowledge];
    const RUNS: &[Use] = &[Use::Runs];

    /// The knowledge sign-in of a product whose runs it can also serve
    /// with a switch (Confluence).
    fn knowledge_sign_in(state: CardState) -> Path {
        Path::new("the team's Atlassian sign-in", state, &[Use::Knowledge])
            .by("ada@example.com", "2026-11-11T10:00:00Z")
            .extend(
                Use::Runs,
                "The same sign-in can read pages during runs and reviews. Turn it on: no second sign-in.",
                Some(Action::new("use_in_runs", "Use in runs too")),
            )
    }

    fn token(state: CardState) -> Path {
        Path::new("an API token", state, &[Use::Runs]).extend(
            Use::Knowledge,
            "Knowledge can sync with the same token: add a source there.",
            Some(Action::link("Add a source in Knowledge", "/knowledge/")),
        )
    }

    fn knowledge_needs() -> Vec<Need> {
        vec![Need {
            for_use: Use::Knowledge,
            detail: "Syncing pages into Knowledge needs an Atlassian sign-in or an API token: the Jira app only serves runs.".into(),
            action: Some(Action::link("Connect in Knowledge", "/knowledge/")),
        }]
    }

    fn use_of(p: &ProductState, u: Use) -> &UseState {
        p.uses.iter().find(|s| s.of == u).unwrap()
    }

    #[test]
    fn nothing_connected_is_not_connected() {
        let p = product_state(BOTH, &[], &knowledge_needs());
        assert_eq!(p.card, off());
        assert!(p.uses.iter().all(|u| u.state == "off"));
        // Records that exist but were never connected change nothing.
        let p = product_state(BOTH, &[token(off()), knowledge_sign_in(off())], &[]);
        assert_eq!(p.card.status, "not_connected");
        assert_eq!(p.card.action, "connect");
        assert_eq!((p.how.as_str(), p.connected_by.as_str()), ("", ""));
    }

    /// The reported case: signed in for Knowledge, no connection saved for
    /// runs. The card is Connected, says for what, and offers the switch.
    #[test]
    fn a_knowledge_sign_in_alone_is_connected_and_offers_runs_without_a_second_sign_in() {
        let p = product_state(BOTH, &[knowledge_sign_in(ok(""))], &knowledge_needs());
        assert_eq!(p.card.status, "connected");
        assert_eq!(p.card.action, "manage");
        assert_eq!(
            p.card.reason,
            "Connected for Knowledge. The same sign-in can read pages during runs and reviews. Turn it on: no second sign-in."
        );
        assert_eq!(use_of(&p, Use::Knowledge).state, "on");
        let runs = use_of(&p, Use::Runs);
        assert_eq!(runs.state, "ready");
        assert_eq!(runs.action.as_ref().unwrap().kind, "use_in_runs");
        assert_eq!(p.connected_by, "ada@example.com");
        assert_eq!(p.connected_at, "2026-11-11T10:00:00Z");
        assert_eq!(p.how, "the team's Atlassian sign-in");
    }

    #[test]
    fn a_token_alone_is_connected_for_runs_and_ready_for_knowledge() {
        let p = product_state(BOTH, &[token(ok(""))], &knowledge_needs());
        assert_eq!(p.card.status, "connected");
        assert_eq!(
            p.card.reason,
            "Connected for runs and reviews. Knowledge can sync with the same token: add a source there."
        );
        let k = use_of(&p, Use::Knowledge);
        assert_eq!(
            (k.state, k.action.as_ref().unwrap().kind),
            ("ready", "link")
        );
    }

    /// The Jira app can't sync into Knowledge: a different grant is needed,
    /// and the card says exactly which, with one action.
    #[test]
    fn a_path_that_cannot_serve_another_use_says_what_is_missing() {
        let app = Path::new(
            "the Jira app",
            ok("Connected through the Jira app."),
            &[Use::Runs],
        );
        let p = product_state(BOTH, &[app], &knowledge_needs());
        assert_eq!(p.card.status, "connected");
        assert_eq!(
            p.card.reason,
            "Connected for runs and reviews. Connected through the Jira app. Syncing pages into Knowledge needs an Atlassian sign-in or an API token: the Jira app only serves runs."
        );
        let k = use_of(&p, Use::Knowledge);
        assert_eq!(k.state, "needs");
        assert_eq!(k.action.as_ref().unwrap().href, "/knowledge/");
        // With nothing connected there is nothing "missing" to say yet.
        let p = product_state(BOTH, &[], &knowledge_needs());
        assert_eq!(use_of(&p, Use::Knowledge).state, "off");
    }

    #[test]
    fn both_uses_working_says_so() {
        let p = product_state(
            BOTH,
            &[token(ok("")), knowledge_sign_in(ok(""))],
            &knowledge_needs(),
        );
        assert_eq!(p.card.status, "connected");
        assert_eq!(p.card.reason, "Used in runs and reviews, and in Knowledge.");
        assert!(p.uses.iter().all(|u| u.state == "on"));
        // Who connected it is the first working path's record.
        assert_eq!(p.how, "an API token");
    }

    /// A use switched on with the knowledge sign-in is served by the same
    /// path as Knowledge.
    #[test]
    fn one_sign_in_serving_both_uses() {
        let both = Path::new(
            "the team's Atlassian sign-in",
            ok(""),
            &[Use::Runs, Use::Knowledge],
        );
        let p = product_state(BOTH, &[both], &[]);
        assert_eq!(p.card.reason, "Used in runs and reviews, and in Knowledge.");
    }

    #[test]
    fn a_broken_use_beside_a_working_one_needs_attention_but_is_never_not_connected() {
        let p = product_state(
            BOTH,
            &[
                token(ok("")),
                knowledge_sign_in(st(
                    "needs_reconnect",
                    "Atlassian ended the sign-in.",
                    "reconnect",
                )),
            ],
            &[],
        );
        assert_eq!(p.card.status, "needs_reconnect");
        assert_eq!(p.card.action, "reconnect");
        assert_eq!(
            p.card.reason,
            "Runs and reviews still work. Knowledge: Atlassian ended the sign-in."
        );
        assert_eq!(use_of(&p, Use::Runs).state, "on");
        let k = use_of(&p, Use::Knowledge);
        assert_eq!(
            (k.state, k.action.as_ref().unwrap().kind),
            ("attention", "reconnect")
        );
        // The other way round reads the same way.
        let p = product_state(
            BOTH,
            &[
                token(st(
                    "error",
                    "Confluence didn't accept the email and token.",
                    "check_again",
                )),
                knowledge_sign_in(ok("")),
            ],
            &[],
        );
        assert_eq!(p.card.status, "error");
        assert!(p
            .card
            .reason
            .starts_with("Knowledge still works. Runs and reviews: "));
    }

    #[test]
    fn everything_broken_is_the_broken_paths_own_state() {
        let broken = st(
            "needs_reconnect",
            "Atlassian ended the sign-in.",
            "reconnect",
        );
        let p = product_state(
            BOTH,
            &[knowledge_sign_in(broken.clone())],
            &knowledge_needs(),
        );
        assert_eq!(p.card, broken);
        // The other use can't be offered from a sign-in that doesn't work.
        assert_eq!(use_of(&p, Use::Runs).state, "off");
    }

    /// Several records for one use (Figma: an app sign-in, an older remote
    /// record, a key): one card, decided by the first that is set up.
    #[test]
    fn the_preferred_path_decides_a_use() {
        let app = |s| Path::new("Coderhelm's app", s, RUNS);
        let remote = |s| Path::new("Figma's own server", s, RUNS);
        let key = |s| Path::new("a key", s, RUNS);
        // An app sign-in that works wins over everything else.
        let p = product_state(
            RUNS,
            &[
                app(ok("Signed in with Coderhelm's app.")),
                remote(off()),
                key(ok("")),
            ],
            &[],
        );
        assert_eq!(p.card, ok("Signed in with Coderhelm's app."));
        assert_eq!(p.how, "Coderhelm's app");
        // A sign-in never finished doesn't hide a key that works.
        let p = product_state(RUNS, &[app(off()), remote(off()), key(ok(""))], &[]);
        assert_eq!(p.card, ok(""));
        assert_eq!(p.how, "a key");
        // A sign-in that needs renewing is what agents would use: it shows,
        // even with a key saved.
        let renew = st("needs_reconnect", "Figma refused it.", "reconnect");
        let p = product_state(RUNS, &[app(renew.clone()), key(ok(""))], &[]);
        assert_eq!(p.card, renew);
        // Switched on with no credentials, and a sign-in that works.
        let p = product_state(
            RUNS,
            &[
                remote(ok("")),
                key(st(
                    "needs_credentials",
                    "No key is saved.",
                    "add_credentials",
                )),
            ],
            &[],
        );
        assert_eq!(p.card.status, "connected");
        // Only records nobody finished: not connected, with the first words.
        let p = product_state(
            RUNS,
            &[
                app(st(
                    "not_connected",
                    "Your own app is saved. Connect it to sign in.",
                    "connect",
                )),
                remote(off()),
            ],
            &[],
        );
        assert_eq!(p.card.status, "not_connected");
        assert_eq!(
            p.card.reason,
            "Your own app is saved. Connect it to sign in."
        );
    }

    /// Every combination of the ways a two-use product is reached: the card
    /// is "not connected" exactly when nothing is set up, so never when a
    /// path works.
    #[test]
    fn a_working_sign_in_is_never_shown_as_not_connected() {
        let states = [
            off(),
            ok(""),
            st("needs_reconnect", "It has to be renewed.", "reconnect"),
            st("error", "The last test failed.", "check_again"),
            st("needs_credentials", "No key is saved.", "add_credentials"),
        ];
        let mut seen = 0;
        for a in &states {
            for b in &states {
                for c in &states {
                    let paths = [
                        token(a.clone()),
                        Path::new("the vendor's own server", b.clone(), RUNS),
                        knowledge_sign_in(c.clone()),
                    ];
                    let p = product_state(BOTH, &paths, &knowledge_needs());
                    let works = paths.iter().any(|x| x.state.status == "connected");
                    let set_up = paths.iter().any(|x| x.state.status != "not_connected");
                    assert_eq!(
                        p.card.status == "not_connected",
                        !set_up,
                        "{a:?} {b:?} {c:?} gave {:?}",
                        p.card
                    );
                    if works {
                        assert_ne!(p.card.status, "not_connected", "{a:?} {b:?} {c:?}");
                    }
                    if p.card.status == "connected" {
                        assert!(p.uses.iter().any(|u| u.state == "on"));
                        assert!(!p.card.reason.is_empty(), "says what it is used for");
                    }
                    // A use works exactly when the first path set up for it does.
                    let runs_first = [a, b].into_iter().find(|s| s.status != "not_connected");
                    assert_eq!(
                        use_of(&p, Use::Runs).state == "on",
                        runs_first.is_some_and(|s| s.status == "connected"),
                        "{a:?} {b:?} {c:?}"
                    );
                    assert_eq!(
                        use_of(&p, Use::Knowledge).state == "on",
                        c.status == "connected",
                        "{a:?} {b:?} {c:?}"
                    );
                    // A use is "ready" or "needs" only beside a working path.
                    if !works {
                        assert!(p.uses.iter().all(|u| !matches!(u.state, "ready" | "needs")));
                    }
                    seen += 1;
                }
            }
        }
        assert_eq!(seen, 125);
    }

    #[test]
    fn the_cards_extra_fields() {
        let p = product_state(BOTH, &[knowledge_sign_in(ok(""))], &[]);
        let v = p.fields();
        assert_eq!(v["connected_by"], "ada@example.com");
        assert_eq!(v["uses"][0]["id"], "runs");
        assert_eq!(v["uses"][0]["label"], "Runs and reviews");
        assert_eq!(v["uses"][0]["state"], "ready");
        assert_eq!(v["uses"][0]["action"]["kind"], "use_in_runs");
        assert_eq!(v["uses"][1]["state"], "on");
        assert!(v["uses"][1]["action"].is_null());
    }
}
