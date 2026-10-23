//! Where CoderHelm posts messages, and how each chat channel renders them.
//!
//! Every sender (alert cards, the follow-up when a fix is proposed, review
//! reminders, release notes failures, the test message) builds one
//! channel-neutral [`Message`]: a banner, lines of text, facts, hidden details
//! and link buttons. [`teams`] renders it as an Adaptive Card for a Teams
//! Workflows or incoming webhook, [`slack`] as Block Kit for a Slack incoming
//! webhook. A destination ([`Channels`]) posts to Teams, Slack or both, and a
//! failure on one channel never stops the other ([`deliver`]).
//!
//! Webhook URLs post as the team, so they are secrets: [`store`] seals them
//! with KMS and API answers only ever show a hint.

pub mod messages;
pub mod slack;
pub mod store;
pub mod teams;

use serde_json::Value;
use tracing::warn;

/// A chat channel CoderHelm can post to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelKind {
    Teams,
    Slack,
}

impl ChannelKind {
    pub const ALL: [ChannelKind; 2] = [ChannelKind::Teams, ChannelKind::Slack];

    /// The key used in settings and API answers.
    pub fn key(self) -> &'static str {
        match self {
            ChannelKind::Teams => "teams",
            ChannelKind::Slack => "slack",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "teams" => Some(ChannelKind::Teams),
            "slack" => Some(ChannelKind::Slack),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            ChannelKind::Teams => "Microsoft Teams",
            ChannelKind::Slack => "Slack",
        }
    }

    /// Pure: a webhook URL this channel accepts. The server only ever posts to
    /// the channel's own webhook hosts.
    pub fn valid_url(self, url: &str) -> bool {
        match self {
            ChannelKind::Teams => teams::valid_url(url),
            ChannelKind::Slack => slack::valid_url(url),
        }
    }

    /// Pure: the message as this channel's JSON payload.
    pub fn render(self, m: &Message) -> Value {
        match self {
            ChannelKind::Teams => teams::render(m),
            ChannelKind::Slack => slack::render(m),
        }
    }
}

/// Where one destination posts: plain webhook URLs, opened from storage for
/// this send only. Never logged or returned.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Channels {
    pub teams: Option<String>,
    pub slack: Option<String>,
}

impl std::fmt::Debug for Channels {
    // Never prints a URL.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channels")
            .field("teams", &self.teams.is_some())
            .field("slack", &self.slack.is_some())
            .finish()
    }
}

impl Channels {
    pub fn is_empty(&self) -> bool {
        self.teams.is_none() && self.slack.is_none()
    }

    pub fn get(&self, kind: ChannelKind) -> Option<&str> {
        match kind {
            ChannelKind::Teams => self.teams.as_deref(),
            ChannelKind::Slack => self.slack.as_deref(),
        }
    }

    pub fn set(&mut self, kind: ChannelKind, url: Option<String>) {
        match kind {
            ChannelKind::Teams => self.teams = url,
            ChannelKind::Slack => self.slack = url,
        }
    }

    /// Only the URLs its channel accepts.
    pub fn usable(mut self) -> Self {
        for kind in ChannelKind::ALL {
            let ok = self.get(kind).map(|u| kind.valid_url(u)).unwrap_or(false);
            if !ok {
                self.set(kind, None);
            }
        }
        self
    }
}

// ─── The message model ──────────────────────────────────────────────────────

/// How a banner or line reads at a glance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    Neutral,
    Attention,
    Good,
    Warning,
    Accent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Size {
    #[default]
    Default,
    Small,
    Medium,
    Large,
}

/// Space above a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Spacing {
    #[default]
    Default,
    None,
    Small,
    Medium,
}

/// One line (or paragraph) of text. `**bold**` is the only markup.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Text {
    pub text: String,
    pub bold: bool,
    /// Secondary text: context, what CoderHelm did.
    pub subtle: bool,
    pub size: Size,
    pub color: Option<Tone>,
    pub spacing: Spacing,
}

impl Text {
    pub fn new(text: impl Into<String>) -> Self {
        Text {
            text: text.into(),
            ..Text::default()
        }
    }
    pub fn bold(mut self) -> Self {
        self.bold = true;
        self
    }
    pub fn subtle(mut self) -> Self {
        self.subtle = true;
        self
    }
    pub fn size(mut self, size: Size) -> Self {
        self.size = size;
        self
    }
    pub fn color(mut self, tone: Tone) -> Self {
        self.color = Some(tone);
        self
    }
    pub fn spacing(mut self, spacing: Spacing) -> Self {
        self.spacing = spacing;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Block {
    /// The colored title of a message.
    Banner {
        text: String,
        tone: Tone,
        size: Size,
    },
    Text(Text),
    /// Label and value pairs.
    Facts {
        facts: Vec<(String, String)>,
        spacing: Spacing,
    },
    /// Blocks kept together.
    Group {
        blocks: Vec<Block>,
        spacing: Spacing,
    },
    /// Facts behind a Details toggle where the channel has one (Teams); shown
    /// at the end where it doesn't (Slack).
    Details {
        facts: Vec<(String, String)>,
    },
}

/// A button that opens a link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Button {
    pub title: String,
    pub url: String,
}

impl Button {
    pub fn new(title: impl Into<String>, url: impl Into<String>) -> Self {
        Button {
            title: title.into(),
            url: url.into(),
        }
    }
}

/// One message, before a channel renders it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Message {
    /// One plain line: the notification preview where a channel shows one.
    pub summary: String,
    pub blocks: Vec<Block>,
    pub buttons: Vec<Button>,
    /// Teams: the card spans the conversation's full width.
    pub full_width: bool,
}

// ─── Delivery ───────────────────────────────────────────────────────────────

/// Posts a JSON payload to a webhook. Real HTTP in production; a fake in tests.
#[async_trait::async_trait]
pub trait Poster: Send + Sync {
    async fn post(&self, url: &str, body: &Value) -> Result<(), String>;
}

/// Posts over HTTP. Retries once when the channel throttles (429). Errors never
/// carry the URL: it is a secret.
pub struct Http<'a>(pub &'a reqwest::Client);

#[async_trait::async_trait]
impl Poster for Http<'_> {
    async fn post(&self, url: &str, body: &Value) -> Result<(), String> {
        for attempt in 0..2 {
            let resp = self
                .0
                .post(url)
                .timeout(std::time::Duration::from_secs(6))
                .json(body)
                .send()
                .await
                .map_err(|e| e.without_url().to_string())?;
            let status = resp.status();
            if status.is_success() {
                return Ok(());
            }
            if status.as_u16() == 429 && attempt == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
                continue;
            }
            let text = resp.text().await.unwrap_or_default();
            return Err(format!(
                "webhook returned {status}: {}",
                crate::truncate_str(&text, 300)
            ));
        }
        Err("webhook throttled".into())
    }
}

/// What happened on each channel of one send. `None` = not configured.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Delivery {
    pub teams: Option<Result<(), String>>,
    pub slack: Option<Result<(), String>>,
}

impl Delivery {
    pub fn get(&self, kind: ChannelKind) -> Option<&Result<(), String>> {
        match kind {
            ChannelKind::Teams => self.teams.as_ref(),
            ChannelKind::Slack => self.slack.as_ref(),
        }
    }

    /// At least one channel took the message.
    pub fn any_ok(&self) -> bool {
        ChannelKind::ALL
            .iter()
            .any(|k| matches!(self.get(*k), Some(Ok(()))))
    }
}

/// Post `msg` to every channel of `to`, at the same time. Each channel's
/// failure is logged and reported, and never stops the other channel or the
/// caller.
pub async fn deliver(poster: &dyn Poster, to: &Channels, msg: &Message, what: &str) -> Delivery {
    let send = |kind: ChannelKind| async move {
        let url = to.get(kind)?;
        let result = poster.post(url, &kind.render(msg)).await;
        if let Err(e) = &result {
            warn!(what, channel = kind.key(), error = %e, "Notification failed");
        }
        Some(result)
    };
    let (teams, slack) = tokio::join!(send(ChannelKind::Teams), send(ChannelKind::Slack));
    Delivery { teams, slack }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Records posts; fails every post to a URL containing `fail`.
    #[derive(Default)]
    struct Fake {
        posts: Mutex<Vec<(String, Value)>>,
        fail: String,
    }

    #[async_trait::async_trait]
    impl Poster for Fake {
        async fn post(&self, url: &str, body: &Value) -> Result<(), String> {
            self.posts
                .lock()
                .unwrap()
                .push((url.to_string(), body.clone()));
            if !self.fail.is_empty() && url.contains(&self.fail) {
                Err("webhook returned 500".into())
            } else {
                Ok(())
            }
        }
    }

    const TEAMS: &str = "https://acme.webhook.office.com/webhookb2/abc";
    const SLACK: &str = "https://hooks.slack.com/services/T000/B000/XXXX";

    fn msg() -> Message {
        Message {
            summary: "Review needed".into(),
            blocks: vec![Block::Banner {
                text: "⏳ Review needed".into(),
                tone: Tone::Warning,
                size: Size::Large,
            }],
            buttons: vec![Button::new("Open PR", "https://github.com/acme/web/pull/1")],
            full_width: true,
        }
    }

    #[tokio::test]
    async fn both_channels_get_their_own_payload() {
        let fake = Fake::default();
        let to = Channels {
            teams: Some(TEAMS.into()),
            slack: Some(SLACK.into()),
        };
        let d = deliver(&fake, &to, &msg(), "test").await;
        assert_eq!(d.teams, Some(Ok(())));
        assert_eq!(d.slack, Some(Ok(())));
        let posts = fake.posts.lock().unwrap();
        let teams = posts.iter().find(|(u, _)| u == TEAMS).unwrap();
        let slack = posts.iter().find(|(u, _)| u == SLACK).unwrap();
        assert_eq!(teams.1["type"], "message");
        assert!(slack.1["blocks"].is_array());
    }

    #[tokio::test]
    async fn a_failing_slack_post_does_not_stop_teams() {
        let fake = Fake {
            fail: "slack".into(),
            ..Fake::default()
        };
        let to = Channels {
            teams: Some(TEAMS.into()),
            slack: Some(SLACK.into()),
        };
        let d = deliver(&fake, &to, &msg(), "test").await;
        assert_eq!(d.teams, Some(Ok(())));
        assert!(matches!(d.slack, Some(Err(_))));
        assert!(d.any_ok());
        assert_eq!(fake.posts.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_failing_teams_post_does_not_stop_slack() {
        let fake = Fake {
            fail: "office".into(),
            ..Fake::default()
        };
        let to = Channels {
            teams: Some(TEAMS.into()),
            slack: Some(SLACK.into()),
        };
        let d = deliver(&fake, &to, &msg(), "test").await;
        assert!(matches!(d.teams, Some(Err(_))));
        assert_eq!(d.slack, Some(Ok(())));
    }

    #[tokio::test]
    async fn only_configured_channels_are_posted() {
        let fake = Fake::default();
        let to = Channels {
            teams: None,
            slack: Some(SLACK.into()),
        };
        let d = deliver(&fake, &to, &msg(), "test").await;
        assert_eq!(d.teams, None);
        assert_eq!(d.slack, Some(Ok(())));
        assert_eq!(fake.posts.lock().unwrap().len(), 1);
        let none = deliver(&fake, &Channels::default(), &msg(), "test").await;
        assert!(!none.any_ok());
    }

    #[test]
    fn usable_drops_urls_on_the_wrong_host() {
        let c = Channels {
            teams: Some(SLACK.into()),
            slack: Some(SLACK.into()),
        }
        .usable();
        assert_eq!(c.teams, None);
        assert_eq!(c.slack.as_deref(), Some(SLACK));
    }

    #[test]
    fn debug_never_prints_a_url() {
        let c = Channels {
            teams: Some(TEAMS.into()),
            slack: None,
        };
        let s = format!("{c:?}");
        assert!(!s.contains("office"), "{s}");
    }
}
