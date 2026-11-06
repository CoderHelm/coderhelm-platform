//! Which pull requests CoderHelm may change: only ones it created itself.
//!
//! A run that finds its ticket covered by another open pull request may
//! continue in it, push to it, edit its description or close an earlier one
//! it made redundant. Each of those is allowed only on a pull request
//! CoderHelm opened: its app's bot is the author, the branch lives in the
//! repository itself (not a fork), and one of the team's runs recorded it.
//! A person's pull request is never pushed to, edited or closed, however
//! well it matches the ticket. The worker and the gateway both decide with
//! [`judge`], on the pull request as GitHub shows it at that moment.

use serde_json::Value;

/// A pull request, as GitHub shows it now.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrFacts {
    /// `owner/name` of the repository the pull request is in.
    pub repo: String,
    pub number: u64,
    /// `open` or `closed`.
    pub state: String,
    pub merged: bool,
    /// The login of who opened it.
    pub author: String,
    /// GitHub's account type of who opened it (`User`, `Bot`).
    pub author_type: String,
    /// The head branch.
    pub head: String,
    pub head_sha: String,
    /// `owner/name` of the repository the head branch lives in.
    pub head_repo: String,
    /// The base branch.
    pub base: String,
    pub url: String,
    pub title: String,
    pub body: String,
}

impl PrFacts {
    /// From GitHub's pull request object (`GET /repos/{o}/{r}/pulls/{n}`).
    pub fn from_github(v: &Value) -> Option<PrFacts> {
        let s = |p: &str| {
            v.pointer(p)
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };
        Some(PrFacts {
            repo: s("/base/repo/full_name"),
            number: v["number"].as_u64().filter(|n| *n > 0)?,
            state: s("/state"),
            merged: v["merged"].as_bool().unwrap_or(false) || !s("/merged_at").is_empty(),
            author: s("/user/login"),
            author_type: s("/user/type"),
            head: s("/head/ref"),
            head_sha: s("/head/sha"),
            head_repo: s("/head/repo/full_name"),
            base: s("/base/ref"),
            url: s("/html_url"),
            title: s("/title"),
            body: s("/body"),
        })
    }
}

/// Why CoderHelm may not change a pull request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotOwn {
    /// It merged or closed: there is nothing to continue in or close.
    NotOpen { merged: bool },
    /// A person (or another app) opened it.
    Person(String),
    /// Its branch is in a fork, not in the repository.
    Fork,
    /// None of the team's runs recorded it.
    NoRun,
    /// GitHub couldn't be read to be sure.
    Unread(String),
}

impl NotOwn {
    /// It merged or closed meanwhile: the run takes its normal path.
    pub fn gone(&self) -> bool {
        matches!(self, NotOwn::NotOpen { .. })
    }

    /// In plain words, for the run page and the logs.
    pub fn say(&self, number: u64) -> String {
        match self {
            NotOwn::NotOpen { merged: true } => format!("#{number} has merged"),
            NotOwn::NotOpen { merged: false } => format!("#{number} is closed"),
            NotOwn::Person(who) if who.is_empty() => {
                format!("#{number} is a person's pull request, not CoderHelm's")
            }
            NotOwn::Person(who) => format!("#{number} is {who}'s pull request, not CoderHelm's"),
            NotOwn::Fork => format!("#{number}'s branch is in a fork, not one CoderHelm made"),
            NotOwn::NoRun => format!("no CoderHelm run of this team opened #{number}"),
            NotOwn::Unread(why) => format!("#{number} couldn't be read to make sure ({why})"),
        }
    }
}

/// Whether `login` is a CoderHelm app's bot. `bot`: this app's own bot
/// login, when known; then nothing else counts.
pub fn is_coderhelm_bot(login: &str, account_type: &str, bot: Option<&str>) -> bool {
    match bot.map(str::trim).filter(|b| !b.is_empty()) {
        Some(b) => login.eq_ignore_ascii_case(b),
        None => {
            account_type.eq_ignore_ascii_case("Bot")
                && login.ends_with("[bot]")
                && login.to_ascii_lowercase().contains("coderhelm")
        }
    }
}

/// Whether CoderHelm may push to, edit or close the pull request: open,
/// opened by its bot (`bot`: the app's own login when known), on a branch in
/// the repository itself, and recorded by one of the team's runs
/// (`opened_by_run`).
pub fn judge(f: &PrFacts, bot: Option<&str>, opened_by_run: bool) -> Result<(), NotOwn> {
    if f.state != "open" || f.merged {
        return Err(NotOwn::NotOpen { merged: f.merged });
    }
    if !is_coderhelm_bot(&f.author, &f.author_type, bot) {
        return Err(NotOwn::Person(f.author.clone()));
    }
    if f.head.is_empty() || f.head_repo.is_empty() || !f.head_repo.eq_ignore_ascii_case(&f.repo) {
        return Err(NotOwn::Fork);
    }
    if !opened_by_run {
        return Err(NotOwn::NoRun);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pr(login: &str, kind: &str, head_repo: &str) -> Value {
        json!({
            "number": 52,
            "state": "open",
            "merged": false,
            "title": "Ship the change",
            "body": "Source ticket: ABC-2",
            "html_url": "https://github.com/acme/web/pull/52",
            "user": {"login": login, "type": kind},
            "head": {"ref": "coderhelm/abc-2", "sha": "h2", "repo": {"full_name": head_repo}},
            "base": {"ref": "main", "repo": {"full_name": "acme/web"}}
        })
    }

    fn facts(v: &Value) -> PrFacts {
        PrFacts::from_github(v).unwrap()
    }

    #[test]
    fn coderhelms_own_open_pull_request_may_be_changed() {
        let f = facts(&pr("coderhelm[bot]", "Bot", "acme/web"));
        assert_eq!(f.repo, "acme/web");
        assert_eq!(f.head, "coderhelm/abc-2");
        assert_eq!(judge(&f, Some("coderhelm[bot]"), true), Ok(()));
        // Without the app's login, a CoderHelm bot account still counts.
        assert_eq!(judge(&f, None, true), Ok(()));
    }

    #[test]
    fn a_persons_pull_request_is_never_changed() {
        let f = facts(&pr("octocat", "User", "acme/web"));
        assert_eq!(
            judge(&f, Some("coderhelm[bot]"), true),
            Err(NotOwn::Person("octocat".into()))
        );
        assert_eq!(judge(&f, None, true), Err(NotOwn::Person("octocat".into())));
        // A person whose login has the word in it is still a person.
        let f = facts(&pr("coderhelm-fan", "User", "acme/web"));
        assert!(matches!(judge(&f, None, true), Err(NotOwn::Person(_))));
        // Another app's bot, or another CoderHelm app when this one's login
        // is known.
        let f = facts(&pr("release-drafter[bot]", "Bot", "acme/web"));
        assert!(matches!(judge(&f, None, true), Err(NotOwn::Person(_))));
        let f = facts(&pr("other-coderhelm[bot]", "Bot", "acme/web"));
        assert!(matches!(
            judge(&f, Some("coderhelm[bot]"), true),
            Err(NotOwn::Person(_))
        ));
    }

    #[test]
    fn a_fork_or_an_unrecorded_pull_request_is_not_coderhelms() {
        let f = facts(&pr("coderhelm[bot]", "Bot", "someone/web"));
        assert_eq!(judge(&f, Some("coderhelm[bot]"), true), Err(NotOwn::Fork));
        let f = facts(&pr("coderhelm[bot]", "Bot", "acme/web"));
        assert_eq!(judge(&f, Some("coderhelm[bot]"), false), Err(NotOwn::NoRun));
    }

    #[test]
    fn a_merged_or_closed_pull_request_is_gone() {
        let mut v = pr("coderhelm[bot]", "Bot", "acme/web");
        v["state"] = json!("closed");
        let e = judge(&facts(&v), Some("coderhelm[bot]"), true).unwrap_err();
        assert!(e.gone());
        assert_eq!(e.say(52), "#52 is closed");
        v["merged"] = json!(true);
        let e = judge(&facts(&v), Some("coderhelm[bot]"), true).unwrap_err();
        assert_eq!(e, NotOwn::NotOpen { merged: true });
        assert_eq!(e.say(52), "#52 has merged");
        // A person's pull request that closed is gone too: the normal path.
        let mut v = pr("octocat", "User", "acme/web");
        v["state"] = json!("closed");
        assert!(judge(&facts(&v), None, true).unwrap_err().gone());
    }
}
