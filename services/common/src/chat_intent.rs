//! What a Chat question asks for, read before the model answers: an
//! orientation ("I'm new, what can you do for me"), whether Chat can do
//! something, what is waiting on the person, where something stands, how to
//! do something, a lookup, a judgement call, small talk, or a correction of
//! something Chat said. Also the role the person says they hold.
//!
//! The reading decides only what code would otherwise force on every
//! answer: a fixed answer shape is used only for the question it was made
//! for ([`Intent::fixed_shape`]), an uncited reply is kept only for small
//! talk ([`Intent::uncited_ok`]), and a correction or an orientation must
//! look something up this turn before it answers. The model is told the
//! reading as a note it checks against the question itself
//! ([`Reading::note`]): when the question asks something else, it answers
//! what the question asks.

use std::sync::LazyLock;

use regex::Regex;

/// What the person wants from the answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intent {
    /// New to Coderhelm, or asking what it can do for them.
    Orientation,
    /// Whether Chat or Coderhelm can do something.
    Capability,
    /// What is waiting on the person asking.
    NeedsMe,
    /// Where a piece of work stands.
    Status,
    /// How to do something.
    HowTo,
    /// A fact: what, where, why, who.
    Lookup,
    /// A verdict: is it right, should we, is it safe.
    Judgement,
    /// The person says something Chat said was wrong, or asks what it meant.
    Correction,
    /// A greeting, thanks or acknowledgement.
    SmallTalk,
}

impl Intent {
    /// In a few words, for the note and the logs.
    pub fn words(self) -> &'static str {
        match self {
            Intent::Orientation => "an orientation: what Coderhelm already does for their team and what it can do for them",
            Intent::Capability => "whether Chat or Coderhelm can do something",
            Intent::NeedsMe => "what is waiting on them",
            Intent::Status => "where a piece of work stands",
            Intent::HowTo => "how to do something",
            Intent::Lookup => "a specific fact",
            Intent::Judgement => "your judgement on something",
            Intent::Correction => "a correction: they question something you said",
            Intent::SmallTalk => "small talk",
        }
    }

    /// A short key, for logs and evals.
    pub fn key(self) -> &'static str {
        match self {
            Intent::Orientation => "orientation",
            Intent::Capability => "capability",
            Intent::NeedsMe => "needs_me",
            Intent::Status => "status",
            Intent::HowTo => "how_to",
            Intent::Lookup => "lookup",
            Intent::Judgement => "judgement",
            Intent::Correction => "correction",
            Intent::SmallTalk => "small_talk",
        }
    }

    /// A fixed answer shape (the "what's waiting on you" list) fits only
    /// the question it was made for.
    pub fn fixed_shape(self) -> bool {
        self == Intent::NeedsMe
    }

    /// An answer with no citation is kept as written only for small talk;
    /// every other answer rests on something read this turn.
    pub fn uncited_ok(self) -> bool {
        self == Intent::SmallTalk
    }

    /// How the answer is sized and laid out.
    pub fn shape(self) -> &'static str {
        match self {
            Intent::Orientation => "Call coderhelm_setup first. Open with one or two sentences on what Coderhelm already does for this team, from what it found (cited). Then three to five things Coderhelm can do for someone in their role, each in a line with one example question they could ask. Don't list their to-do items or one ticket's details unless they asked; if things are waiting on them, say so in one line and that \"What needs my answer?\" lists them. Keep it under about 250 words.",
            Intent::Capability => "Say yes or no first: whether Chat or Coderhelm can do it, from coderhelm_setup or the tools you have this turn, cited. Then, in a few lines, what you can do instead or how to get it done. If it's something you can do right now with your tools, do it.",
            Intent::NeedsMe => "Answer in the shape work_needs_me's result gives.",
            Intent::Status => "Lead with where it stands and when that last changed, then only the detail that explains it.",
            Intent::HowTo => "Give the shortest way to do it first, as numbered steps, from the team's own docs or code (cited), then any exceptions.",
            Intent::Lookup => "Lead with the answer in a sentence or two, then only the detail that supports it.",
            Intent::Judgement => "Give your verdict first (yes, no, or what it depends on), then the evidence you checked this turn, then what would settle what you couldn't check.",
            Intent::Correction => "Check the disputed point again now with a tool, never from the earlier answer's words. Say in the first sentence whether you were wrong and what is right, plainly, then answer what they asked. Don't guess at causes you can't see.",
            Intent::SmallTalk => "Reply in a sentence or two, with no lookups. If it fits, offer one thing you can do.",
        }
    }
}

/// What the question names, from the gateway's reading of it
/// (`chat_hints`): it asks what is waiting on the person, where something
/// stands, or names a piece of work (a ticket, a pull request, a title).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Signals {
    pub needs_me: bool,
    pub status: bool,
    pub names_work: bool,
}

/// How a question was read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reading {
    pub intent: Intent,
    /// The role the person says they hold ("cto", "product manager").
    pub role: Option<String>,
    /// They say they are new to Coderhelm.
    pub newcomer: bool,
    /// The sentences of Chat's last answer the person questions.
    pub disputed: Vec<String>,
    /// It asks how the app fits together: how something works, what
    /// happens when, what calls what, what is behind a screen, which
    /// service, where a request goes. The app map answers it first.
    pub about_app: bool,
}

fn has(text: &str, words: &[&str]) -> bool {
    words.iter().any(|w| has_phrase(text, w))
}

/// `w` in `text` at word boundaries.
fn has_phrase(text: &str, w: &str) -> bool {
    text.match_indices(w).any(|(i, _)| {
        let before = text[..i].chars().next_back();
        let after = text[i + w.len()..].chars().next();
        !before.is_some_and(char::is_alphanumeric) && !after.is_some_and(char::is_alphanumeric)
    })
}

/// Lower case, curly quotes made straight.
fn plain(text: &str) -> String {
    text.trim()
        .to_lowercase()
        .replace(['\u{2019}', '\u{2018}'], "'")
        .replace(['\u{201c}', '\u{201d}'], "\"")
}

const NEWCOMER: &[&str] = &[
    "first time",
    "i'm new",
    "im new",
    "i am new",
    "new here",
    "new to coderhelm",
    "new to this",
    "just joined",
    "just started using",
    "getting started",
    "get started",
    "where do i start",
    "where should i start",
    "onboard me",
    "show me around",
];

/// Asks what Coderhelm or Chat offers them, in general.
const OFFER: &[&str] = &[
    "what can you do",
    "what can coderhelm do",
    "what does coderhelm do",
    "what do you do",
    "how can you help",
    "how could you help",
    "how can coderhelm help",
    "how does coderhelm help",
    "what can you help",
    "what can i use you for",
    "what can i use coderhelm for",
    "what should i use you for",
    "what should i use coderhelm for",
    "how should i use you",
    "how should i use coderhelm",
    "how do i use coderhelm",
    "useful to me",
    "useful for me",
    "could be useful",
    "would be useful",
    "be helpful to me",
    "what is coderhelm",
    "what's coderhelm",
    "who are you",
    "what are you",
    "what should i know",
    "what are you good at",
    "what can you tell me",
];

const CAPABILITY: &[&str] = &[
    "are you able",
    "is coderhelm able",
    "are you allowed",
    "is it possible for you",
    "can you run",
    "can you see",
    "can you read",
    "can you access",
    "can you open",
    "can you connect",
    "can you use",
    "can you change",
    "can you edit",
    "can you write",
    "can you deploy",
    "can you merge",
    "can coderhelm",
    "could coderhelm",
    "does coderhelm",
    "do you support",
    "does it support",
    "do you have access",
    "have access to",
    "do you know about",
];

const JUDGEMENT: &[&str] = &[
    "accurate",
    "is this right",
    "is that right",
    "is it right",
    "is this correct",
    "is that correct",
    "is it correct",
    "is this true",
    "is that true",
    "should we",
    "should i",
    "do you think",
    "would you",
    "is it safe",
    "is this safe",
    "safe to",
    "worth",
    "is it ok",
    "is it okay",
    "recommend",
    "better to",
    "which is better",
    "pros and cons",
    "tradeoff",
    "trade-off",
    "risky",
    "make sense",
    "makes sense",
    "good idea",
    "bad idea",
];

const HOW_TO: &[&str] = &[
    "how do i",
    "how can i",
    "how do we",
    "how can we",
    "how would i",
    "how should i",
    "how should we",
    "how to",
    "steps to",
    "what's the process",
    "what is the process",
    "walk me through",
    "how does one",
    "how can a",
    "how can an",
    "how can someone",
    "how can they",
    "how can people",
    "how do they",
    "how do people",
    "how does someone",
];

/// The person says Chat got something wrong, or asks what it meant.
const CORRECTION: &[&str] = &[
    "what do you mean",
    "what did you mean",
    "what does that mean",
    "that's wrong",
    "thats wrong",
    "that is wrong",
    "you're wrong",
    "you are wrong",
    "that's not right",
    "that is not right",
    "that isn't right",
    "that's not true",
    "that isn't true",
    "not true",
    "that's incorrect",
    "incorrect",
    "that's false",
    "you said",
    "you told me",
    "i see",
    "i can see",
    "but it is",
    "but it's",
    "it is connected",
    "it's connected",
    "is connected",
    "are you sure",
    "not what i asked",
    "i didn't ask",
    "i did not ask",
    "i asked",
    "that's not what",
    "is wrong",
    "was wrong",
    "got it wrong",
    "got that wrong",
    "why did you say",
    "why do you say",
];

/// How the app fits together: the app map's questions.
const ABOUT_APP: &[&str] = &[
    "what happens when",
    "what happens if",
    "what happens after",
    "what calls",
    "who calls",
    "what uses",
    "what's behind",
    "what is behind",
    "behind the",
    "which service",
    "what service",
    "which part",
    "fit together",
    "fits together",
    "talks to",
    "connected to",
    "connects to",
    "flow of",
    "end to end",
];

/// Words that end "how does X work": how something works, gets somewhere
/// or is handled.
const WORKS: &[&str] = &[
    "work", "works", "get", "gets", "go", "goes", "flow", "flows", "handled", "wired",
];

/// "How does X work", "how do refunds get to Stripe", "where does the
/// booking request go": "how" or "where", then does, do, is or are, then a
/// working word within a few words.
fn how_it_works(q: &str) -> bool {
    let words: Vec<&str> = q
        .split(|c: char| !c.is_alphanumeric() && c != '\'')
        .filter(|w| !w.is_empty())
        .collect();
    words.windows(2).enumerate().any(|(i, w)| {
        matches!(w[0], "how" | "where")
            && matches!(w[1], "does" | "do" | "is" | "are")
            && !matches!(words.get(i + 2).copied(), Some("i" | "we" | "you" | "one"))
            && words.iter().skip(i + 3).take(8).any(|x| WORKS.contains(x))
    })
}

/// The question asks how the app fits together.
pub fn about_app(question: &str) -> bool {
    let q = plain(question);
    has(&q, ABOUT_APP) || how_it_works(&q)
}

/// Words of a greeting, a thanks or an acknowledgement.
const SMALL: &[&str] = &[
    "hi",
    "hello",
    "hey",
    "there",
    "yo",
    "thanks",
    "thank",
    "you",
    "so",
    "much",
    "thx",
    "ty",
    "ok",
    "okay",
    "k",
    "cool",
    "great",
    "got",
    "it",
    "nice",
    "perfect",
    "good",
    "morning",
    "afternoon",
    "evening",
    "bye",
    "awesome",
    "sounds",
    "appreciate",
    "appreciated",
    "cheers",
    "how",
    "are",
    "doing",
    "chat",
    "coderhelm",
    "lol",
    "sure",
    "yes",
    "no",
    "yep",
    "nope",
    "that",
    "helps",
    "helped",
    "makes",
    "sense",
    "understood",
    "love",
    "this",
    "a",
    "lot",
    "all",
    "again",
    "will",
    "do",
];

static ROLE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?:^|[\s,.;(])(?:i'?m|i am|as)\s+(?:the|a|an|our)\s+([a-z][a-z/&-]*(?:\s+[a-z][a-z/&-]*){0,3}?)(?:\s+(?:of|at|for|on|in|here|and|who|that)\b|\s*[,.!?;:)]|\s*$)",
    )
    .unwrap()
});

/// Words that never name a role ("I'm the one who", "as a result").
const NOT_ROLES: &[&str] = &[
    "one", "result", "reminder", "test", "matter", "way", "first", "only", "same", "person",
    "user", "new", "little", "bit",
];

/// The role the person says they hold, as they wrote it.
pub fn role_of(question: &str) -> Option<String> {
    let q = plain(question);
    for c in ROLE.captures_iter(&q) {
        let r = c[1].trim().to_string();
        let head = r.split_whitespace().next().unwrap_or("");
        if r.is_empty() || NOT_ROLES.contains(&head) || r.split_whitespace().count() > 4 {
            continue;
        }
        return Some(r);
    }
    None
}

/// A greeting, a thanks or an acknowledgement, and nothing else.
pub fn is_small_talk(question: &str) -> bool {
    let q = plain(question);
    let words: Vec<&str> = q
        .split(|c: char| !c.is_alphanumeric() && c != '\'')
        .map(|w| w.trim_matches('\''))
        .filter(|w| !w.is_empty())
        .collect();
    if words.is_empty() {
        // Only emoji or punctuation ("👍", "?").
        return !q.contains('?') || q.chars().count() <= 3;
    }
    words.len() <= 8
        && words
            .iter()
            .all(|w| SMALL.contains(w) || SMALL.contains(&w.trim_end_matches("'s")))
}

/// Content words of a sentence: lower case, four letters or more.
fn content_words(text: &str) -> Vec<String> {
    const COMMON: &[&str] = &[
        "that", "this", "what", "your", "with", "from", "have", "there", "their", "about", "would",
        "could", "should", "which", "when", "where", "into", "they", "them", "were", "been",
        "does", "mean", "said", "isn't", "aren't", "don't", "just", "like", "some",
    ];
    plain(text)
        .split(|c: char| !c.is_alphanumeric() && c != '\'')
        .filter(|w| w.chars().count() >= 4 && !COMMON.contains(w))
        .map(str::to_string)
        .collect()
}

/// The sentences of `answer` that `question` questions: those sharing the
/// most content words with it (two at least), at most three.
pub fn disputed(question: &str, answer: &str) -> Vec<String> {
    let q = content_words(question);
    if q.is_empty() {
        return Vec::new();
    }
    let mut scored: Vec<(usize, usize, String)> = answer
        .split(['.', '\n', '!', '?'])
        .map(str::trim)
        .filter(|s| s.chars().count() >= 12)
        .enumerate()
        .filter_map(|(i, s)| {
            let words = content_words(s);
            let shared = q.iter().filter(|w| words.contains(w)).count();
            (shared >= 2).then(|| {
                (
                    shared,
                    i,
                    s.trim_start_matches(['-', '*', '#', ' ']).to_string(),
                )
            })
        })
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    scored.truncate(3);
    scored.sort_by_key(|s| s.1);
    scored.into_iter().map(|(_, _, s)| s).collect()
}

/// Reads a question. `last_answer` is Chat's previous answer in the thread.
pub fn read(question: &str, last_answer: Option<&str>, s: Signals) -> Reading {
    let q = plain(question);
    let role = role_of(question);
    let newcomer = has(&q, NEWCOMER);
    let last = last_answer.map(str::trim).filter(|a| !a.is_empty());
    let mut disputes: Vec<String> = Vec::new();
    let corrects = match last {
        Some(a) if has(&q, CORRECTION) || q.starts_with("no,") || q.starts_with("actually") => {
            disputes = disputed(question, a);
            // "What do you mean" and plain contradictions question the last
            // answer even when no sentence of it is named again.
            !disputes.is_empty()
                || has(
                    &q,
                    &[
                        "what do you mean",
                        "what did you mean",
                        "you said",
                        "that's wrong",
                        "you're wrong",
                        "that's not right",
                        "that isn't right",
                        "not true",
                        "incorrect",
                        "not what i asked",
                        "i didn't ask",
                        "are you sure",
                    ],
                )
        }
        _ => false,
    };
    let intent = if corrects {
        Intent::Correction
    } else if s.needs_me {
        Intent::NeedsMe
    } else if (newcomer || has(&q, OFFER)) && !s.names_work {
        Intent::Orientation
    } else if is_small_talk(question) {
        Intent::SmallTalk
    } else if has(&q, CAPABILITY) {
        Intent::Capability
    } else if has(&q, JUDGEMENT) {
        Intent::Judgement
    } else if s.status {
        Intent::Status
    } else if has(&q, HOW_TO) {
        Intent::HowTo
    } else {
        Intent::Lookup
    };
    Reading {
        intent,
        role,
        newcomer,
        disputed: if intent == Intent::Correction {
            disputes
        } else {
            Vec::new()
        },
        about_app: matches!(intent, Intent::Lookup | Intent::HowTo) && about_app(question),
    }
}

impl Reading {
    /// The note the model reads before the question.
    pub fn note(&self) -> String {
        let mut out = format!(
            "\nHow this question reads (check it against the question itself; if it asks something else, answer what it actually asks):\n- They want {}.\n",
            self.intent.words()
        );
        if let Some(r) = &self.role {
            out.push_str(&format!(
                "- They say they are the {r}: answer for what someone in that role needs.\n"
            ));
        }
        if self.newcomer {
            out.push_str("- They are new to Coderhelm.\n");
        }
        if !self.disputed.is_empty() {
            out.push_str("- What they question in your last answer:\n");
            for d in &self.disputed {
                out.push_str(&format!(
                    "  \u{201c}{}\u{201d}\n",
                    crate::truncate_str(d, 300)
                ));
            }
        }
        out.push_str(&format!("- How to answer: {}\n", self.intent.shape()));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(q: &str) -> Intent {
        read(q, None, Signals::default()).intent
    }

    #[test]
    fn a_newcomer_asking_what_helps_them_wants_an_orientation() {
        let q = "this is my first time using coderhelm. i'm the cto of the company. are are a few things that you think could be useful to me";
        let reading = read(q, None, Signals::default());
        assert_eq!(reading.intent, Intent::Orientation);
        assert_eq!(reading.role.as_deref(), Some("cto"));
        assert!(reading.newcomer);
        for q in [
            "What can you do?",
            "how can coderhelm help me as an engineering manager?",
            "I'm new here, where do I start?",
            "what is coderhelm",
            "What should I use you for?",
        ] {
            assert_eq!(r(q), Intent::Orientation, "{q}");
        }
    }

    #[test]
    fn a_question_naming_work_or_asking_what_waits_is_not_an_orientation() {
        let s = Signals {
            needs_me: true,
            ..Default::default()
        };
        assert_eq!(
            read("I'm new, what needs my answer?", None, s).intent,
            Intent::NeedsMe
        );
        let s = Signals {
            names_work: true,
            status: true,
            ..Default::default()
        };
        assert_eq!(
            read("what can you tell me about ABC-12?", None, s).intent,
            Intent::Status
        );
    }

    #[test]
    fn every_intent_reads_from_its_own_words() {
        assert_eq!(
            r("are you able to run skills in the web repo"),
            Intent::Capability
        );
        assert_eq!(r("Can you see our production logs?"), Intent::Capability);
        assert_eq!(r("How can I cancel a reservation?"), Intent::HowTo);
        assert_eq!(r("how do I add a knowledge base"), Intent::HowTo);
        assert_eq!(
            r("is this comment accurate https://github.com/acme/web/pull/14#issuecomment-1"),
            Intent::Judgement
        );
        assert_eq!(r("should we roll back the last deploy?"), Intent::Judgement);
        assert_eq!(r("what caused the enospc error?"), Intent::Lookup);
        assert_eq!(
            r("what does B7 mean in reference to crawlers"),
            Intent::Lookup
        );
        let st = Signals {
            status: true,
            ..Default::default()
        };
        assert_eq!(
            read("where are we on the portal?", None, st).intent,
            Intent::Status
        );
        for q in [
            "thanks!",
            "hi",
            "ok cool",
            "thank you so much",
            "👍",
            "Good morning",
        ] {
            assert_eq!(r(q), Intent::SmallTalk, "{q}");
        }
        assert_ne!(r("hi, why is the build red?"), Intent::SmallTalk);
    }

    #[test]
    fn a_correction_names_what_it_questions_in_the_last_answer() {
        let last = "Welcome. Three runs are waiting on you. Jira isn't connected to Coderhelm, so your Jira tickets aren't in this list. Ask me for a DORA report.";
        let q = "what do you mean that jira isn't connected? i see atlassian in the integrations";
        let reading = read(q, Some(last), Signals::default());
        assert_eq!(reading.intent, Intent::Correction);
        assert_eq!(
            reading.disputed,
            ["Jira isn't connected to Coderhelm, so your Jira tickets aren't in this list"]
        );
        let note = reading.note();
        assert!(note.contains("Jira isn't connected to Coderhelm"), "{note}");
        assert!(
            note.contains("Check the disputed point again now"),
            "{note}"
        );
        // The same words with no earlier answer are not a correction.
        assert_ne!(r(q), Intent::Correction);
        // A plain contradiction with no shared words still is.
        assert_eq!(
            read("that's wrong", Some(last), Signals::default()).intent,
            Intent::Correction
        );
        // A follow-up that questions nothing is read as itself.
        assert_eq!(
            read("and who owns the web repo?", Some(last), Signals::default()).intent,
            Intent::Lookup
        );
    }

    #[test]
    fn only_the_list_question_gets_the_fixed_shape_and_only_small_talk_goes_uncited() {
        for i in [
            Intent::Orientation,
            Intent::Capability,
            Intent::Status,
            Intent::HowTo,
            Intent::Lookup,
            Intent::Judgement,
            Intent::Correction,
            Intent::SmallTalk,
        ] {
            assert!(!i.fixed_shape(), "{i:?}");
            assert!(!i.shape().is_empty());
        }
        assert!(Intent::NeedsMe.fixed_shape());
        assert!(Intent::SmallTalk.uncited_ok());
        assert!(!Intent::Orientation.uncited_ok() && !Intent::Correction.uncited_ok());
    }

    #[test]
    fn how_the_app_fits_together_reads_as_a_map_question() {
        for q in [
            "What happens when someone books a slip?",
            "how does booking a slip work",
            "How do refunds get to Stripe?",
            "what calls the bookings endpoint",
            "What's behind the /slips/:id screen?",
            "which service charges the card",
            "where does the booking request go",
            "how is the harbor website connected to the booking service",
        ] {
            let r = read(q, None, Signals::default());
            assert!(r.about_app, "{q}");
            assert!(matches!(r.intent, Intent::Lookup | Intent::HowTo), "{q}");
        }
        for q in [
            "what can you do?",
            "how do I add a knowledge base",
            "where are we on the portal?",
            "thanks!",
            "what caused the enospc error?",
            "should we roll back the last deploy?",
        ] {
            assert!(!read(q, None, Signals::default()).about_app, "{q}");
        }
    }

    #[test]
    fn roles_are_read_as_written_and_non_roles_are_not() {
        assert_eq!(
            role_of("As a product manager, what can you do?").as_deref(),
            Some("product manager")
        );
        assert_eq!(
            role_of("I am the head of platform here").as_deref(),
            Some("head")
        );
        assert_eq!(
            role_of("I'm a backend engineer.").as_deref(),
            Some("backend engineer")
        );
        assert_eq!(role_of("I'm the one who merged it"), None);
        assert_eq!(role_of("as a result the build broke"), None);
        assert_eq!(role_of("what is the cto dashboard"), None);
    }
}
