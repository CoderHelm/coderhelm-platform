//! How Chat gets the evidence a question needs and finishes it: a
//! diagnosis (an alert, an error, an incident) reads the live evidence (the
//! alarm's metric and its traffic, the service's logs for the window, its
//! resource metrics) before the code ([`EvidenceFirst`]); a result too long
//! to send whole keeps its head and tail and says what was cut and how to
//! read the rest ([`fit_result`]), and an answer over a cut-off result it
//! never narrowed goes back ([`read_the_rest`]); and an answer that stops
//! short of checks it can still run ("I ran out of lookups") goes back to
//! run them ([`says_unfinished`]). No count of lookups ends an answer.

/// A diagnosis reads the live evidence before the code: until `live_wanted`
/// lookups of the `live` tools (metrics, logs, connected monitoring) were
/// made, only `others_before` other lookups run (to find the alarm's
/// definition or the service's name); another is answered with
/// [`evidence_first_note`] and not run.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EvidenceFirst {
    pub live: Vec<String>,
    pub live_wanted: usize,
    pub others_before: usize,
}

/// Live lookups that make a diagnosis's evidence: the metric and its
/// traffic, the logs for the window, the resource metrics.
pub const LIVE_WANTED: usize = 3;
/// Other lookups a diagnosis may make before its live evidence is in.
pub const OTHERS_BEFORE: usize = 2;

impl EvidenceFirst {
    /// `None` when no live tool is offered (nothing to read first).
    pub fn over(live: Vec<String>) -> Option<Self> {
        (!live.is_empty()).then_some(EvidenceFirst {
            live,
            live_wanted: LIVE_WANTED,
            others_before: OTHERS_BEFORE,
        })
    }

    /// Whether `name` waits for the live evidence, given the calls made
    /// before it.
    pub fn holds(&self, name: &str, made: &[String]) -> bool {
        if self.live.iter().any(|l| l == name) {
            return false;
        }
        let live = made.iter().filter(|c| self.live.contains(c)).count();
        let others = made.len() - live;
        live < self.live_wanted && others >= self.others_before
    }
}

/// What a lookup held for the live evidence reads.
pub fn evidence_first_note(live: &[String]) -> String {
    format!(
        "Not run: this is a diagnosis and its live evidence isn't in yet. Read it first with {}, \
         the independent lookups together in one turn: the metric behind the alarm or error \
         (its statistic, such as p99, a few minutes either side of when it fired) and the \
         traffic on the same target (request count); the service's logs for that window; the \
         service's resource metrics (CPU, memory). Then read the code to explain what the \
         evidence shows.",
        live.join(", ")
    )
}

/// Most of a tool result the model reads at once, in characters.
pub const MAX_RESULT_CHARS: usize = 8_000;
/// How a result cut by [`fit_result`] says so.
pub const CUT_OFF: &str = "[cut off:";

/// A result as the model reads it: whole when it fits, else its head and
/// tail (an error is usually at one end, the latest state at the other) and,
/// between them, what was left out and how to read it.
pub fn fit_result(text: &str) -> String {
    let total = text.chars().count();
    if total <= MAX_RESULT_CHARS {
        return text.to_string();
    }
    let head_n = MAX_RESULT_CHARS * 3 / 4;
    let tail_n = MAX_RESULT_CHARS - head_n;
    let head: String = text.chars().take(head_n).collect();
    let tail: String = text.chars().skip(total - tail_n).collect();
    format!(
        "{head}\n{CUT_OFF} {} of {total} characters are not shown here. Narrow this lookup to \
         read them: a shorter time window around the moment that matters, a filter on the \
         service, the error or the field you need, fewer fields, or the next page. Don't answer \
         from the part shown as if it were all of it.]\n{tail}",
        total - head_n - tail_n
    )
}

/// A result that came back cut off: here or by the source itself.
pub fn was_cut(text: &str) -> bool {
    text.contains(CUT_OFF) || text.contains("bytes elided") || text.contains("[truncated")
}

/// The note sent back when an answer rests on a result it left cut off.
pub fn read_the_rest(what: &str) -> String {
    format!(
        "<check>The result of \"{what}\" was cut off and your answer didn't read the rest. \
         Narrow that lookup (a shorter time window, a filter on the service or the error, fewer \
         fields, or the next page) and read what was left out, then answer with citations.</check>"
    )
}

/// The answer stops short: it lists checks it says it didn't run for want
/// of lookups or time ("Not checked (I ran out of lookups)"). A check that
/// ran and failed, named with its own reason, is not this.
pub fn says_unfinished(answer: &str) -> bool {
    const PHRASES: &[&str] = &[
        "ran out of lookups",
        "run out of lookups",
        "out of lookups",
        "no lookups left",
        "lookups left",
        "lookup limit",
        "lookup budget",
        "used all the lookups",
        "used all my lookups",
        "didn't get to check",
        "did not get to check",
        "didn't get to look",
        "did not get to look",
        "didn't have time to check",
        "haven't checked yet",
        "have not checked yet",
        "not checked (i ran out",
        "not checked yet",
    ];
    let a = answer.to_lowercase().replace('\u{2019}', "'");
    PHRASES.iter().any(|p| a.contains(p))
}

/// A diagnosis that hedges instead of concluding ("probably one or two
/// slow requests"): the lookup that would settle it wasn't run.
pub fn says_hedged(answer: &str) -> bool {
    const PHRASES: &[&str] = &[
        "one or two slow requests",
        "one or two slow calls",
        "a few slow requests",
        "probably just",
        "likely just",
        "might just be",
        "could just be",
        "probably a blip",
        "probably noise",
        "likely noise",
        "hard to say",
        "difficult to say",
        "can't say for sure",
        "cannot say for sure",
        "it's possible that",
        "it is possible that",
    ];
    let a = answer.to_lowercase().replace('\u{2019}', "'");
    PHRASES.iter().any(|p| a.contains(p))
}

/// The note sent back when a diagnosis hedges.
pub const FIRM_UP: &str = "<check>Your answer hedges instead of concluding. Run the lookup that settles it: for latency or errors, break the metric down by path (route or downstream call) and line it up with request volume, over the alarm's window and the same hours on a normal day; a percentile over little traffic is set by a handful of slow calls. Then state the cause the evidence shows, plainly, and the concrete fix (for a slow call: cache it or speed it up; for an alarm that fires only on low traffic: a minimum request count on the alarm). Cite each point.</check>";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_answer_that_stops_short_is_told_from_one_that_names_a_failed_check() {
        assert!(says_unfinished(
            "Likely a slow dependency.\n\n**Not checked (I ran out of lookups)**\n- the target group's p99"
        ));
        assert!(says_unfinished(
            "I didn\u{2019}t get to check the service logs."
        ));
        assert!(!says_unfinished(
            "Jira: not checked: Jira isn't connected to Coderhelm."
        ));
        // A run's own stop reason is not the answer stopping short.
        assert!(!says_unfinished("The build stopped: Ran out of time [S1]."));
        assert!(!says_unfinished(
            "The p99 rose with traffic at 04:10 [S1], and the logs show slow calls to the payments API [S2]."
        ));
    }

    #[test]
    fn a_hedged_diagnosis_is_told_from_a_firm_one() {
        assert!(says_hedged(
            "The alarm was probably one or two slow requests at night; the upstream wasn't slow."
        ));
        assert!(says_hedged("It's hard to say what caused it."));
        assert!(!says_hedged(
            "One path is slow all day: GET /v1/tides/forecast averages 3.4 s [S2]. The alarm fires only overnight, when traffic drops to about 110 requests per 5 minutes and those calls become the p99 [S1]."
        ));
    }

    #[test]
    fn a_diagnosis_reads_its_live_evidence_before_more_code() {
        let e = EvidenceFirst::over(vec!["metrics_query".into(), "logs_search".into()]).unwrap();
        let made = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        // Two code reads to find the names, then the evidence first.
        assert!(!e.holds("github__search_code", &made(&[])));
        assert!(!e.holds("github__read_file", &made(&["github__search_code"])));
        assert!(e.holds(
            "github__read_file",
            &made(&["github__search_code", "github__read_file"])
        ));
        assert!(!e.holds(
            "metrics_query",
            &made(&["github__search_code", "github__read_file"])
        ));
        // Once the evidence is in, the code is read freely.
        let all = made(&[
            "github__search_code",
            "github__read_file",
            "metrics_query",
            "metrics_query",
            "logs_search",
        ]);
        assert!(!e.holds("github__read_file", &all));
        assert!(EvidenceFirst::over(Vec::new()).is_none());
        assert!(evidence_first_note(&e.live).contains("metrics_query, logs_search"));
    }

    #[test]
    fn a_long_result_keeps_its_ends_and_says_how_to_read_the_rest() {
        let short = "p99 2.1s at 04:10";
        assert_eq!(fit_result(short), short);
        assert!(!was_cut(short));
        let long = format!("HEAD{}TAIL", "x".repeat(MAX_RESULT_CHARS * 2));
        let fit = fit_result(&long);
        assert!(
            fit.starts_with("HEAD") && fit.ends_with("TAIL"),
            "{}",
            &fit[..40]
        );
        assert!(fit.contains("Narrow this lookup"));
        assert!(was_cut(&fit));
        assert!(fit.chars().count() < MAX_RESULT_CHARS + 600);
        // A source's own cut counts too.
        assert!(was_cut("rows...\n\n[… 4096 bytes elided …]\n\nmore rows"));
        assert!(read_the_rest("metrics query").contains("Narrow that lookup"));
    }
}
