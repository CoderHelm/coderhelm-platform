//! Where a ticket's fix belongs: in the code, outside it (data or settings in
//! another system), both, or not yet known.
//!
//! The planner decides this as structured output (a ```fix_location.json
//! block next to the openspec files) from what it looked up: the rule the code
//! is meant to follow, and what the data or settings show. When the code works
//! as designed and the outcome comes from a source system (a booking system's
//! setting, a CMS entry, a feature flag in another tool), the fix is a change
//! there. Overriding that system's data in code (a hardcoded list of record
//! ids) hides the real cause and goes stale, so the run reports the change to
//! make instead of opening a pull request.

use serde::{Deserialize, Deserializer, Serialize};

/// The fence the planner writes its decision in.
pub const FENCE: &str = "fix_location.json";

/// Stored next to the openspec files, so a reused plan keeps its decision.
pub const FILE: &str = "fix_location.json";

/// The run status when the whole fix is a change outside the code.
pub const STATUS_OUTSIDE: &str = "needs_outside_change";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FixLocation {
    /// A change in this code (what every run did before this field existed).
    #[default]
    Code,
    /// The code behaves as designed; data or settings in another system
    /// produce the outcome, and changing them there is the fix.
    OutsideCode,
    /// A code change and a change in another system are both needed.
    Both,
    /// The planner looked and could not tell which.
    Unknown,
}

impl FixLocation {
    /// Reads the planner's value; anything it can't name is `Unknown`.
    pub fn parse(s: &str) -> FixLocation {
        match s
            .trim()
            .to_ascii_lowercase()
            .replace(['-', ' '], "_")
            .as_str()
        {
            "code" => FixLocation::Code,
            "outside_code" | "outside_the_code" | "outside" => FixLocation::OutsideCode,
            "both" => FixLocation::Both,
            _ => FixLocation::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            FixLocation::Code => "code",
            FixLocation::OutsideCode => "outside_code",
            FixLocation::Both => "both",
            FixLocation::Unknown => "unknown",
        }
    }
}

impl<'de> Deserialize<'de> for FixLocation {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(FixLocation::parse(&s))
    }
}

/// What the decision rests on.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixEvidence {
    /// What was looked up (the query or tool, and a summary of what came
    /// back), one item per lookup.
    #[serde(default, deserialize_with = "string_or_list")]
    pub looked_up: Vec<String>,
    /// The rule the code is meant to follow, in plain words.
    #[serde(default)]
    pub code_rule: String,
    /// What the data or settings show.
    #[serde(default)]
    pub data_shows: String,
}

/// The change a person makes outside the code.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutsideChange {
    /// The system the change is made in ("the booking system").
    #[serde(default)]
    pub system: String,
    /// Exactly what to change.
    #[serde(default)]
    pub change: String,
    /// Who makes it, or where in that system.
    #[serde(default, rename = "where")]
    pub where_: String,
    /// How to confirm it worked afterwards.
    #[serde(default)]
    pub verify: String,
}

impl OutsideChange {
    fn is_usable(&self) -> bool {
        !self.change.trim().is_empty() && !self.system.trim().is_empty()
    }
}

/// The planner's structured decision.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixDecision {
    #[serde(default)]
    pub fix_location: FixLocation,
    /// The cause, in one or two plain sentences.
    #[serde(default)]
    pub cause: String,
    #[serde(default)]
    pub evidence: FixEvidence,
    #[serde(default)]
    pub outside_change: Option<OutsideChange>,
    /// When the ticket has history (an earlier CoderHelm pull request closed
    /// without merging, a lesson or a person saying the fix is elsewhere):
    /// why a code change is still right, answering that history. Empty when
    /// the ticket has none.
    #[serde(default)]
    pub history_check: String,
}

fn string_or_list<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }
    Ok(match Option::<OneOrMany>::deserialize(d)? {
        None => Vec::new(),
        Some(OneOrMany::One(s)) if s.trim().is_empty() => Vec::new(),
        Some(OneOrMany::One(s)) => vec![s],
        Some(OneOrMany::Many(v)) => v.into_iter().filter(|s| !s.trim().is_empty()).collect(),
    })
}

/// What the run does with the plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// Implement and open a pull request, as before.
    Code,
    /// No branch, no pull request: report the change to make outside the code.
    OutsideOnly,
    /// Implement the code part; the PR and the ticket also carry the outside change.
    CodeAndOutside,
    /// Not sure where the fix belongs: report what was found and ask.
    Ask,
}

impl FixDecision {
    /// What the decision actually supports. An outside change with nothing to
    /// change is not a decision: it is reported as unknown, never coded around.
    pub fn effective_location(&self) -> FixLocation {
        let usable = self
            .outside_change
            .as_ref()
            .is_some_and(OutsideChange::is_usable);
        match self.fix_location {
            FixLocation::OutsideCode | FixLocation::Both if !usable => FixLocation::Unknown,
            l => l,
        }
    }
}

/// How the run proceeds. A plan without a decision (a model that left the
/// block out, or one that didn't read) is not a code plan: where the fix
/// belongs was never decided, so the run asks instead of coding.
pub fn route(decision: Option<&FixDecision>) -> Route {
    match decision.map(FixDecision::effective_location) {
        None => Route::Ask,
        Some(FixLocation::Code) => Route::Code,
        Some(FixLocation::OutsideCode) => Route::OutsideOnly,
        Some(FixLocation::Both) => Route::CodeAndOutside,
        Some(FixLocation::Unknown) => Route::Ask,
    }
}

/// The sentinel tasks.md carries when the planner plans no code.
pub const NO_CODE_CHANGE: &str = "NO_CODE_CHANGE:";

/// How the run proceeds for this plan. A plan that says it changes no code
/// but decided `code` contradicts itself: nothing would be implemented, so it
/// is reported and asked about instead.
pub fn route_for_plan(decision: Option<&FixDecision>, tasks: &str) -> Route {
    match route(decision) {
        Route::Code | Route::CodeAndOutside if tasks.trim_start().starts_with(NO_CODE_CHANGE) => {
            Route::Ask
        }
        r => r,
    }
}

/// The report as Jira shows it: the Jira app turns each paragraph into one
/// plain-text paragraph (no markdown, single newlines become spaces), so
/// every line becomes its own paragraph and the emphasis marks go.
pub fn jira_text(markdown: &str) -> String {
    markdown
        .lines()
        .map(|l| l.replace("**", "").trim().to_string())
        .filter(|l| !l.is_empty())
        .map(|l| match l.strip_prefix("- ") {
            Some(item) => format!("• {item}"),
            None => l,
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// The decision in the planner's reply, if it wrote one that reads.
pub fn parse(response: &str) -> Option<FixDecision> {
    let marker = format!("```{FENCE}");
    let start = response.find(&marker)?;
    let body_start = start + response[start..].find('\n')? + 1;
    let end = response[body_start..]
        .find("```")
        .map(|i| body_start + i)
        .unwrap_or(response.len());
    let raw = response[body_start..end].trim();
    match serde_json::from_str::<FixDecision>(raw) {
        Ok(d) => Some(d),
        Err(e) => {
            tracing::warn!(error = %e, "Planner's fix_location block didn't read");
            None
        }
    }
}

/// The planner's instructions for the decision, appended to its prompt.
pub const PLANNER_GUIDANCE: &str = r#"## Where the fix belongs (decide before you plan code)

Before writing tasks, decide whether the fix belongs in this code at all.

- Find the rule the code is meant to follow for the case in the ticket (read the code path; read its comments, names and tests for intent).
- When a live source, data lookup or check can show the actual record or setting, look it up and compare it with that rule.
- If the code behaves as designed and the outcome comes from data or configuration in a source system (a booking or scheduling system, a CMS, a ticket tool's configuration, a feature flag held in another system, a cloud console setting), the fix is a change in that system, not in this code. Set `fix_location` to `outside_code`, describe the exact change, and set tasks.md to `NO_CODE_CHANGE: <one line>`.
- Never override source data in code: no hardcoded record ids, allowlists, denylists or "force" lists that change behavior for specific records the source system already decides. That hides the cause and goes stale. If that is the only way you see to make the ticket's symptom go away, the fix is outside the code.
- If a code change is needed AND a change in another system, use `both`: plan the code part normally and describe the outside change.
- If you looked and still cannot tell whether the code or the data is at fault, use `unknown`, say what you checked and what a person needs to confirm, and set tasks.md to `NO_CODE_CHANGE: <what is unclear>`. Prefer asking over coding.
- Ordinary feature work and bugs in the code's own logic are `code`.
- Quote values only as the tools returned them; a value shown as `[hidden]` stays hidden.
- If the prompt has a "This ticket's history" section (an earlier pull request for this ticket closed without merging, a lesson, or a person saying the fix is elsewhere), read it first. The default then is NOT to plan code again: use `outside_code` or `unknown`. Plan code only when you can say why a code change is right despite that history, in `history_check`; a change like the one already closed is never right.

After the four openspec files, output this block (JSON, all fields required; `outside_change` is null for `code`):

```fix_location.json
{
  "fix_location": "code" | "outside_code" | "both" | "unknown",
  "cause": "one or two plain sentences: why the ticket's symptom happens",
  "evidence": {
    "looked_up": ["each lookup you made and a short summary of what came back"],
    "code_rule": "the rule the code is meant to follow for this case",
    "data_shows": "what the data or settings show for the records in the ticket"
  },
  "outside_change": {
    "system": "the system the change is made in",
    "change": "exactly what to change, naming the record(s)",
    "where": "who can make it, or where in that system",
    "verify": "how to confirm afterwards (for example: after the next sync, the item shows on the site)"
  },
  "history_check": "only when the prompt has a ticket history: why a code change is right despite it; otherwise empty"
}
```"#;

fn push_evidence(out: &mut String, d: &FixDecision) {
    let ev = &d.evidence;
    if !ev.looked_up.is_empty() {
        out.push_str("**What I checked**\n");
        for l in &ev.looked_up {
            out.push_str(&format!("- {}\n", one_line(l)));
        }
        out.push('\n');
    }
    if !ev.code_rule.trim().is_empty() {
        out.push_str(&format!(
            "**What the code is meant to do:** {}\n\n",
            ev.code_rule.trim()
        ));
    }
    if !ev.data_shows.trim().is_empty() {
        out.push_str(&format!(
            "**What the data shows:** {}\n\n",
            ev.data_shows.trim()
        ));
    }
}

fn push_change(out: &mut String, c: &OutsideChange) {
    out.push_str(&format!("- **Where:** {}", c.system.trim()));
    if !c.where_.trim().is_empty() {
        out.push_str(&format!(" ({})", c.where_.trim()));
    }
    out.push('\n');
    out.push_str(&format!("- **What to change:** {}\n", c.change.trim()));
    if !c.verify.trim().is_empty() {
        out.push_str(&format!("- **How to confirm:** {}\n", c.verify.trim()));
    }
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The report for a run whose whole fix is outside the code: the ticket
/// comment, and what the run page shows. `heading` adds the title line (the
/// Jira comment carries its own title).
pub fn outside_report(d: &FixDecision, heading: bool) -> String {
    let mut out = String::new();
    if heading {
        out.push_str("**Needs a change outside the code**\n\n");
    }
    if !d.cause.trim().is_empty() {
        out.push_str(&format!("**Cause:** {}\n\n", d.cause.trim()));
    }
    push_evidence(&mut out, d);
    if let Some(c) = d.outside_change.as_ref().filter(|c| c.is_usable()) {
        out.push_str("**The change to make**\n");
        push_change(&mut out, c);
        out.push('\n');
    }
    out.push_str(
        "No code was changed and no pull request was opened: the code works as designed, and \
         changing it would override the data in the source system. Once the change above is \
         made, nothing else is needed here.",
    );
    out
}

/// The decision a run asks with when the plan carried none.
pub fn undecided() -> FixDecision {
    FixDecision {
        fix_location: FixLocation::Unknown,
        cause: "The plan didn't say where the fix belongs (in this code or in data or settings \
                in another system), so I stopped before changing any code."
            .to_string(),
        ..Default::default()
    }
}

/// The report when the planner could not tell where the fix belongs.
pub fn unknown_report(d: &FixDecision, heading: bool) -> String {
    let mut out = String::new();
    if heading {
        out.push_str("**Not sure yet where the fix belongs**\n\n");
    }
    if !d.cause.trim().is_empty() {
        out.push_str(&format!("**What I found:** {}\n\n", d.cause.trim()));
    }
    push_evidence(&mut out, d);
    if let Some(c) = d.outside_change.as_ref() {
        if !c.change.trim().is_empty() {
            out.push_str(&format!(
                "**Possibly a change outside the code:** {}\n\n",
                c.change.trim()
            ));
        }
    }
    out.push_str(
        "I didn't change any code. Please confirm whether the code or the data is at fault \
         (reply here with what you find), and I'll pick it up from there.",
    );
    out
}

/// The section a pull request (and its ticket comment) carries when a change
/// outside the code is needed too. Empty when it isn't.
pub fn also_outside_section(d: Option<&FixDecision>) -> String {
    let Some(d) = d else {
        return String::new();
    };
    if d.effective_location() != FixLocation::Both {
        return String::new();
    }
    let Some(c) = d.outside_change.as_ref() else {
        return String::new();
    };
    let mut out = String::from("\n\n## Also needed outside the code\n\n");
    if !d.cause.trim().is_empty() {
        out.push_str(&format!("{}\n\n", d.cause.trim()));
    }
    push_change(&mut out, c);
    out.push_str("\nThis pull request does not make that change; it has to be made there.\n");
    out
}

/// What the reviewer is told about the plan behind a CoderHelm pull request,
/// so it can judge a diff against what the planner found in the data.
pub fn reviewer_context(d: &FixDecision) -> String {
    let mut out = format!(
        "\n## What the planner found (where the fix belongs: {})\n",
        d.effective_location().as_str()
    );
    if !d.cause.trim().is_empty() {
        out.push_str(&format!("Cause: {}\n", d.cause.trim()));
    }
    for l in &d.evidence.looked_up {
        out.push_str(&format!("- Looked up: {}\n", one_line(l)));
    }
    if !d.evidence.code_rule.trim().is_empty() {
        out.push_str(&format!("Code's rule: {}\n", d.evidence.code_rule.trim()));
    }
    if !d.evidence.data_shows.trim().is_empty() {
        out.push_str(&format!("Data shows: {}\n", d.evidence.data_shows.trim()));
    }
    if let Some(c) = &d.outside_change {
        if !c.change.trim().is_empty() {
            out.push_str(&format!(
                "Change outside the code: {} — {}\n",
                c.system.trim(),
                c.change.trim()
            ));
        }
    }
    out
}

#[cfg(test)]
pub mod fixture {
    //! A ticket where the code is right and the data decides: an item is
    //! missing from the site because its term is not published on the web
    //! in the source booking system, and the sync job correctly marks it
    //! inactive.

    /// The planner's reply on that ticket, as the model writes it.
    pub const PLANNER_REPLY: &str = r#"I looked up the program and its term.

```proposal.md
The Saturday class (booking system program 12345) is missing from the site.
```

```design.md
No code change: the sync job marks programs inactive when their term is not available on the web.
```

```tasks.md
NO_CODE_CHANGE: the term's "available on web" setting is off in the booking system.
```

```spec.md
Given the term is available on web, when the sync runs, then the class shows on the site.
```

```fix_location.json
{
  "fix_location": "outside_code",
  "cause": "The class's term is set to not be available on the web in the booking system, so the sync job marks the class inactive, as it is designed to.",
  "evidence": {
    "looked_up": [
      "programs where external id = 12345: one row, term id 678, active = false",
      "terms where id = 678: available_on_web = false"
    ],
    "code_rule": "The sync job marks a program inactive when its term is not available on the web (dags/sync_programs.py, is_program_active).",
    "data_shows": "Term 678 has available_on_web = false in the booking system, so program 12345 is correctly inactive."
  },
  "outside_change": {
    "system": "the booking system",
    "change": "Set term 678 (the term of program 12345) to available on web.",
    "where": "the term's settings page in the booking system's admin, by someone who manages the schedule",
    "verify": "After the next sync runs, program 12345 is active and the Saturday class shows on the site."
  }
}
```"#;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_data_case_is_outside_the_code_and_opens_no_pr() {
        let d = parse(fixture::PLANNER_REPLY).expect("the block reads");
        assert_eq!(d.fix_location, FixLocation::OutsideCode);
        assert_eq!(route(Some(&d)), Route::OutsideOnly);
        assert_eq!(d.evidence.looked_up.len(), 2);
        assert!(d.evidence.code_rule.contains("inactive"));
        let c = d.outside_change.as_ref().unwrap();
        assert_eq!(c.system, "the booking system");
        assert!(c.verify.contains("next sync"));
    }

    #[test]
    fn the_ticket_comment_says_cause_evidence_change_and_how_to_confirm() {
        let d = parse(fixture::PLANNER_REPLY).unwrap();
        let report = outside_report(&d, true);
        assert!(report.starts_with("**Needs a change outside the code**"));
        assert!(report.contains("**Cause:** The class's term is set to not be available"));
        assert!(report.contains("- terms where id = 678: available_on_web = false"));
        assert!(report.contains("**What the code is meant to do:** The sync job marks"));
        assert!(report.contains("**What the data shows:** Term 678"));
        assert!(report.contains("- **Where:** the booking system (the term's settings page"));
        assert!(report.contains("- **What to change:** Set term 678"));
        assert!(report.contains("- **How to confirm:** After the next sync runs"));
        assert!(report.contains("no pull request was opened"));
        // The Jira comment carries its own title.
        assert!(!outside_report(&d, false).contains("**Needs a change outside the code**"));
    }

    #[test]
    fn a_plan_without_a_decision_asks_and_never_codes() {
        assert_eq!(route(None), Route::Ask);
        assert!(unknown_report(&undecided(), false).contains("stopped before changing any code"));
        assert_eq!(parse("```tasks.md\n- [ ] a.rs: x\n```"), None);
        let d = FixDecision::default();
        assert_eq!(route(Some(&d)), Route::Code);
    }

    #[test]
    fn outside_without_a_change_to_make_is_unknown_and_asks() {
        let d = FixDecision {
            fix_location: FixLocation::OutsideCode,
            cause: "Something in the data.".into(),
            ..Default::default()
        };
        assert_eq!(route(Some(&d)), Route::Ask);
        let both = FixDecision {
            fix_location: FixLocation::Both,
            outside_change: Some(OutsideChange {
                system: "the CMS".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(route(Some(&both)), Route::Ask);
        assert!(unknown_report(&d, true).contains("I didn't change any code"));
    }

    #[test]
    fn values_are_read_loosely_but_never_guessed() {
        assert_eq!(FixLocation::parse("Outside-Code"), FixLocation::OutsideCode);
        assert_eq!(FixLocation::parse("BOTH"), FixLocation::Both);
        assert_eq!(FixLocation::parse("code"), FixLocation::Code);
        assert_eq!(FixLocation::parse("maybe"), FixLocation::Unknown);
        let d: FixDecision = serde_json::from_str(
            r#"{"fix_location":"code","evidence":{"looked_up":"one lookup"},"outside_change":null}"#,
        )
        .unwrap();
        assert_eq!(d.evidence.looked_up, vec!["one lookup".to_string()]);
        assert!(d.outside_change.is_none());
    }

    #[test]
    fn both_puts_the_outside_change_in_the_pr_body() {
        let mut d = parse(fixture::PLANNER_REPLY).unwrap();
        assert_eq!(also_outside_section(Some(&d)), "");
        d.fix_location = FixLocation::Both;
        assert_eq!(route(Some(&d)), Route::CodeAndOutside);
        let s = also_outside_section(Some(&d));
        assert!(s.contains("## Also needed outside the code"));
        assert!(s.contains("Set term 678"));
        assert!(s.contains("After the next sync runs"));
        assert_eq!(also_outside_section(None), "");
    }

    #[test]
    fn a_plan_that_codes_nothing_is_never_sent_to_implement() {
        let code = FixDecision::default();
        assert_eq!(
            route_for_plan(Some(&code), "NO_CODE_CHANGE: data is off"),
            Route::Ask
        );
        assert_eq!(route_for_plan(None, "NO_CODE_CHANGE: x"), Route::Ask);
        // No decision is never a code plan: the run asks (fail closed).
        assert_eq!(route_for_plan(None, "- [ ] a.rs: x"), Route::Ask);
        assert_eq!(route(None), Route::Ask);
        assert_eq!(route(Some(&code)), Route::Code);
        let d = parse(fixture::PLANNER_REPLY).unwrap();
        let tasks = "NO_CODE_CHANGE: the term's setting is off";
        assert_eq!(route_for_plan(Some(&d), tasks), Route::OutsideOnly);
    }

    #[test]
    fn the_jira_comment_is_one_plain_paragraph_per_line() {
        let d = parse(fixture::PLANNER_REPLY).unwrap();
        let jira = jira_text(&outside_report(&d, false));
        assert!(!jira.contains("**"));
        assert!(jira.contains("\n\n• terms where id = 678: available_on_web = false\n\n"));
        assert!(jira.starts_with("Cause: The class's term"));
        assert!(jira.contains("• How to confirm: After the next sync runs"));
    }

    #[test]
    fn the_decision_round_trips_through_storage() {
        let d = parse(fixture::PLANNER_REPLY).unwrap();
        let stored = serde_json::to_string(&d).unwrap();
        assert!(stored.contains("\"fix_location\":\"outside_code\""));
        assert!(stored.contains("\"where\":"));
        let back: FixDecision = serde_json::from_str(&stored).unwrap();
        assert_eq!(back, d);
    }
}
