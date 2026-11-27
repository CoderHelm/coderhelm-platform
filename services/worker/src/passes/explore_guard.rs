//! Explore before the first edit; extend what exists instead of adding a
//! twin beside it.
//!
//! A build once wrote a second overrides map and two helpers next to the
//! map that already did the job, in its first edit, without one search.
//! In the coder's loop now:
//!
//! - **The first write of a build names what it reuses.** It carries
//!   [`REUSES_INPUT`]: the existing maps, helpers, config and tests the
//!   change extends, by path, or `none: searched <terms>`; and a search
//!   (`search_code`, a graph lookup, a `grep`) has run in the round. Until
//!   both hold, the write is refused with what to do. The list is kept as
//!   the coder's evidence (`Reuses: ...`).
//! - **A new map, helper or config beside an existing equivalent is
//!   refused** ([`ExploreGuard::check_edit`]): an edit whose new top-level
//!   definition parallels one already in the file (`minimal_change`) needs
//!   [`WHY_NEW_INPUT`], saying why the existing one can't hold the change.
//!   The reason is kept as the habits check's line `Wider than the ticket:`.
//! - **A second near-identical helper in the same build names what it
//!   shares** ([`ExploreGuard::check_edit`]): the guard keeps the helpers
//!   the build added, and an edit adding one whose name or body nearly
//!   matches one of them (`minimal_change::twins`), in this edit or an
//!   earlier one, needs [`SHARES_INPUT`]: the helper both now call, or why
//!   they can't share one. One build once added three near-identical
//!   helpers to one file in turn; each compared only with the code that
//!   was there before the build, so none was stopped.
//!
//! The refusals are the same every time; no count of tries opens them.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use super::minimal_change;

/// The write tools' field naming what the change reuses.
pub const REUSES_INPUT: &str = "reuses";
/// The write tools' field saying why a parallel structure is needed.
pub const WHY_NEW_INPUT: &str = "why_new";
/// The write tools' field naming what a second similar helper shares.
pub const SHARES_INPUT: &str = "shares";

/// The tools that count as a search of the code.
pub fn is_search(tool: &str, input: &serde_json::Value) -> bool {
    match tool {
        "search_code" | "find_references" | "find_symbol" | "find_definition" | "who_calls"
        | "code_graph" => true,
        "bash" => input
            .get("command")
            .and_then(|c| c.as_str())
            .is_some_and(|c| {
                let c = c.trim_start();
                ["grep ", "rg ", "git grep", "ag ", "find "]
                    .iter()
                    .any(|p| c.starts_with(p) || c.contains(&format!("| {p}")))
            }),
        t => t.starts_with("graph_") || t.contains("search"),
    }
}

fn text_input<'a>(input: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    input
        .get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// The schema fields the write tools take for the guard.
pub fn offer_fields(schema: &mut serde_json::Value) {
    if let Some(props) = schema.get_mut("properties").and_then(|p| p.as_object_mut()) {
        props.insert(
            REUSES_INPUT.to_string(),
            serde_json::json!({
                "type": "string",
                "description": "Required on the first write of a build: the existing maps, helpers, constants, config and tests this change extends, each with its path (found by search_code), or `none: searched <terms>`."
            }),
        );
        props.insert(
            WHY_NEW_INPUT.to_string(),
            serde_json::json!({
                "type": "string",
                "description": "Only when this edit adds a map, helper or config next to an existing similar one: why the existing one can't hold the change."
            }),
        );
        props.insert(
            SHARES_INPUT.to_string(),
            serde_json::json!({
                "type": "string",
                "description": "Only when this edit adds a helper like one this build already added: the shared helper both call (by name and path), or why they can't share one."
            }),
        );
    }
}

/// The guard of one coder round.
#[derive(Debug, Default)]
pub struct ExploreGuard {
    /// The round must name what it reuses before its first write.
    require: bool,
    searched: AtomicBool,
    reuses: Mutex<Option<String>>,
    kept: Mutex<Vec<(String, String)>>,
    /// The helpers this build added: (path, definition, body).
    added: Mutex<Vec<(String, minimal_change::Def, String)>>,
    /// The round's tool calls up to its first write (`common::code_map`).
    pub counter: common::code_map::ExploreCounter,
}

impl ExploreGuard {
    /// A build round: the first write names what it reuses.
    pub fn for_build() -> Self {
        Self {
            require: true,
            ..Default::default()
        }
    }

    /// A fix round: the parallel check only.
    pub fn for_fix() -> Self {
        Self::default()
    }

    /// Records a tool call the coder made.
    pub fn saw(&self, tool: &str, input: &serde_json::Value) {
        if is_search(tool, input) {
            self.searched.store(true, Ordering::Relaxed);
        }
    }

    /// Whether a write may go ahead: `Err` is the refusal the coder reads.
    pub fn check_first_write(&self, input: &serde_json::Value) -> Result<(), String> {
        if !self.require || self.reuses.lock().unwrap().is_some() {
            return Ok(());
        }
        let searched = self.searched.load(Ordering::Relaxed);
        match (searched, text_input(input, REUSES_INPUT)) {
            (true, Some(r)) => {
                *self.reuses.lock().unwrap() = Some(r.to_string());
                Ok(())
            }
            (false, _) => Err(format!(
                "Not written: search before the first edit. Run `search_code` for the ticket's names and for the kind of structure you are about to add (a map, a helper, a config key, a test of the same code), read what it finds, then write again with `{REUSES_INPUT}` naming what you extend, by path."
            )),
            (true, None) => Err(format!(
                "Not written: the first write of a build names what it reuses. Call it again with `{REUSES_INPUT}`: the existing maps, helpers, config and tests this change extends, each with its path, or `none: searched <terms>`."
            )),
        }
    }

    /// Whether an edit of an existing file may add the definitions it adds:
    /// one parallel to an existing definition in the file needs a reason,
    /// and one twin to a helper this build added names what they share.
    pub fn check_edit(
        &self,
        path: &str,
        old: &str,
        new: &str,
        input: &serde_json::Value,
    ) -> Result<(), String> {
        self.check_twins(path, old, new, input)?;
        let pairs = minimal_change::parallel_in_edit(path, old, new);
        if pairs.is_empty() {
            return Ok(());
        }
        if let Some(why) = text_input(input, WHY_NEW_INPUT) {
            self.kept
                .lock()
                .unwrap()
                .push((path.to_string(), why.to_string()));
            return Ok(());
        }
        let list = pairs
            .iter()
            .map(|(n, e)| format!("`{}` next to the existing `{}`", n.name, e.name))
            .collect::<Vec<_>>()
            .join(", ");
        Err(format!(
            "Not written: this edit adds {list} in `{path}`. Extend the existing one instead (add the entries, or a parameter to the helper). If a separate one is really needed, call again with `{WHY_NEW_INPUT}` saying why the existing one can't hold the change."
        ))
    }

    /// A helper the edit adds that is a twin of one this build added (in
    /// this edit or before it) needs [`SHARES_INPUT`]. The helpers it adds
    /// are kept for the next edit.
    fn check_twins(
        &self,
        path: &str,
        old: &str,
        new: &str,
        input: &serde_json::Value,
    ) -> Result<(), String> {
        let before: Vec<String> = minimal_change::defs(old, path)
            .into_iter()
            .map(|d| d.name)
            .collect();
        let fresh: Vec<(String, minimal_change::Def, String)> = minimal_change::defs(new, path)
            .into_iter()
            .filter(|d| d.kind == minimal_change::DefKind::Function && !before.contains(&d.name))
            .map(|d| {
                let body = minimal_change::body_of(new, path, &d.name).unwrap_or_default();
                (path.to_string(), d, body)
            })
            .collect();
        if fresh.is_empty() {
            return Ok(());
        }
        let mut added = self.added.lock().unwrap();
        let mut pairs: Vec<String> = Vec::new();
        for (i, (_, d, body)) in fresh.iter().enumerate() {
            let earlier = added
                .iter()
                .filter(|(_, a, _)| a.name != d.name)
                .chain(fresh[..i].iter());
            if let Some((p, a, _)) = earlier
                .into_iter()
                .find(|(_, a, b)| minimal_change::twins(a, b, d, body))
            {
                let at = if p == path {
                    String::new()
                } else {
                    format!(" in `{p}`")
                };
                pairs.push(format!("`{}` like `{}`{at}", d.name, a.name));
            }
        }
        if !pairs.is_empty() {
            match text_input(input, SHARES_INPUT) {
                Some(shares) => self
                    .kept
                    .lock()
                    .unwrap()
                    .push((path.to_string(), format!("shares {shares}"))),
                None => {
                    return Err(format!(
                        "Not written: this edit adds {}, which this build already added. Put the shared logic in one helper (a parameter for what differs) and call it from both places. If they truly can't share one, call again with `{SHARES_INPUT}` naming what they share and why they stay apart.",
                        pairs.join(", ")
                    ))
                }
            }
        }
        for f in fresh {
            if !added
                .iter()
                .any(|(p, d, _)| *p == f.0 && d.name == f.1.name)
            {
                added.push(f);
            }
        }
        Ok(())
    }

    /// The coder's evidence lines: what it reuses, and each kept parallel
    /// with its reason (read by the habits check).
    pub fn evidence_lines(&self) -> String {
        let mut out = String::new();
        if let Some(r) = self.reuses.lock().unwrap().as_ref() {
            out.push_str(&format!("Reuses: {r}\n"));
        }
        for (p, why) in self.kept.lock().unwrap().iter() {
            out.push_str(&format!("Wider than the ticket: {p}: {why}\n"));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_first_write_follows_a_search_and_names_what_it_reuses() {
        let g = ExploreGuard::for_build();
        let w = json!({"path": "dags/tours.py"});
        assert!(g
            .check_first_write(&w)
            .unwrap_err()
            .contains("search before the first edit"));
        g.saw("read_file", &json!({}));
        assert!(g.check_first_write(&w).is_err());
        g.saw("bash", &json!({"command": "grep -rn OVERRIDES dags"}));
        assert!(g.check_first_write(&w).unwrap_err().contains("`reuses`"));
        let named = json!({"path": "dags/tours.py", "reuses": "`TOUR_OVERRIDES` in dags/tours.py"});
        assert!(g.check_first_write(&named).is_ok());
        // Later writes go ahead.
        assert!(g.check_first_write(&w).is_ok());
        assert!(g.evidence_lines().starts_with("Reuses: `TOUR_OVERRIDES`"));
        // A fix round asks for none of it.
        assert!(ExploreGuard::for_fix().check_first_write(&w).is_ok());
    }

    #[test]
    fn a_twin_beside_the_existing_map_needs_a_reason() {
        let g = ExploreGuard::for_fix();
        let old = "TOUR_STOP_OVERRIDES = {\n    \"a\": [\"x\"],\n}\n";
        let twin = format!("{old}TOUR_TASTING_STOP_OVERRIDES = {{\n}}\n");
        let e = g
            .check_edit("dags/tours.py", old, &twin, &json!({}))
            .unwrap_err();
        assert!(
            e.contains("`TOUR_TASTING_STOP_OVERRIDES` next to the existing `TOUR_STOP_OVERRIDES`")
        );
        // Entries in the existing map are the change it should make.
        let entries = old.replace("}\n", "    \"b\": [\"x\", \"tasting\"],\n}\n");
        assert!(g
            .check_edit("dags/tours.py", old, &entries, &json!({}))
            .is_ok());
        // A stated reason is let through and kept.
        assert!(g
            .check_edit(
                "dags/tours.py",
                old,
                &twin,
                &json!({"why_new": "a different key type"})
            )
            .is_ok());
        assert!(g
            .evidence_lines()
            .contains("Wider than the ticket: dags/tours.py: a different key type"));
        let mut schema = json!({"type": "object", "properties": {}});
        offer_fields(&mut schema);
        assert!(schema["properties"]["reuses"].is_object());
        assert!(schema["properties"]["shares"].is_object());
    }

    #[test]
    fn a_second_similar_helper_in_one_build_names_what_it_shares() {
        let g = ExploreGuard::for_fix();
        let base = "export const keyOf = (s: Item) => s.label.toLowerCase();\n";
        let first = format!("{base}export const dedupeItemsByKey = (items: Item[]): Item[] => {{\n  const seen = new Set<string>();\n  return items.filter((i) => {{\n    if (seen.has(keyOf(i))) return false;\n    seen.add(keyOf(i));\n    return true;\n  }});\n}};\n");
        assert!(g
            .check_edit("src/maps.ts", base, &first, &json!({}))
            .is_ok());
        let second = format!("{first}export const dedupeOptionsByValue = <T extends {{ value: string }}>(options: T[]): T[] => {{\n  const seen = new Set<string>();\n  return options.filter((o) => {{\n    if (seen.has(o.value)) return false;\n    seen.add(o.value);\n    return true;\n  }});\n}};\n");
        let e = g
            .check_edit("src/maps.ts", &first, &second, &json!({}))
            .unwrap_err();
        assert!(
            e.contains("`dedupeOptionsByValue` like `dedupeItemsByKey`"),
            "{e}"
        );
        // Both in one edit are caught too.
        let g2 = ExploreGuard::for_fix();
        assert!(g2
            .check_edit("src/maps.ts", base, &second, &json!({}))
            .is_err());
        // Naming what they share lets it through, kept as the reason.
        assert!(g
            .check_edit(
                "src/maps.ts",
                &first,
                &second,
                &json!({"shares": "`firstBy` in src/maps.ts; options carry no label"})
            )
            .is_ok());
        assert!(g
            .evidence_lines()
            .contains("Wider than the ticket: src/maps.ts: shares `firstBy`"));
        // An unrelated helper goes ahead.
        let other = format!("{first}export const priceLabel = (cents: number): string => {{\n  const d = Math.floor(cents / 100);\n  return `${{d}}`;\n}};\n");
        assert!(ExploreGuard::for_fix()
            .check_edit("src/maps.ts", &first, &other, &json!({}))
            .is_ok());
    }
}
