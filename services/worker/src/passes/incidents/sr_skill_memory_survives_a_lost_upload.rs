//! sr. A scheduled skill keeps its memory (last week's snapshot) in its
//! output folder, which rides into the sandbox with each script and comes
//! back whole after it. A script whose upload failed came back with no
//! files at all; taken as the whole folder, it dropped the restored
//! snapshot, the run kept no memory, and the next week ran as a first run
//! with no week-over-week comparison.
//!
//! Guarantee: an answer is the whole output folder only when the script ran,
//! nothing was dropped, and it didn't come back empty where there were
//! files; anything else is merged into the folder, never put in its place.
//! The memory folder rides inside the skill's folder at
//! `.coderhelm-out/`, so every script sees it at `$CODERHELM_OUT`.

use std::collections::BTreeMap;

use crate::passes::skill_schedule::{pack, whole};

#[test]
fn a_lost_upload_never_wipes_the_memory_folder() {
    let restored = BTreeMap::from([(
        "snapshots/2026-11-23.json".to_string(),
        "{\"counts\":{}}".to_string(),
    )]);
    // The script ran, but nothing came back (the upload failed).
    assert!(!whole(true, 0, 0, restored.len()));
    // A build that never ran, or one that dropped files, is merged too.
    assert!(!whole(false, 0, 2, restored.len()));
    assert!(!whole(true, 1, 2, restored.len()));
    // A script that ran and brought the folder back replaces it.
    assert!(whole(true, 0, 2, restored.len()));

    // The memory rides where the script looks for it.
    let bytes = pack(&[("count_visits.py".into(), b"print(1)".to_vec())], &restored).unwrap();
    let mut ar = tar::Archive::new(flate2::read::GzDecoder::new(&bytes[..]));
    let paths: Vec<String> = ar
        .entries()
        .unwrap()
        .map(|e| e.unwrap().path().unwrap().to_string_lossy().to_string())
        .collect();
    assert!(paths.contains(&"skill/.coderhelm-out/snapshots/2026-11-23.json".to_string()));
}
