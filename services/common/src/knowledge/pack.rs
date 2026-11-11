//! Search packs: a compact sketch of every passage of a big synced source,
//! so one search ranks tens of thousands of items without reading each
//! item's own index.
//!
//! Search reads the index of every file it ranks (cached per version), and
//! that stops being quick somewhere past a few thousand files: a source of
//! 50,000 items would be 50,000 reads a question. A pack keeps, for each
//! item version, each passage's sign bits (one bit a dimension of its
//! vector, 128 bytes at 1,024 dimensions) and the hashes of its words.
//! Search scores every packed passage from these (bits in common for the
//! vector list, words in common for the keyword list), keeps the best
//! [`SHORTLIST`] of each, and reads only those items' indexes, which are
//! then ranked exactly like any other passage. So a pack only chooses
//! which items get the full ranking; it never scores an answer itself.
//!
//! An item whose current version isn't in the pack (changed since the pack
//! was written, or not added yet) is read from its own index as before, so
//! a stale or missing pack makes search slower, never wrong.
//!
//! A source's pack is [`SHARDS`] objects, each item in the shard its id
//! picks, so a sync rewrites only the shards its changes touch. Sources of
//! fewer than [`PACK_FROM`] items have none.

use std::collections::{HashMap, HashSet};

use sha2::{Digest, Sha256};

use super::search::{terms, tokens, Index};

/// Shards one source's pack is split into.
pub const SHARDS: usize = 16;
/// A source gets a pack once it holds this many items.
pub const PACK_FROM: usize = 500;
/// Passages each list keeps from a pack for the full ranking.
pub const SHORTLIST: usize = 64;
/// The keyword list keeps a packed passage holding at least this share of
/// the question's words (the same floor as the full keyword list).
pub const MIN_SHARE: f32 = super::search::MIN_TERM_SHARE;

const MAGIC: &[u8; 4] = b"CHPK";
const FORMAT: u8 = 1;

/// One passage's sketch.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Passage {
    /// The sign of each dimension of its vector, 64 to a word.
    pub bits: Vec<u64>,
    /// Hashes of its words (heading included), sorted, each once.
    pub words: Vec<u16>,
}

/// One item version's sketch.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Item {
    pub file_id: String,
    /// The item's `ready_version` this was made from.
    pub version: u32,
    /// Its vectors' dimension (stored once per shard).
    pub dims: u32,
    pub passages: Vec<Passage>,
}

/// One shard of a source's pack.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Shard {
    /// The embedding model and dimension every sketch came from.
    pub model: String,
    pub dims: u32,
    pub items: Vec<Item>,
}

/// The shard an item belongs to.
pub fn shard_of(file_id: &str) -> usize {
    let h = Sha256::digest(file_id.as_bytes());
    h[0] as usize % SHARDS
}

/// Where one shard version is kept.
pub fn shard_key(team: &str, kb_id: &str, source_id: &str, shard: usize, version: u32) -> String {
    format!("{team}/knowledge/{kb_id}/_packs/{source_id}/s{shard}-v{version}.bin")
}

/// Every key of a pack, from the source's shard versions (0: empty shard).
pub fn keys(team: &str, kb_id: &str, source_id: &str, versions: &[u32]) -> Vec<String> {
    versions
        .iter()
        .enumerate()
        .filter(|(_, v)| **v > 0)
        .map(|(i, v)| shard_key(team, kb_id, source_id, i, *v))
        .collect()
}

/// The sign bits of a vector.
pub fn sign_bits(v: &[f32]) -> Vec<u64> {
    let mut out = vec![0u64; v.len().div_ceil(64)];
    for (i, x) in v.iter().enumerate() {
        if *x > 0.0 {
            out[i / 64] |= 1 << (i % 64);
        }
    }
    out
}

fn word_hash(w: &str) -> u16 {
    // FNV-1a, folded to 16 bits: a miss is a word not there; a rare
    // collision only adds a passage to the shortlist.
    let mut h: u32 = 0x811c_9dc5;
    for b in w.bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    ((h >> 16) ^ (h & 0xffff)) as u16
}

/// The hashed words of a passage.
pub fn words_of(loc: &str, text: &str) -> Vec<u16> {
    let mut w: Vec<u16> = tokens(&format!("{loc} {text}"))
        .iter()
        .map(|t| word_hash(t))
        .collect();
    w.sort_unstable();
    w.dedup();
    w
}

/// The sketch of one item version's index.
pub fn sketch(file_id: &str, version: u32, index: &Index) -> Item {
    Item {
        file_id: file_id.to_string(),
        version,
        dims: index.dims as u32,
        passages: index
            .chunks
            .iter()
            .map(|c| Passage {
                bits: sign_bits(&c.vector),
                words: words_of(&c.loc, &c.text),
            })
            .collect(),
    }
}

// ── Stored form ─────────────────────────────────────────────────────────────

fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// A shard as bytes: little endian, `CHPK`, format, then each item.
pub fn encode(s: &Shard) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(FORMAT);
    let model = s.model.as_bytes();
    put_u16(&mut out, model.len().min(u16::MAX as usize) as u16);
    out.extend_from_slice(&model[..model.len().min(u16::MAX as usize)]);
    put_u32(&mut out, s.dims);
    put_u32(&mut out, s.items.len() as u32);
    let words = (s.dims as usize).div_ceil(64);
    for it in &s.items {
        let id = it.file_id.as_bytes();
        out.push(id.len().min(255) as u8);
        out.extend_from_slice(&id[..id.len().min(255)]);
        put_u32(&mut out, it.version);
        put_u32(&mut out, it.passages.len() as u32);
        for p in &it.passages {
            for i in 0..words {
                out.extend_from_slice(&p.bits.get(i).copied().unwrap_or(0).to_le_bytes());
            }
            let n = p.words.len().min(u16::MAX as usize);
            put_u16(&mut out, n as u16);
            for w in &p.words[..n] {
                put_u16(&mut out, *w);
            }
        }
    }
    out
}

struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], String> {
        let end = self.at.checked_add(n).filter(|e| *e <= self.b.len());
        let end = end.ok_or("pack shard cut short")?;
        let s = &self.b[self.at..end];
        self.at = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, String> {
        let s = self.take(2)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }
    fn u32(&mut self) -> Result<u32, String> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }
    fn u64(&mut self) -> Result<u64, String> {
        let s = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(s);
        Ok(u64::from_le_bytes(a))
    }
}

pub fn decode(bytes: &[u8]) -> Result<Shard, String> {
    let mut r = Reader { b: bytes, at: 0 };
    if r.take(4)? != MAGIC || r.u8()? != FORMAT {
        return Err("not a pack shard".into());
    }
    let n = r.u16()? as usize;
    let model = String::from_utf8(r.take(n)?.to_vec()).map_err(|_| "pack model")?;
    let dims = r.u32()?;
    if dims > 65_536 {
        return Err("pack dimensions out of range".into());
    }
    let words = (dims as usize).div_ceil(64);
    let count = r.u32()? as usize;
    let mut items = Vec::with_capacity(count.min(100_000));
    for _ in 0..count {
        let n = r.u8()? as usize;
        let file_id = String::from_utf8(r.take(n)?.to_vec()).map_err(|_| "pack item id")?;
        let version = r.u32()?;
        let np = r.u32()? as usize;
        let mut passages = Vec::with_capacity(np.min(10_000));
        for _ in 0..np {
            let mut bits = Vec::with_capacity(words);
            for _ in 0..words {
                bits.push(r.u64()?);
            }
            let nw = r.u16()? as usize;
            let mut w = Vec::with_capacity(nw);
            for _ in 0..nw {
                w.push(r.u16()?);
            }
            passages.push(Passage { bits, words: w });
        }
        items.push(Item {
            file_id,
            version,
            dims,
            passages,
        });
    }
    Ok(Shard { model, dims, items })
}

/// About how much memory a shard takes.
pub fn shard_bytes(s: &Shard) -> usize {
    s.items
        .iter()
        .map(|i| {
            i.file_id.len()
                + 48
                + i.passages
                    .iter()
                    .map(|p| p.bits.len() * 8 + p.words.len() * 2 + 48)
                    .sum::<usize>()
        })
        .sum::<usize>()
        + 64
}

// ── Choosing what to rank ───────────────────────────────────────────────────

/// The items worth ranking in full for a question: the files of the
/// [`SHORTLIST`] passages with the most sign bits in common with the
/// question's vector, and of the [`SHORTLIST`] holding most of its words.
/// Only items `wanted` says are current (file id, version) are scored.
pub fn shortlist<'a>(
    shards: &[&'a Shard],
    query_vector: &[f32],
    query: &str,
    wanted: &dyn Fn(&str, u32) -> bool,
) -> HashSet<&'a str> {
    let qbits = sign_bits(query_vector);
    let qwords: Vec<u16> = {
        let mut w: Vec<u16> = terms(query).iter().map(|t| word_hash(t)).collect();
        w.sort_unstable();
        w.dedup();
        w
    };
    let mut by_bits: Vec<(u32, &str)> = Vec::new();
    let mut by_words: Vec<(f32, u32, &str)> = Vec::new();
    for s in shards {
        for it in &s.items {
            if !wanted(&it.file_id, it.version) {
                continue;
            }
            for p in &it.passages {
                let same: u32 = p
                    .bits
                    .iter()
                    .zip(&qbits)
                    .map(|(a, b)| (!(a ^ b)).count_ones())
                    .sum();
                by_bits.push((same, it.file_id.as_str()));
                if !qwords.is_empty() {
                    let hits = qwords
                        .iter()
                        .filter(|w| p.words.binary_search(w).is_ok())
                        .count();
                    let share = hits as f32 / qwords.len() as f32;
                    if share >= MIN_SHARE {
                        by_words.push((share, same, it.file_id.as_str()));
                    }
                }
            }
        }
    }
    let mut out: HashSet<&str> = HashSet::new();
    if by_bits.len() > SHORTLIST {
        by_bits.select_nth_unstable_by(SHORTLIST - 1, |a, b| b.0.cmp(&a.0));
        by_bits.truncate(SHORTLIST);
    }
    out.extend(by_bits.iter().map(|(_, f)| *f));
    by_words.sort_by(|a, b| b.0.total_cmp(&a.0).then(b.1.cmp(&a.1)));
    out.extend(by_words.iter().take(SHORTLIST).map(|(_, _, f)| *f));
    out
}

/// Applies a sync's changes to one shard: `fresh` are new sketches made
/// this sync, `current` every item searchable now with its version. Entries
/// for items that are gone or changed are dropped, so the shard never names
/// a version search can't read. Returns whether anything changed.
pub fn apply(
    shard: &mut Shard,
    fresh: &HashMap<String, Item>,
    current: &HashMap<&str, u32>,
    shard_no: usize,
) -> bool {
    let before = shard.items.len();
    let mut changed = false;
    shard.items.retain(|it| {
        current.get(it.file_id.as_str()) == Some(&it.version) && !fresh.contains_key(&it.file_id)
    });
    if shard.items.len() != before {
        changed = true;
    }
    if shard.items.is_empty() {
        if let Some(d) = fresh.values().map(|i| i.dims).find(|d| *d > 0) {
            shard.dims = d;
        }
    }
    for (id, it) in fresh {
        if shard_of(id) == shard_no
            && it.dims == shard.dims
            && current.get(id.as_str()) == Some(&it.version)
        {
            shard.items.push(it.clone());
            changed = true;
        }
    }
    shard.items.sort_by(|a, b| a.file_id.cmp(&b.file_id));
    changed
}

#[cfg(test)]
mod tests {
    use super::super::search::IndexedChunk;
    use super::*;

    fn index(texts: &[(&str, Vec<f32>)]) -> Index {
        Index::build(
            "m",
            4,
            texts
                .iter()
                .enumerate()
                .map(|(i, (t, v))| IndexedChunk {
                    ord: i as u32,
                    loc: "Intro".into(),
                    text: t.to_string(),
                    vector: v.clone(),
                })
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn shards_round_trip_and_refuse_garbage() {
        let s = Shard {
            model: "m".into(),
            dims: 4,
            items: vec![sketch(
                "f1",
                3,
                &index(&[("Deploys run on Fridays", vec![0.5, -0.1, 0.2, -0.9])]),
            )],
        };
        let back = decode(&encode(&s)).unwrap();
        assert_eq!(back, s);
        assert_eq!(back.items[0].passages[0].bits, vec![0b0101]);
        assert!(decode(b"nope").is_err());
        let mut cut = encode(&s);
        cut.truncate(cut.len() - 3);
        assert!(decode(&cut).is_err());
        assert!(shard_bytes(&s) > 0);
        assert!(shard_of("f1") < SHARDS);
        assert_eq!(
            keys("T", "kb", "s", &[0, 2]),
            vec![shard_key("T", "kb", "s", 1, 2)]
        );
        assert!(crate::knowledge::owns_key(
            "T",
            &shard_key("T", "kb", "s", 1, 2)
        ));
    }

    #[test]
    fn the_shortlist_finds_close_vectors_and_shared_words_and_skips_stale_items() {
        let near = sketch(
            "near",
            1,
            &index(&[("unrelated words here", vec![1.0, 1.0, 1.0, 1.0])]),
        );
        let worded = sketch(
            "worded",
            1,
            &index(&[("rollback ticket ERR-4021", vec![-1.0, -1.0, -1.0, -1.0])]),
        );
        let stale = sketch(
            "stale",
            1,
            &index(&[("rollback ticket ERR-4021", vec![1.0, 1.0, 1.0, 1.0])]),
        );
        let s = Shard {
            model: "m".into(),
            dims: 4,
            items: vec![near, worded, stale],
        };
        let wanted = |id: &str, v: u32| id != "stale" && v == 1;
        let got = shortlist(&[&s], &[1.0, 1.0, 1.0, 1.0], "rollback ERR-4021", &wanted);
        assert!(got.contains("near"), "{got:?}");
        assert!(got.contains("worded"), "{got:?}");
        assert!(!got.contains("stale"));
    }

    #[test]
    fn the_shortlist_is_bounded() {
        let items: Vec<Item> = (0..500)
            .map(|i| {
                sketch(
                    &format!("f{i}"),
                    1,
                    &index(&[("same text", vec![1.0, 0.5, -0.5, 1.0])]),
                )
            })
            .collect();
        let s = Shard {
            model: "m".into(),
            dims: 4,
            items,
        };
        let got = shortlist(&[&s], &[1.0, 0.5, -0.5, 1.0], "same text", &|_, _| true);
        assert!(got.len() <= 2 * SHORTLIST, "{}", got.len());
    }

    #[test]
    fn applying_changes_drops_gone_and_stale_items_and_adds_fresh_ones() {
        let ix = index(&[("text", vec![1.0, 0.0, 0.0, 0.0])]);
        // Find ids in the same shard.
        let ids: Vec<String> = (0..200)
            .map(|i| format!("i{i}"))
            .filter(|id| shard_of(id) == 0)
            .take(3)
            .collect();
        let mut s = Shard {
            model: "m".into(),
            dims: 4,
            items: vec![sketch(&ids[0], 1, &ix), sketch(&ids[1], 1, &ix)],
        };
        let current: HashMap<&str, u32> =
            HashMap::from([(ids[0].as_str(), 2), (ids[2].as_str(), 1)]);
        let fresh = HashMap::from([
            (ids[0].clone(), sketch(&ids[0], 2, &ix)),
            (ids[2].clone(), sketch(&ids[2], 1, &ix)),
        ]);
        assert!(apply(&mut s, &fresh, &current, 0));
        let got: Vec<(&str, u32)> = s
            .items
            .iter()
            .map(|i| (i.file_id.as_str(), i.version))
            .collect();
        let mut want = vec![(ids[0].as_str(), 2), (ids[2].as_str(), 1)];
        want.sort();
        assert_eq!(got, want);
        // Nothing new: unchanged.
        assert!(!apply(&mut s, &HashMap::new(), &current, 0));
    }
}
