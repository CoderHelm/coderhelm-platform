//! What an EC2 instance type runs on: its architecture and CPU generation,
//! and whether that CPU has the features an advisory's flaw needs.
//!
//! The family is the part of the type before the dot (`m7i-flex` in
//! `m7i-flex.large`). Graviton families carry a `g` after the generation
//! (`m6g`, `c7gn`, `t4g`, `r8gd`). The x86 families are listed by their
//! processors as AWS documents them; a family not listed is "not known",
//! never guessed.

use serde::{Deserialize, Serialize};

use super::advisories::Hardware;

/// What runs an instance.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Cpu {
    /// `x86_64`, `arm64`, or empty when not known.
    pub arch: String,
    /// `Intel Ice Lake`, `AMD Zen 4`, `AWS Graviton`; empty when not known.
    pub generation: String,
    /// AVX-512 VBMI: `None` when the family is not known.
    pub avx512_vbmi: Option<bool>,
    /// Any AVX-512.
    pub avx512: Option<bool>,
}

/// x86 families with AVX-512 VBMI: Intel Ice Lake and newer, AMD Zen 4
/// and newer.
const VBMI: &[(&str, &[&str])] = &[
    (
        "Intel Ice Lake",
        &[
            "m6i", "m6id", "m6in", "m6idn", "c6i", "c6id", "c6in", "r6i", "r6id", "r6in", "r6idn",
            "i4i", "x2idn", "x2iedn", "trn1", "trn1n",
        ],
    ),
    (
        "Intel Sapphire Rapids",
        &[
            "m7i", "m7i-flex", "c7i", "c7i-flex", "r7i", "r7iz", "i7ie", "i7i", "u7i", "u7in",
            "u7inh",
        ],
    ),
    (
        "Intel Granite Rapids",
        &[
            "m8i", "m8i-flex", "c8i", "c8i-flex", "r8i", "r8i-flex", "x8i", "i8i",
        ],
    ),
    ("AMD Zen 4", &["m7a", "c7a", "r7a", "hpc7a"]),
    ("AMD Zen 5", &["m8a", "c8a", "r8a", "hpc8a"]),
];

/// x86 families without AVX-512 VBMI.
const NO_VBMI: &[(&str, bool, &[&str])] = &[
    // (generation, has some AVX-512, families)
    (
        "Intel Skylake or Cascade Lake",
        true,
        &[
            "t3", "m5", "m5d", "m5n", "m5dn", "m5zn", "c5", "c5d", "c5n", "r5", "r5d", "r5n",
            "r5dn", "r5b", "z1d", "x2iezn", "i3en", "d3", "d3en", "g4dn", "inf1", "p3dn",
        ],
    ),
    (
        "Intel Broadwell or older",
        false,
        &[
            "t2", "m4", "c4", "r4", "x1", "x1e", "i3", "d2", "h1", "p2", "p3", "g3", "m3", "c3",
            "r3", "t1", "m1", "m2", "c1",
        ],
    ),
    (
        "AMD Zen 1 to Zen 3",
        false,
        &[
            "t3a", "m5a", "c5a", "c5ad", "r5a", "r5ad", "m6a", "c6a", "r6a", "hpc6a", "g4ad",
        ],
    ),
];

/// The family of an instance type (`m7i-flex` in `m7i-flex.large`).
pub fn family(instance_type: &str) -> String {
    instance_type
        .split('.')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

fn graviton(f: &str) -> bool {
    if f == "a1" {
        return true;
    }
    // A letter or letters, a generation digit, then `g`.
    let letters = f.chars().take_while(|c| c.is_ascii_lowercase()).count();
    let rest = &f[letters..];
    let digits = rest.chars().take_while(|c| c.is_ascii_digit()).count();
    letters > 0 && digits > 0 && rest[digits..].starts_with('g')
}

/// What an instance type runs on. `arch` is what AWS reported for the
/// instance, when known (`DescribeInstances`' architecture).
pub fn cpu_of(instance_type: &str, arch: &str) -> Cpu {
    let f = family(instance_type);
    if let Some((generation, _)) = VBMI.iter().find(|(_, fs)| fs.contains(&f.as_str())) {
        return Cpu {
            arch: "x86_64".into(),
            generation: generation.to_string(),
            avx512_vbmi: Some(true),
            avx512: Some(true),
        };
    }
    if !f.is_empty() && graviton(&f) {
        return Cpu {
            arch: "arm64".into(),
            generation: "AWS Graviton".into(),
            avx512_vbmi: Some(false),
            avx512: Some(false),
        };
    }
    if let Some((generation, any, _)) = NO_VBMI.iter().find(|(_, _, fs)| fs.contains(&f.as_str())) {
        return Cpu {
            arch: "x86_64".into(),
            generation: generation.to_string(),
            avx512_vbmi: Some(false),
            avx512: Some(*any),
        };
    }
    let arch = match arch {
        "arm64" | "aarch64" => "arm64",
        "x86_64" | "amd64" => "x86_64",
        _ => "",
    };
    Cpu {
        arch: arch.into(),
        avx512_vbmi: (arch == "arm64").then_some(false),
        avx512: (arch == "arm64").then_some(false),
        ..Default::default()
    }
}

/// Whether an advisory's hardware condition holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Holds {
    Yes,
    No,
    #[default]
    Unknown,
}

/// Whether `h` holds on `cpu`, and why, in a line.
pub fn holds(h: &Hardware, cpu: &Cpu, what: &str) -> (Holds, String) {
    if h.is_empty() {
        return (Holds::Yes, String::new());
    }
    let named = if cpu.generation.is_empty() {
        what.to_string()
    } else {
        format!("{what} ({}, {})", cpu.generation, cpu.arch)
    };
    if !cpu.arch.is_empty() && h.not_arch.contains(&cpu.arch) {
        return (
            Holds::No,
            format!(
                "{named} is {}; the advisory says {} is not affected",
                cpu.arch, cpu.arch
            ),
        );
    }
    if !cpu.arch.is_empty() && !h.arch.is_empty() && !h.arch.contains(&cpu.arch) {
        return (
            Holds::No,
            format!(
                "{named} is {}; the flaw needs {}",
                cpu.arch,
                h.arch.join(" or ")
            ),
        );
    }
    let feature = match h.cpu_feature.as_str() {
        "avx512_vbmi" => cpu.avx512_vbmi,
        "avx512" => cpu.avx512,
        _ => None,
    };
    let feature_name = match h.cpu_feature.as_str() {
        "avx512_vbmi" => "AVX-512 VBMI",
        "avx512" => "AVX-512",
        _ => "",
    };
    match (feature_name.is_empty(), feature) {
        (true, _) if !cpu.arch.is_empty() => (
            Holds::Yes,
            format!("{named} is {}, the architecture the flaw needs", cpu.arch),
        ),
        (false, Some(true)) => (
            Holds::Yes,
            format!("{named} has {feature_name}, which the flaw needs"),
        ),
        (false, Some(false)) => (
            Holds::No,
            format!("{named} has no {feature_name}, which the flaw needs"),
        ),
        _ => (
            Holds::Unknown,
            if feature_name.is_empty() {
                format!("CoderHelm does not know what CPU {what} runs on")
            } else {
                format!("CoderHelm does not know whether {what} has {feature_name}, which the flaw needs")
            },
        ),
    }
}

/// The instance families where a `feature` condition holds, for the line
/// that says what moving there means.
pub fn families_with(feature: &str) -> String {
    match feature {
        "avx512_vbmi" => {
            "Intel Ice Lake and newer (m6i, c6i, r6i, m7i, c7i, r7i, m8i ...) and AMD Zen 4 and newer (m7a, c7a, r7a, m8a ...)".into()
        }
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vbmi() -> Hardware {
        Hardware {
            arch: vec!["x86_64".into()],
            not_arch: vec!["arm64".into()],
            cpu_feature: "avx512_vbmi".into(),
            words: String::new(),
        }
    }

    #[test]
    fn instance_types_name_their_cpu() {
        let c = cpu_of("m7i-flex.large", "");
        assert_eq!((c.arch.as_str(), c.avx512_vbmi), ("x86_64", Some(true)));
        assert_eq!(cpu_of("c7a.2xlarge", "").generation, "AMD Zen 4");
        assert_eq!(cpu_of("m6g.large", "").arch, "arm64");
        assert_eq!(cpu_of("c7gn.xlarge", "").arch, "arm64");
        assert_eq!(cpu_of("t4g.micro", "").arch, "arm64");
        assert_eq!(cpu_of("m5.large", "").avx512_vbmi, Some(false));
        assert_eq!(cpu_of("m6a.large", "").avx512, Some(false));
        let unknown = cpu_of("zz9.large", "x86_64");
        assert_eq!(
            (unknown.arch.as_str(), unknown.avx512_vbmi),
            ("x86_64", None)
        );
        assert_eq!(cpu_of("zz9.large", "arm64").avx512_vbmi, Some(false));
    }

    #[test]
    fn the_hardware_condition_holds_only_on_vbmi_x86() {
        let h = vbmi();
        assert_eq!(
            holds(&h, &cpu_of("m7i.large", ""), "m7i.large").0,
            Holds::Yes
        );
        assert_eq!(
            holds(&h, &cpu_of("m6i.large", ""), "m6i.large").0,
            Holds::Yes
        );
        let (no, why) = holds(&h, &cpu_of("m6g.large", ""), "m6g.large");
        assert_eq!(no, Holds::No);
        assert!(why.contains("arm64"), "{why}");
        let (no, why) = holds(&h, &cpu_of("m5.large", ""), "m5.large");
        assert_eq!(no, Holds::No);
        assert!(why.contains("no AVX-512 VBMI"), "{why}");
        assert_eq!(
            holds(&h, &cpu_of("zz9.large", "x86_64"), "zz9.large").0,
            Holds::Unknown
        );
        // No condition: it holds everywhere.
        assert_eq!(
            holds(&Hardware::default(), &Cpu::default(), "x").0,
            Holds::Yes
        );
    }
}
