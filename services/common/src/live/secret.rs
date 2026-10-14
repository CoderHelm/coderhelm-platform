//! Seals a team's API key so the stored value is useless on its own.
//!
//! The key is encrypted by KMS with the team bound into the encryption
//! context: the stored value can only be opened by naming the same team, so one
//! team's item copied under another team's id will not decrypt.

use std::collections::HashMap;

use aws_sdk_kms::primitives::Blob;
use base64::{engine::general_purpose::STANDARD, Engine};

const PURPOSE: &str = "data-source-key";

fn context(team_id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("team".to_string(), team_id.to_string()),
        ("purpose".to_string(), PURPOSE.to_string()),
    ])
}

/// Encrypts `plaintext` for `team_id`. Returns text safe to store.
pub async fn seal(
    kms: &aws_sdk_kms::Client,
    key_id: &str,
    team_id: &str,
    plaintext: &str,
) -> Result<String, String> {
    if key_id.is_empty() {
        return Err("no secrets key configured".to_string());
    }
    let out = kms
        .encrypt()
        .key_id(key_id)
        .set_encryption_context(Some(context(team_id)))
        .plaintext(Blob::new(plaintext.as_bytes()))
        .send()
        .await
        .map_err(|e| format!("kms encrypt: {e}"))?;
    let blob = out.ciphertext_blob().ok_or("kms returned no ciphertext")?;
    Ok(STANDARD.encode(blob.as_ref()))
}

/// Opens a value sealed for `team_id`.
pub async fn open(
    kms: &aws_sdk_kms::Client,
    team_id: &str,
    sealed: &str,
) -> Result<String, String> {
    let bytes = STANDARD
        .decode(sealed)
        .map_err(|_| "stored key is not valid".to_string())?;
    let out = kms
        .decrypt()
        .set_encryption_context(Some(context(team_id)))
        .ciphertext_blob(Blob::new(bytes))
        .send()
        .await
        .map_err(|e| format!("kms decrypt: {e}"))?;
    let plain = out.plaintext().ok_or("kms returned no plaintext")?;
    String::from_utf8(plain.as_ref().to_vec()).map_err(|_| "stored key is not text".to_string())
}

/// The last four characters, for telling keys apart without revealing one.
pub fn hint(key: &str) -> String {
    let chars: Vec<char> = key.chars().collect();
    if chars.len() < 8 {
        return String::new();
    }
    chars[chars.len() - 4..].iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hint_never_reveals_a_short_key() {
        assert_eq!(hint("abcdefgh1234"), "1234");
        assert_eq!(hint("short"), "");
        assert_eq!(hint(""), "");
    }

    #[test]
    fn context_binds_the_team() {
        assert_ne!(context("TEAM#a"), context("TEAM#b"));
        assert_eq!(context("TEAM#a")["purpose"], PURPOSE);
    }
}
