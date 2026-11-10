//! Seals a team's API key so the stored value is useless on its own.
//!
//! The key is encrypted by KMS with the team bound into the encryption
//! context: the stored value can only be opened by naming the same team, so one
//! team's item copied under another team's id will not decrypt.

use std::collections::HashMap;

use base64::{engine::general_purpose::STANDARD, Engine};

use crate::mcp_credentials::{Cipher, Kms};

const PURPOSE: &str = "data-source-key";

fn context(team_id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("team".to_string(), team_id.to_string()),
        ("purpose".to_string(), PURPOSE.to_string()),
    ])
}

/// Encrypts `plaintext` for `team_id`. Returns text safe to store. A key
/// larger than KMS encrypts directly is sealed in an envelope
/// ([`crate::envelope`]).
pub async fn seal(
    kms: &aws_sdk_kms::Client,
    key_id: &str,
    team_id: &str,
    plaintext: &str,
) -> Result<String, String> {
    if let Some(e) = crate::envelope::too_large("The key", plaintext.len()) {
        return Err(e);
    }
    let cipher = Kms {
        client: kms,
        key_id,
    };
    let blob = cipher
        .encrypt(context(team_id), plaintext.as_bytes())
        .await?;
    Ok(STANDARD.encode(blob))
}

/// Opens a value sealed for `team_id` (directly or in an envelope).
pub async fn open(
    kms: &aws_sdk_kms::Client,
    team_id: &str,
    sealed: &str,
) -> Result<String, String> {
    let bytes = STANDARD
        .decode(sealed)
        .map_err(|_| "stored key is not valid".to_string())?;
    let cipher = Kms {
        client: kms,
        key_id: "",
    };
    let plain = cipher.decrypt(context(team_id), &bytes).await?;
    String::from_utf8(plain).map_err(|_| "stored key is not text".to_string())
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
