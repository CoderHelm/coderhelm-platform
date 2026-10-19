//! Sealed credentials for a team's MCP servers (Figma, Sentry, Sanity, ...).
//!
//! The values a team saves for an MCP server are encrypted by KMS before they
//! are stored, with the team and the server bound into the encryption
//! context: the stored value opens only for that team and that server, so an
//! item copied under another team or another server will not decrypt. Values
//! are opened at the moment a call needs them and are never returned by an API
//! or written to a log.
//!
//! Records written before sealing hold the values as plain JSON in
//! `credentials`. Reading one seals it in place ([`migrate_item`]). Every
//! production record has been sealed; the read path stays as a safety net.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_kms::primitives::Blob;
use base64::{engine::general_purpose::STANDARD, Engine};

/// Where sealed values are stored on a `PLUGIN#<id>` record.
pub const SEALED_ATTR: &str = "credentials_sealed";
/// Where records written before sealing kept the values, as plain JSON.
pub const PLAIN_ATTR: &str = "credentials";
const PREFIX: &str = "PLUGIN#";
const PURPOSE: &str = "mcp-credentials";
/// KMS encrypts at most 4 KiB directly.
pub const MAX_PLAINTEXT_BYTES: usize = 4096;

type Item = HashMap<String, AttributeValue>;
pub type Credentials = HashMap<String, String>;

/// The encryption context: the team, the server and what the value is for.
pub fn context(team_id: &str, plugin_id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("team".to_string(), team_id.to_string()),
        ("plugin".to_string(), plugin_id.to_string()),
        ("purpose".to_string(), PURPOSE.to_string()),
    ])
}

/// Encrypts and decrypts under an encryption context. KMS in production; a
/// fake in tests.
#[async_trait::async_trait]
pub trait Cipher: Send + Sync {
    async fn encrypt(
        &self,
        context: HashMap<String, String>,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, String>;
    async fn decrypt(
        &self,
        context: HashMap<String, String>,
        blob: &[u8],
    ) -> Result<Vec<u8>, String>;
}

/// KMS with the secrets key. `key_id` is needed only to encrypt.
pub struct Kms<'a> {
    pub client: &'a aws_sdk_kms::Client,
    pub key_id: &'a str,
}

#[async_trait::async_trait]
impl Cipher for Kms<'_> {
    async fn encrypt(
        &self,
        context: HashMap<String, String>,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, String> {
        if self.key_id.is_empty() {
            return Err("no secrets key configured".to_string());
        }
        let out = self
            .client
            .encrypt()
            .key_id(self.key_id)
            .set_encryption_context(Some(context))
            .plaintext(Blob::new(plaintext))
            .send()
            .await
            .map_err(|e| format!("kms encrypt: {e}"))?;
        let blob = out.ciphertext_blob().ok_or("kms returned no ciphertext")?;
        Ok(blob.as_ref().to_vec())
    }

    async fn decrypt(
        &self,
        context: HashMap<String, String>,
        blob: &[u8],
    ) -> Result<Vec<u8>, String> {
        let out = self
            .client
            .decrypt()
            .set_encryption_context(Some(context))
            .ciphertext_blob(Blob::new(blob))
            .send()
            .await
            .map_err(|e| format!("kms decrypt: {e}"))?;
        let plain = out.plaintext().ok_or("kms returned no plaintext")?;
        Ok(plain.as_ref().to_vec())
    }
}

/// Encrypts `creds` for one team's server. Returns text safe to store.
pub async fn seal(
    cipher: &dyn Cipher,
    team_id: &str,
    plugin_id: &str,
    creds: &Credentials,
) -> Result<String, String> {
    let json = serde_json::to_vec(creds).map_err(|_| "credentials are not valid".to_string())?;
    if json.len() > MAX_PLAINTEXT_BYTES {
        return Err("credentials are too long".to_string());
    }
    let blob = cipher.encrypt(context(team_id, plugin_id), &json).await?;
    Ok(STANDARD.encode(blob))
}

/// Opens values sealed for one team's server.
pub async fn open(
    cipher: &dyn Cipher,
    team_id: &str,
    plugin_id: &str,
    sealed: &str,
) -> Result<Credentials, String> {
    let bytes = STANDARD
        .decode(sealed)
        .map_err(|_| "stored credentials are not valid".to_string())?;
    let plain = cipher.decrypt(context(team_id, plugin_id), &bytes).await?;
    serde_json::from_slice(&plain).map_err(|_| "stored credentials are not valid".to_string())
}

/// What a record holds.
#[derive(Clone, PartialEq, Eq)]
pub enum Stored {
    Sealed(String),
    /// Written before sealing: the values as plain JSON.
    Plain(String),
    None,
}

impl std::fmt::Debug for Stored {
    // Never prints a value.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Stored::Sealed(_) => "Sealed(..)",
            Stored::Plain(_) => "Plain(..)",
            Stored::None => "None",
        })
    }
}

fn s<'a>(item: &'a Item, k: &str) -> Option<&'a str> {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .map(|v| v.as_str())
        .filter(|v| !v.is_empty())
}

/// What `item` holds. Sealed wins when a record somehow holds both.
pub fn stored(item: &Item) -> Stored {
    if let Some(v) = s(item, SEALED_ATTR) {
        return Stored::Sealed(v.to_string());
    }
    if let Some(v) = s(item, PLAIN_ATTR) {
        return Stored::Plain(v.to_string());
    }
    Stored::None
}

/// The server id of a `PLUGIN#<id>` record.
pub fn plugin_id(item: &Item) -> Option<&str> {
    s(item, "sk")?.strip_prefix(PREFIX)
}

/// The values a record holds, opened. A record with none gives an empty map.
pub async fn load(
    cipher: &dyn Cipher,
    team_id: &str,
    plugin_id: &str,
    stored: &Stored,
) -> Result<Credentials, String> {
    match stored {
        Stored::Sealed(v) => open(cipher, team_id, plugin_id, v).await,
        Stored::Plain(v) => {
            serde_json::from_str(v).map_err(|_| "stored credentials are not valid".to_string())
        }
        Stored::None => Ok(Credentials::new()),
    }
}

/// How sealing a plain record changes it: the sealed value to set, under the
/// condition that the plain value is still the one read.
#[derive(Clone, PartialEq, Eq)]
pub struct Rewrite {
    pub team_id: String,
    pub plugin_id: String,
    pub old_plain: String,
    pub sealed: String,
}

/// Seals a record that still holds plain values. `None` when there is nothing
/// to do (already sealed, no values, or not a plugin record).
pub async fn plan_rewrite(cipher: &dyn Cipher, item: &Item) -> Result<Option<Rewrite>, String> {
    let Stored::Plain(old) = stored(item) else {
        return Ok(None);
    };
    let (Some(team), Some(plugin)) = (s(item, "pk"), plugin_id(item)) else {
        return Ok(None);
    };
    let creds: Credentials =
        serde_json::from_str(&old).map_err(|_| "stored credentials are not valid".to_string())?;
    let sealed = seal(cipher, team, plugin, &creds).await?;
    Ok(Some(Rewrite {
        team_id: team.to_string(),
        plugin_id: plugin.to_string(),
        old_plain: old,
        sealed,
    }))
}

/// The record after `rewrite`: the sealed value in, the plain value gone.
pub fn apply(item: &Item, rewrite: &Rewrite) -> Item {
    let mut out = item.clone();
    out.remove(PLAIN_ATTR);
    out.insert(
        SEALED_ATTR.to_string(),
        AttributeValue::S(rewrite.sealed.clone()),
    );
    out
}

/// Writes `rewrite`, only if the record still holds the plain value read.
/// `Ok(false)` when the record changed in between (someone saved new values).
pub async fn write_rewrite(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    rewrite: &Rewrite,
) -> Result<bool, String> {
    let res = dynamo
        .update_item()
        .table_name(table)
        .key("pk", AttributeValue::S(rewrite.team_id.clone()))
        .key(
            "sk",
            AttributeValue::S(format!("{PREFIX}{}", rewrite.plugin_id)),
        )
        .update_expression("SET #sealed = :sealed REMOVE #plain")
        .condition_expression("#plain = :old")
        .expression_attribute_names("#sealed", SEALED_ATTR)
        .expression_attribute_names("#plain", PLAIN_ATTR)
        .expression_attribute_values(":sealed", AttributeValue::S(rewrite.sealed.clone()))
        .expression_attribute_values(":old", AttributeValue::S(rewrite.old_plain.clone()))
        .send()
        .await;
    match res {
        Ok(_) => Ok(true),
        Err(e) => {
            let lost_race = e
                .as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception());
            if lost_race {
                Ok(false)
            } else {
                Err(format!("dynamodb update: {}", e.into_service_error()))
            }
        }
    }
}

/// Seals one record read with plain values, in place. Errors are reported,
/// never fatal to the caller: the plain values it read are still usable.
pub async fn migrate_item(
    dynamo: &aws_sdk_dynamodb::Client,
    cipher: &dyn Cipher,
    table: &str,
    item: &Item,
) -> Result<bool, String> {
    match plan_rewrite(cipher, item).await? {
        Some(rw) => write_rewrite(dynamo, table, &rw).await,
        None => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stands in for KMS: the blob carries the context it was sealed under,
    /// and opening under any other context fails, as KMS does.
    struct Fake;

    fn ctx_key(c: &HashMap<String, String>) -> String {
        let mut v: Vec<_> = c.iter().map(|(k, v)| format!("{k}={v}")).collect();
        v.sort();
        v.join("&")
    }

    #[async_trait::async_trait]
    impl Cipher for Fake {
        async fn encrypt(
            &self,
            context: HashMap<String, String>,
            plaintext: &[u8],
        ) -> Result<Vec<u8>, String> {
            let mut out = ctx_key(&context).into_bytes();
            out.push(0);
            out.extend(plaintext.iter().map(|b| b ^ 0x5a));
            Ok(out)
        }
        async fn decrypt(
            &self,
            context: HashMap<String, String>,
            blob: &[u8],
        ) -> Result<Vec<u8>, String> {
            let at = blob.iter().position(|b| *b == 0).ok_or("bad blob")?;
            if blob[..at] != *ctx_key(&context).as_bytes() {
                return Err("kms decrypt: InvalidCiphertextException".into());
            }
            Ok(blob[at + 1..].iter().map(|b| b ^ 0x5a).collect())
        }
    }

    fn creds() -> Credentials {
        HashMap::from([("api_token".to_string(), "figd_secret_value".to_string())])
    }

    fn plain_item(team: &str, plugin: &str) -> Item {
        HashMap::from([
            ("pk".to_string(), AttributeValue::S(team.into())),
            (
                "sk".to_string(),
                AttributeValue::S(format!("PLUGIN#{plugin}")),
            ),
            ("enabled".to_string(), AttributeValue::Bool(true)),
            (
                PLAIN_ATTR.to_string(),
                AttributeValue::S(serde_json::to_string(&creds()).unwrap()),
            ),
        ])
    }

    #[tokio::test]
    async fn seal_then_open_gives_the_values_back() {
        let sealed = seal(&Fake, "TEAM#a", "figma", &creds()).await.unwrap();
        assert!(!sealed.contains("figd_secret_value"));
        let opened = open(&Fake, "TEAM#a", "figma", &sealed).await.unwrap();
        assert_eq!(opened, creds());
    }

    #[tokio::test]
    async fn another_team_or_server_cannot_open_it() {
        let sealed = seal(&Fake, "TEAM#a", "figma", &creds()).await.unwrap();
        assert!(open(&Fake, "TEAM#b", "figma", &sealed).await.is_err());
        assert!(open(&Fake, "TEAM#a", "sentry", &sealed).await.is_err());
    }

    #[test]
    fn context_binds_team_and_server() {
        assert_ne!(context("TEAM#a", "figma"), context("TEAM#b", "figma"));
        assert_ne!(context("TEAM#a", "figma"), context("TEAM#a", "sentry"));
        assert_eq!(context("TEAM#a", "figma")["purpose"], PURPOSE);
    }

    #[tokio::test]
    async fn too_long_is_refused() {
        let big = HashMap::from([("k".to_string(), "x".repeat(MAX_PLAINTEXT_BYTES))]);
        assert!(seal(&Fake, "TEAM#a", "figma", &big).await.is_err());
    }

    #[tokio::test]
    async fn a_plain_record_is_sealed_once_and_then_left_alone() {
        let item = plain_item("TEAM#a", "figma");
        let rw = plan_rewrite(&Fake, &item).await.unwrap().expect("plain");
        assert_eq!(rw.team_id, "TEAM#a");
        assert_eq!(rw.plugin_id, "figma");
        let after = apply(&item, &rw);
        assert!(!after.contains_key(PLAIN_ATTR));
        assert!(matches!(stored(&after), Stored::Sealed(_)));
        // The sealed record still opens to the same values.
        let opened = load(&Fake, "TEAM#a", "figma", &stored(&after))
            .await
            .unwrap();
        assert_eq!(opened, creds());
        // A second pass has nothing to do.
        assert!(plan_rewrite(&Fake, &after).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn plain_values_still_load_before_the_rewrite() {
        let item = plain_item("TEAM#a", "figma");
        let got = load(&Fake, "TEAM#a", "figma", &stored(&item))
            .await
            .unwrap();
        assert_eq!(got, creds());
        assert!(load(&Fake, "TEAM#a", "figma", &Stored::None)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn records_without_values_are_not_rewritten() {
        let mut item = plain_item("TEAM#a", "figma");
        item.remove(PLAIN_ATTR);
        assert!(plan_rewrite(&Fake, &item).await.unwrap().is_none());
        let mut other = plain_item("TEAM#a", "figma");
        other.insert("sk".into(), AttributeValue::S("CONNECTOR#newrelic".into()));
        assert!(plan_rewrite(&Fake, &other).await.unwrap().is_none());
    }

    #[test]
    fn debug_never_prints_a_value() {
        let p = Stored::Plain("figd_secret_value".into());
        assert!(!format!("{p:?}").contains("figd"));
    }
}
