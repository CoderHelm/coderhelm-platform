//! Envelope encryption for sealed values larger than KMS takes directly.
//!
//! KMS `Encrypt` takes at most 4,096 bytes of plaintext. Some values are
//! bigger: an Atlassian sign-in's token set (a large JWT access token plus a
//! rotating refresh token) is often 5 to 8 KB of JSON. Those are sealed with
//! an envelope: KMS `GenerateDataKey` gives a fresh AES-256 key (in the clear
//! and encrypted under the secrets key, with the same encryption context),
//! the value is encrypted here with AES-256-GCM, and only the encrypted key
//! is stored beside it. Opening asks KMS `Decrypt` for the key, so the
//! encryption context (team, server, purpose, owner) is enforced by KMS
//! exactly as for a direct blob, and it is bound into the GCM tag as well.
//!
//! Values up to [`KMS_DIRECT_MAX`] keep KMS `Encrypt` directly, byte for byte
//! the format every reader already opens (the log analyzer opens webhook URLs
//! with KMS directly, in Python). Larger values, up to [`MAX_SEALED_BYTES`],
//! are enveloped. [`open`] tells the two apart by [`MAGIC`]: a KMS ciphertext
//! blob starts with its format version byte (`0x01`), never with `C`, so
//! every blob written before envelopes still opens, with no migration.
//!
//! Stored format (version 1), then base64 by the caller:
//!
//! ```text
//! "CHE1" | u16 BE length of the encrypted key | encrypted key | 12-byte nonce | ciphertext + 16-byte tag
//! ```

use std::collections::HashMap;

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};
use zeroize::Zeroize;

/// KMS `Encrypt` refuses more plaintext than this.
pub const KMS_DIRECT_MAX: usize = 4096;
/// The most one sealed value may hold. Sealed values live in DynamoDB items,
/// which may be 400 KB in all (names and values); 64 KiB of plaintext is
/// about 88 KB once enveloped and base64 encoded, leaving room for the rest
/// of the item.
pub const MAX_SEALED_BYTES: usize = 64 * 1024;
/// Marks an enveloped blob (format version 1).
pub const MAGIC: &[u8; 4] = b"CHE1";
const KEY_LEN: usize = 32;
const TAG_LEN: usize = 16;

/// A data key from KMS `GenerateDataKey`: the key itself (wiped when
/// dropped) and the same key encrypted under the secrets key.
pub struct DataKey {
    pub plaintext: Vec<u8>,
    pub encrypted: Vec<u8>,
}

impl Drop for DataKey {
    fn drop(&mut self) {
        self.plaintext.zeroize();
    }
}

impl std::fmt::Debug for DataKey {
    // Never prints the key.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DataKey(..)")
    }
}

/// The KMS calls a sealed value needs. KMS in production; a fake in tests.
#[async_trait::async_trait]
pub trait Keys: Send + Sync {
    /// KMS `Encrypt` (at most [`KMS_DIRECT_MAX`] bytes).
    async fn encrypt_direct(
        &self,
        context: HashMap<String, String>,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, String>;
    /// KMS `GenerateDataKey` (AES-256) under `context`.
    async fn generate_data_key(&self, context: HashMap<String, String>) -> Result<DataKey, String>;
    /// KMS `Decrypt` under `context`: a direct blob or an encrypted data key.
    async fn decrypt_direct(
        &self,
        context: HashMap<String, String>,
        blob: &[u8],
    ) -> Result<Vec<u8>, String>;
}

/// Why a value is over [`MAX_SEALED_BYTES`], in words, or `None` when it fits.
/// `what` names the value ("Confluence's sign-in tokens").
pub fn too_large(what: &str, len: usize) -> Option<String> {
    (len > MAX_SEALED_BYTES).then(|| {
        format!(
            "{what} came to {} KB, over the {} KB Coderhelm can store for one value, so nothing was saved.",
            len.div_ceil(1024),
            MAX_SEALED_BYTES / 1024
        )
    })
}

/// The encryption context in one fixed order, for the GCM associated data.
fn aad(context: &HashMap<String, String>) -> Vec<u8> {
    let mut pairs: Vec<_> = context.iter().collect();
    pairs.sort();
    let mut out = MAGIC.to_vec();
    for (k, v) in pairs {
        for part in [k, v] {
            out.extend((part.len() as u32).to_be_bytes());
            out.extend(part.as_bytes());
        }
    }
    out
}

fn gcm_key(raw: &[u8]) -> Result<LessSafeKey, String> {
    if raw.len() != KEY_LEN {
        return Err("kms returned a data key of the wrong size".into());
    }
    let k = UnboundKey::new(&AES_256_GCM, raw).map_err(|_| "data key is not valid".to_string())?;
    Ok(LessSafeKey::new(k))
}

/// Seals `plaintext` under `context`: KMS directly up to
/// [`KMS_DIRECT_MAX`], an envelope above, refused above [`MAX_SEALED_BYTES`].
pub async fn seal(
    keys: &dyn Keys,
    context: HashMap<String, String>,
    plaintext: &[u8],
) -> Result<Vec<u8>, String> {
    if let Some(e) = too_large("The value", plaintext.len()) {
        return Err(e);
    }
    if plaintext.len() <= KMS_DIRECT_MAX {
        return keys.encrypt_direct(context, plaintext).await;
    }
    let ad = aad(&context);
    let dk = keys.generate_data_key(context).await?;
    let key = gcm_key(&dk.plaintext)?;
    let enc_len = u16::try_from(dk.encrypted.len())
        .map_err(|_| "kms returned an encrypted data key that is too long".to_string())?;
    let mut nonce = [0u8; NONCE_LEN];
    SystemRandom::new()
        .fill(&mut nonce)
        .map_err(|_| "no randomness for a nonce".to_string())?;
    let mut body = plaintext.to_vec();
    let sealed = key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce),
        Aad::from(ad),
        &mut body,
    );
    if sealed.is_err() {
        body.zeroize();
        return Err("could not encrypt the value".into());
    }
    let mut out = Vec::with_capacity(MAGIC.len() + 2 + dk.encrypted.len() + NONCE_LEN + body.len());
    out.extend_from_slice(MAGIC);
    out.extend(enc_len.to_be_bytes());
    out.extend_from_slice(&dk.encrypted);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&body);
    Ok(out)
}

/// Opens a blob sealed under `context`, an envelope or a direct KMS blob.
pub async fn open(
    keys: &dyn Keys,
    context: HashMap<String, String>,
    blob: &[u8],
) -> Result<Vec<u8>, String> {
    let Some(rest) = blob.strip_prefix(MAGIC.as_slice()) else {
        return keys.decrypt_direct(context, blob).await;
    };
    let invalid = || "stored value is not valid".to_string();
    let (len, rest) = rest.split_at_checked(2).ok_or_else(invalid)?;
    let len = u16::from_be_bytes([len[0], len[1]]) as usize;
    let (encrypted, rest) = rest.split_at_checked(len).ok_or_else(invalid)?;
    let (nonce, body) = rest.split_at_checked(NONCE_LEN).ok_or_else(invalid)?;
    if encrypted.is_empty() || body.len() < TAG_LEN {
        return Err(invalid());
    }
    let ad = aad(&context);
    let mut raw = keys.decrypt_direct(context, encrypted).await?;
    let key = gcm_key(&raw);
    raw.zeroize();
    let key = key?;
    let nonce = Nonce::try_assume_unique_for_key(nonce).map_err(|_| invalid())?;
    let mut buf = body.to_vec();
    let plain = match key.open_in_place(nonce, Aad::from(ad), &mut buf) {
        Ok(p) => p.to_vec(),
        Err(_) => {
            buf.zeroize();
            return Err(invalid());
        }
    };
    buf.zeroize();
    Ok(plain)
}

/// Any [`Keys`] as a [`crate::mcp_credentials::Cipher`]: the one sealing
/// rule for every caller.
pub struct Enveloped<K>(pub K);

#[async_trait::async_trait]
impl<K: Keys> crate::mcp_credentials::Cipher for Enveloped<K> {
    async fn encrypt(
        &self,
        context: HashMap<String, String>,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, String> {
        seal(&self.0, context, plaintext).await
    }
    async fn decrypt(
        &self,
        context: HashMap<String, String>,
        blob: &[u8],
    ) -> Result<Vec<u8>, String> {
        open(&self.0, context, blob).await
    }
}

#[cfg(test)]
pub(crate) mod testing {
    //! A stand-in for KMS with `GenerateDataKey`.

    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn ctx_key(c: &HashMap<String, String>) -> String {
        let mut v: Vec<_> = c.iter().map(|(k, v)| format!("{k}={v}")).collect();
        v.sort();
        v.join("&")
    }

    /// Like KMS: a blob (direct, or an encrypted data key) carries the
    /// context it was made under, opening under any other fails, and
    /// `Encrypt` refuses more than 4,096 bytes. Counts its calls.
    #[derive(Default)]
    pub struct FakeKeys {
        pub direct: AtomicUsize,
        pub data_keys: AtomicUsize,
    }

    impl FakeKeys {
        pub fn wrap(c: &HashMap<String, String>, p: &[u8]) -> Vec<u8> {
            let mut out = vec![0x01];
            out.extend(ctx_key(c).into_bytes());
            out.push(0);
            out.extend(p.iter().map(|b| b ^ 0x6c));
            out
        }
    }

    #[async_trait::async_trait]
    impl Keys for FakeKeys {
        async fn encrypt_direct(
            &self,
            c: HashMap<String, String>,
            p: &[u8],
        ) -> Result<Vec<u8>, String> {
            if p.len() > KMS_DIRECT_MAX {
                return Err("kms encrypt: ValidationException: plaintext too long".into());
            }
            self.direct.fetch_add(1, Ordering::SeqCst);
            Ok(Self::wrap(&c, p))
        }
        async fn generate_data_key(&self, c: HashMap<String, String>) -> Result<DataKey, String> {
            self.data_keys.fetch_add(1, Ordering::SeqCst);
            let mut key = vec![0u8; KEY_LEN];
            SystemRandom::new().fill(&mut key).unwrap();
            let encrypted = Self::wrap(&c, &key);
            Ok(DataKey {
                plaintext: key,
                encrypted,
            })
        }
        async fn decrypt_direct(
            &self,
            c: HashMap<String, String>,
            b: &[u8],
        ) -> Result<Vec<u8>, String> {
            let b = b.strip_prefix(&[0x01]).ok_or("kms decrypt: bad blob")?;
            let at = b
                .iter()
                .position(|x| *x == 0)
                .ok_or("kms decrypt: bad blob")?;
            if b[..at] != *ctx_key(&c).as_bytes() {
                return Err("kms decrypt: InvalidCiphertextException".into());
            }
            Ok(b[at + 1..].iter().map(|x| x ^ 0x6c).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::FakeKeys;
    use super::*;
    use std::sync::atomic::Ordering;

    fn ctx(team: &str, purpose: &str) -> HashMap<String, String> {
        HashMap::from([
            ("team".to_string(), team.to_string()),
            ("server".to_string(), "atlassian".to_string()),
            ("purpose".to_string(), purpose.to_string()),
        ])
    }

    /// An Atlassian-sized token set: a ~5 KB JWT access token and a refresh
    /// token, as JSON.
    fn atlassian_tokens() -> Vec<u8> {
        let jwt = format!(
            "eyJraWQiOiJhdXRoLmF0bGFzc2lhbi5jb20iLCJhbGciOiJSUzI1NiJ9.{}.{}",
            "eyJzdWIiOiI".repeat(400),
            "c2lnbmF0dXJl".repeat(30)
        );
        let refresh = format!("eyJraWQiOiJyZWZyZXNoIn0.{}", "cmVmcmVzaA".repeat(120));
        serde_json::to_vec(&serde_json::json!({
            "access_token": jwt,
            "refresh_token": refresh,
            "expires_at": 1_800_000_000,
            "scope": "read:confluence-content.all offline_access",
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn an_atlassian_sized_token_set_round_trips_in_an_envelope() {
        let keys = FakeKeys::default();
        let plain = atlassian_tokens();
        assert!(
            plain.len() > KMS_DIRECT_MAX && plain.len() < 8 * 1024,
            "{}",
            plain.len()
        );
        let blob = seal(&keys, ctx("TEAM#a", "mcp-oauth-token"), &plain)
            .await
            .unwrap();
        assert!(blob.starts_with(MAGIC));
        assert_eq!(keys.data_keys.load(Ordering::SeqCst), 1);
        assert_eq!(keys.direct.load(Ordering::SeqCst), 0);
        // The value isn't in the blob in the clear.
        assert!(!blob.windows(16).any(|w| w == &plain[..16]));
        let opened = open(&keys, ctx("TEAM#a", "mcp-oauth-token"), &blob)
            .await
            .unwrap();
        assert_eq!(opened, plain);
    }

    #[tokio::test]
    async fn small_values_keep_the_direct_format() {
        let keys = FakeKeys::default();
        let blob = seal(&keys, ctx("TEAM#a", "mcp-header"), b"Bearer abc")
            .await
            .unwrap();
        assert!(!blob.starts_with(MAGIC));
        assert_eq!(
            blob,
            FakeKeys::wrap(&ctx("TEAM#a", "mcp-header"), b"Bearer abc")
        );
        assert_eq!(keys.data_keys.load(Ordering::SeqCst), 0);
        // Exactly the direct limit is still direct.
        let edge = vec![b'x'; KMS_DIRECT_MAX];
        let blob = seal(&keys, ctx("TEAM#a", "x"), &edge).await.unwrap();
        assert!(!blob.starts_with(MAGIC));
        let blob = seal(&keys, ctx("TEAM#a", "x"), &[edge, vec![b'y']].concat())
            .await
            .unwrap();
        assert!(blob.starts_with(MAGIC));
    }

    #[tokio::test]
    async fn a_blob_written_before_envelopes_still_opens() {
        let keys = FakeKeys::default();
        let old = FakeKeys::wrap(
            &ctx("TEAM#a", "mcp-oauth-token"),
            b"{\"access_token\":\"t\"}",
        );
        let opened = open(&keys, ctx("TEAM#a", "mcp-oauth-token"), &old)
            .await
            .unwrap();
        assert_eq!(opened, b"{\"access_token\":\"t\"}");
    }

    #[tokio::test]
    async fn the_encryption_context_is_enforced() {
        let keys = FakeKeys::default();
        let blob = seal(&keys, ctx("TEAM#a", "mcp-oauth-token"), &atlassian_tokens())
            .await
            .unwrap();
        for wrong in [
            ctx("TEAM#b", "mcp-oauth-token"),
            ctx("TEAM#a", "mcp-header"),
        ] {
            assert!(open(&keys, wrong, &blob).await.is_err());
        }
        let mut other_server = ctx("TEAM#a", "mcp-oauth-token");
        other_server.insert("server".into(), "notion".into());
        assert!(open(&keys, other_server, &blob).await.is_err());
        let mut with_owner = ctx("TEAM#a", "mcp-oauth-token");
        with_owner.insert("owner".into(), "user:a".into());
        assert!(open(&keys, with_owner, &blob).await.is_err());
    }

    #[tokio::test]
    async fn a_changed_blob_does_not_open() {
        let keys = FakeKeys::default();
        let c = ctx("TEAM#a", "mcp-oauth-token");
        let blob = seal(&keys, c.clone(), &atlassian_tokens()).await.unwrap();
        let mut flipped = blob.clone();
        *flipped.last_mut().unwrap() ^= 1;
        assert!(open(&keys, c.clone(), &flipped).await.is_err());
        for cut in [
            MAGIC.len(),
            MAGIC.len() + 1,
            MAGIC.len() + 10,
            blob.len() - 1,
        ] {
            assert!(
                open(&keys, c.clone(), &blob[..cut]).await.is_err(),
                "cut at {cut}"
            );
        }
    }

    #[tokio::test]
    async fn the_size_bound() {
        let keys = FakeKeys::default();
        let c = ctx("TEAM#a", "mcp-oauth-token");
        let max = vec![b'x'; MAX_SEALED_BYTES];
        let blob = seal(&keys, c.clone(), &max).await.unwrap();
        assert_eq!(open(&keys, c.clone(), &blob).await.unwrap(), max);
        // Well under DynamoDB's 400 KB item limit once base64 encoded.
        assert!(blob.len() * 4 / 3 < 100 * 1024);
        let over = vec![b'x'; MAX_SEALED_BYTES + 1];
        let e = seal(&keys, c, &over).await.unwrap_err();
        assert!(e.contains("65 KB") && e.contains("64 KB"), "{e}");
        assert_eq!(keys.data_keys.load(Ordering::SeqCst), 1);
        assert!(too_large("x", MAX_SEALED_BYTES).is_none());
    }

    #[test]
    fn a_data_key_never_prints() {
        let dk = DataKey {
            plaintext: vec![7; 32],
            encrypted: vec![1, 2],
        };
        assert_eq!(format!("{dk:?}"), "DataKey(..)");
    }
}
