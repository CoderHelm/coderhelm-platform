use aws_sdk_s3::Client as S3Client;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::path::{Path, PathBuf};
use tar::{Archive, Builder};
use tracing::{info, warn};

const MEMORY_PREFIX: &str = "teams";
const MEMORY_SUFFIX: &str = "memory.tar.gz";

/// Build the S3 key for a team+repo memory snapshot.
fn memory_key(team_id: &str, repo_owner: &str, repo_name: &str) -> String {
    format!("{team_id}/{MEMORY_PREFIX}/{repo_owner}/{repo_name}/{MEMORY_SUFFIX}")
}

/// Download a memory snapshot from S3 to a local directory.
/// Returns the local path where MenteDB files live.
/// If no snapshot exists (first run), returns the path with an empty directory.
pub async fn download_memory(
    s3: &S3Client,
    bucket: &str,
    team_id: &str,
    repo_owner: &str,
    repo_name: &str,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let key = memory_key(team_id, repo_owner, repo_name);
    let local_dir = PathBuf::from(format!(
        "/tmp/mentedb/{team_id}/{repo_owner}/{repo_name}"
    ));

    // Clean up any leftover from previous invocation
    if local_dir.exists() {
        std::fs::remove_dir_all(&local_dir)?;
    }
    std::fs::create_dir_all(&local_dir)?;

    match s3.get_object().bucket(bucket).key(&key).send().await {
        Ok(response) => {
            let body = response.body.collect().await?.into_bytes();
            let decoder = GzDecoder::new(body.as_ref());
            let mut archive = Archive::new(decoder);
            archive.unpack(&local_dir)?;
            info!(
                key = %key,
                bytes = body.len(),
                "Downloaded memory snapshot from S3"
            );
        }
        Err(e) => {
            let err_str = format!("{e}");
            if err_str.contains("NoSuchKey") || err_str.contains("404") {
                info!(key = %key, "No existing memory snapshot (first run for this repo)");
            } else {
                warn!(key = %key, error = %e, "Failed to download memory snapshot");
                return Err(e.into());
            }
        }
    }

    Ok(local_dir)
}

/// Upload a memory snapshot from local directory back to S3.
pub async fn upload_memory(
    s3: &S3Client,
    bucket: &str,
    team_id: &str,
    repo_owner: &str,
    repo_name: &str,
    local_dir: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let key = memory_key(team_id, repo_owner, repo_name);

    // Tar + gzip the directory
    let mut buf = Vec::new();
    {
        let encoder = GzEncoder::new(&mut buf, Compression::fast());
        let mut tar = Builder::new(encoder);
        tar.append_dir_all(".", local_dir)?;
        tar.into_inner()?.finish()?;
    }

    s3.put_object()
        .bucket(bucket)
        .key(&key)
        .body(buf.into())
        .content_type("application/gzip")
        .send()
        .await?;

    info!(key = %key, "Uploaded memory snapshot to S3");
    Ok(())
}

/// Clean up local memory files from /tmp.
pub fn cleanup_local(local_dir: &Path) {
    if local_dir.exists() {
        if let Err(e) = std::fs::remove_dir_all(local_dir) {
            warn!(path = %local_dir.display(), error = %e, "Failed to clean up local memory files");
        }
    }
}
