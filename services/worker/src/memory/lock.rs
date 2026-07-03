use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;
use tracing::{info, warn};

const LOCK_TTL_SECONDS: i64 = 900; // 15 minutes (matches worker Lambda timeout)

/// Acquire a distributed lock for a team+repo memory.
/// Returns Ok(Some(token)) if acquired — the fencing token must be presented
/// to release_lock, so a finished invocation can never delete a SUCCESSOR's
/// lock (previously the unconditional delete let run A, finishing after its
/// TTL lapsed, drop the lock run B had legitimately taken over).
/// Returns Ok(None) if already locked.
pub async fn acquire_lock(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    repo_owner: &str,
    repo_name: &str,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    let pk = format!("TEAM#{team_id}");
    let sk = format!("MEMLOCK#{repo_owner}/{repo_name}");
    let now = chrono::Utc::now().timestamp();
    let expires_at = now + LOCK_TTL_SECONDS;
    let token = ulid::Ulid::new().to_string();

    let result = dynamo
        .put_item()
        .table_name(table)
        .item("pk", AttributeValue::S(pk.clone()))
        .item("sk", AttributeValue::S(sk.clone()))
        .item("locked_at", AttributeValue::N(now.to_string()))
        .item("expires_at", AttributeValue::N(expires_at.to_string()))
        .item("owner_token", AttributeValue::S(token.clone()))
        // Only succeed if: no lock exists OR existing lock has expired
        .condition_expression("attribute_not_exists(pk) OR expires_at < :now")
        .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
        .send()
        .await;

    match result {
        Ok(_) => {
            info!(
                team_id,
                repo = %format!("{repo_owner}/{repo_name}"),
                "Acquired memory lock"
            );
            Ok(Some(token))
        }
        Err(e) => {
            let err_str = format!("{e}");
            if err_str.contains("ConditionalCheckFailed") {
                warn!(
                    team_id,
                    repo = %format!("{repo_owner}/{repo_name}"),
                    "Memory lock held by another invocation, running stateless"
                );
                Ok(None)
            } else {
                Err(e.into())
            }
        }
    }
}

/// Release a distributed lock for a team+repo memory.
pub async fn release_lock(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    repo_owner: &str,
    repo_name: &str,
    owner_token: Option<&str>,
) {
    let pk = format!("TEAM#{team_id}");
    let sk = format!("MEMLOCK#{repo_owner}/{repo_name}");

    let mut req = dynamo
        .delete_item()
        .table_name(table)
        .key("pk", AttributeValue::S(pk))
        .key("sk", AttributeValue::S(sk));
    if let Some(token) = owner_token {
        // Fenced release: only delete OUR lock, never a successor's.
        req = req
            .condition_expression("attribute_not_exists(pk) OR owner_token = :tok")
            .expression_attribute_values(":tok", AttributeValue::S(token.to_string()));
    } else {
        // Tokenless (error-arm cleanup): only delete a lock that has already
        // expired — a live lock might belong to a concurrent run.
        req = req
            .condition_expression("attribute_not_exists(pk) OR expires_at < :now")
            .expression_attribute_values(
                ":now",
                AttributeValue::N(chrono::Utc::now().timestamp().to_string()),
            );
    }
    if let Err(e) = req.send().await {
        warn!(error = %e, "Failed to release memory lock (will auto-expire)");
    } else {
        info!(
            team_id,
            repo = %format!("{repo_owner}/{repo_name}"),
            "Released memory lock"
        );
    }
}
