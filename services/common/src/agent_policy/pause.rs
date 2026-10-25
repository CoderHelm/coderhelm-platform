//! "Pause all agents", and the work that waits for it.
//!
//! While agents are paused nothing new starts: work that arrives is kept
//! (`AGENT_QUEUE#`) exactly as it was sent, and a loop already running stops
//! at its next call and its work is kept the same way. When the pause is
//! lifted, or its until-time passes, the kept work is sent again in the
//! order it arrived. Nothing is dropped.

use serde_json::Value;

use super::audit::{self, ActorKind, Event};
use super::model::Pause;
use super::store::{PolicyStore, QueuedWork, StoreError};

/// Sends kept work back to the worker (the tickets queue).
#[async_trait::async_trait]
pub trait Resender: Send + Sync {
    async fn send(&self, message: &Value) -> Result<(), String>;
}

/// Turns the pause on. `until` is RFC 3339, or None until lifted.
pub async fn pause(
    store: &dyn PolicyStore,
    team: &str,
    by: &str,
    reason: &str,
    until: Option<String>,
) -> Result<Pause, StoreError> {
    let reason = crate::truncate_str(reason.trim(), 300).to_string();
    let now = super::now();
    for _ in 0..3 {
        let mut p = store.load(team).await.map_err(StoreError::Other)?;
        let expect = p.version;
        let pause = Pause {
            reason: reason.clone(),
            by: by.to_string(),
            at: now.clone(),
            until: until.clone(),
        };
        p.pause = Some(pause.clone());
        p.version += 1;
        p.updated_by = by.to_string();
        p.updated_at = now.clone();
        match store.save(team, &p, expect).await {
            Ok(()) => {
                let _ = store.mark_paused(team).await;
                store
                    .audit(
                        team,
                        Event::new(by, ActorKind::Person, audit::action::PAUSED, "ok").detail(
                            format!(
                                "{}{}",
                                if reason.is_empty() {
                                    "No reason given"
                                } else {
                                    &reason
                                },
                                until
                                    .as_deref()
                                    .map(|u| format!(", until {u}"))
                                    .unwrap_or_default()
                            ),
                        ),
                    )
                    .await;
                return Ok(pause);
            }
            Err(StoreError::Conflict) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(StoreError::Conflict)
}

/// Lifts the pause (if any) and sends the kept work again. Returns how many
/// were sent.
pub async fn lift(
    store: &dyn PolicyStore,
    team: &str,
    by: &str,
    resender: &dyn Resender,
) -> Result<usize, StoreError> {
    for _ in 0..3 {
        let mut p = store.load(team).await.map_err(StoreError::Other)?;
        if p.pause.is_none() {
            break;
        }
        let expect = p.version;
        p.pause = None;
        p.version += 1;
        p.updated_by = by.to_string();
        p.updated_at = super::now();
        match store.save(team, &p, expect).await {
            Ok(()) => {
                store
                    .audit(
                        team,
                        Event::new(by, ActorKind::Person, audit::action::RESUMED, "ok"),
                    )
                    .await;
                break;
            }
            Err(StoreError::Conflict) => continue,
            Err(e) => return Err(e),
        }
    }
    resume_queued(store, team, resender).await
}

/// Sends kept work again when agents aren't paused (any more). Used when a
/// pause is lifted and by the scheduled sweep that notices an until-time
/// passing. Work that fails to send stays kept for the next try.
pub async fn resume_queued(
    store: &dyn PolicyStore,
    team: &str,
    resender: &dyn Resender,
) -> Result<usize, StoreError> {
    let p = store.load(team).await.map_err(StoreError::Other)?;
    if p.paused(&super::now()).is_some() {
        return Ok(0);
    }
    let mut sent = 0;
    let mut failed = false;
    for w in store.list_queued(team).await.map_err(StoreError::Other)? {
        match resender.send(&w.message).await {
            Ok(()) => {
                let _ = store.remove_queued(team, &w.id).await;
                store
                    .audit(
                        team,
                        Event::new(
                            "system",
                            ActorKind::System,
                            audit::action::WORK_RESUMED,
                            "ok",
                        )
                        .run(&w.run_id)
                        .detail(w.label.clone()),
                    )
                    .await;
                sent += 1;
            }
            Err(e) => {
                failed = true;
                tracing::warn!(team, id = %w.id, error = %e, "Couldn't resume kept work; it stays kept");
            }
        }
    }
    if !failed {
        let _ = store.unmark_paused(team).await;
    }
    Ok(sent)
}

/// Keeps `message` if agents are paused. Returns the pause reason when it
/// was kept (the caller then stops), None when work may go on.
pub async fn hold_if_paused(
    store: &dyn PolicyStore,
    team: &str,
    label: &str,
    run_id: &str,
    message: &Value,
) -> Option<String> {
    let p = store.load(team).await.ok()?;
    let reason = p.paused(&super::now())?.reason.clone();
    keep(store, team, label, run_id, message).await;
    Some(reason)
}

/// Keeps work to resume later (a loop that stopped for a pause mid-run).
pub async fn keep(store: &dyn PolicyStore, team: &str, label: &str, run_id: &str, message: &Value) {
    let w = QueuedWork {
        id: super::next_id(),
        queued_at: super::now(),
        label: crate::truncate_str(label, 200).to_string(),
        run_id: run_id.to_string(),
        message: message.clone(),
    };
    if let Err(e) = store.queue_work(team, &w).await {
        tracing::error!(team, error = %e, "Couldn't keep paused work");
        return;
    }
    let _ = store.mark_paused(team).await;
    store
        .audit(
            team,
            Event::new(
                "system",
                ActorKind::System,
                audit::action::WORK_QUEUED,
                "waiting",
            )
            .run(run_id)
            .detail(w.label),
        )
        .await;
}

/// The scheduled sweep: for every team that is (or was) paused, send kept
/// work again once its pause has ended (an until-time passing needs nobody
/// to click anything). Returns how many messages were sent.
pub async fn sweep(store: &dyn PolicyStore, resender: &dyn Resender) -> usize {
    let mut sent = 0;
    for team in store.paused_teams().await.unwrap_or_default() {
        match resume_queued(store, &team, resender).await {
            Ok(n) => sent += n,
            Err(e) => {
                tracing::warn!(team = %team, error = %e, "Paused work sweep failed for a team")
            }
        }
    }
    sent
}
