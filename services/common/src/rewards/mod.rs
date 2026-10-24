//! Learning from outcomes: CoderHelm's reward system.
//!
//! Signals (a review comment acted on or dismissed, a thumbs up or down, a
//! pull request merged as written, edited, closed or reverted, CI's first-try
//! result, a plan task approved or rejected, time to merge, re-review asks,
//! an alert tied to a merged change) are each recorded as one
//! [`event::RewardEvent`], from the GitHub webhooks and dashboard actions
//! that already report them.
//!
//! A transparent weighted score ([`score`]) turns events into a reward per
//! run, review, finding category, agent, repository, skill version and
//! prompt version. The agents then seek reward in context, with no training:
//! good and bad past examples are put in their prompts ([`exemplars`]), the
//! reviewer is calibrated per repository and category ([`calibration`]),
//! prompt variants are picked by a capped bandit ([`bandit`]), and lessons
//! are written only from evidence ([`lessons`]). Every behavior change is
//! logged with its evidence and can be reverted per repository
//! ([`changes`]). All records are per team ([`store`]).

pub mod bandit;
pub mod calibration;
pub mod changes;
pub mod event;
pub mod exemplars;
pub mod lessons;
pub mod mapping;
pub mod score;
pub mod store;
pub mod summary;

pub use event::{RewardEvent, RewardKind};
