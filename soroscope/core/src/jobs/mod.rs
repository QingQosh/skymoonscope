//! Async job queue — split into three focused sub-modules:
//!
//! | Module | Responsibility |
//! |---|---|
//! | [`domain`] | Pure types, the status FSM, and `JobQueueConfig`. |
//! | [`store`] | SQL persistence via [`store::JobQueue`] and Axum HTTP handlers. |
//! | [`scheduler`] | Retry/backoff scheduling and the [`scheduler::JobWorker`] loop. |
//!
//! All public items from the three modules are re-exported here so existing
//! callers (`use crate::jobs::JobQueue`, etc.) continue to compile unchanged.

#![allow(
    dead_code,
    clippy::large_enum_variant,
    clippy::manual_inspect,
    clippy::needless_borrows_for_generic_args
)]

pub mod domain;
pub mod scheduler;
pub mod store;

// Re-export everything that was previously at the top level of `jobs.rs`.
pub use domain::{
    Job, JobError, JobId, JobListFilter, JobPayload, JobProgress, JobQueueConfig, JobResult,
    JobStatus, JobType, WebhookConfig,
};
pub use scheduler::{JobWorker, SchedulerExt};
pub use store::{
    cancel_job_handler, get_job_handler, submit_job_handler, DbPool, JobQueue, SubmitJobRequest,
    SubmitJobResponse,
};
