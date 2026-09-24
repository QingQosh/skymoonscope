//! Domain types and the job-status finite-state machine.
//!
//! This module owns every pure-data definition that the rest of the `jobs`
//! sub-system shares: identifiers, enumerations, payload shapes, the error
//! type, and the configuration struct.  Nothing here touches a database,
//! a Redis connection, or a scheduler.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::simulation::{SimulationResult, SorobanResources};

// ── Identifier ───────────────────────────────────────────────────────────────

/// Unique identifier for a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema, sqlx::Type)]
#[sqlx(transparent)]
pub struct JobId(pub Uuid);

impl JobId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for JobId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

// ── Status FSM ───────────────────────────────────────────────────────────────

/// Status of a job in its lifecycle.
///
/// Valid transitions:
/// ```text
/// Queued → Processing → Completed
///                     → Failed
/// Queued  → Cancelled
/// Processing → Cancelled
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, sqlx::Type)]
#[sqlx(rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobStatus {
    Queued,
    Processing,
    Completed,
    Failed,
    Cancelled,
}

impl JobStatus {
    /// Returns `true` when the job has reached a terminal state.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// Returns `true` when cancellation is a legal transition from this state.
    pub fn can_cancel(self) -> bool {
        matches!(self, Self::Queued | Self::Processing)
    }
}

// ── Job type ─────────────────────────────────────────────────────────────────

/// Discriminator that selects which analysis the worker will run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, sqlx::Type)]
#[sqlx(rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    Analyze,
    Compare,
    OptimizeLimits,
}

// ── Payload ──────────────────────────────────────────────────────────────────

/// Input data for a job, discriminated by variant.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum JobPayload {
    Analyze {
        contract_id: String,
        function_name: String,
        args: Option<Vec<String>>,
        ledger_overrides: Option<HashMap<String, String>>,
    },
    Compare {
        mode: String,
        current_wasm: Option<Vec<u8>>,
        base_wasm: Option<Vec<u8>>,
        contract_id: Option<String>,
        function_name: Option<String>,
        args: Vec<String>,
    },
    OptimizeLimits {
        contract_id: String,
        function_name: String,
        args: Vec<String>,
        safety_margin: f64,
    },
}

impl JobPayload {
    /// The contract this job targets, if any.
    pub fn contract_id(&self) -> Option<&str> {
        match self {
            JobPayload::Analyze { contract_id, .. } => Some(contract_id),
            JobPayload::OptimizeLimits { contract_id, .. } => Some(contract_id),
            JobPayload::Compare { contract_id, .. } => contract_id.as_deref(),
        }
    }

    /// The contract function this job invokes, if any.
    pub fn function_name(&self) -> Option<&str> {
        match self {
            JobPayload::Analyze { function_name, .. } => Some(function_name),
            JobPayload::OptimizeLimits { function_name, .. } => Some(function_name),
            JobPayload::Compare { function_name, .. } => function_name.as_deref(),
        }
    }
}

// ── List filter ──────────────────────────────────────────────────────────────

/// Filters accepted by [`crate::jobs::store::JobQueue::list`].
#[derive(Debug, Clone, Default)]
pub struct JobListFilter {
    pub status: Option<JobStatus>,
    pub job_type: Option<JobType>,
    pub contract_id: Option<String>,
}

// ── Progress ─────────────────────────────────────────────────────────────────

/// A point-in-time progress snapshot for a running job.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobProgress {
    pub percent: i32,
    pub message: String,
    pub updated_at: DateTime<Utc>,
}

// ── Result ───────────────────────────────────────────────────────────────────

/// Outcome of a finished job, stored as a tagged JSON blob in the database.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "data")]
pub enum JobResult {
    Success {
        #[serde(skip_serializing_if = "Option::is_none")]
        resources: Option<SorobanResources>,
        #[serde(skip_serializing_if = "Option::is_none")]
        simulation_result: Option<SimulationResult>,
        #[serde(skip_serializing_if = "Option::is_none")]
        optimization: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        comparison: Option<Value>,
    },
    Failed {
        error: String,
        error_type: String,
    },
}

// ── Webhook ──────────────────────────────────────────────────────────────────

/// Delivery configuration for post-completion webhook notifications.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WebhookConfig {
    pub callback_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
}

// ── Job row ──────────────────────────────────────────────────────────────────

/// The full database row for a job, plus convenience accessors that decode the
/// opaque JSON columns.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
pub struct Job {
    pub id: JobId,
    pub job_type: JobType,
    pub status: JobStatus,
    pub payload: Value,
    pub result: Option<Value>,
    pub progress_percent: i32,
    pub progress_message: String,
    pub webhook_url: Option<String>,
    pub webhook_headers: Option<Value>,
    pub webhook_secret: Option<String>,
    pub error_message: Option<String>,
    pub error_type: Option<String>,
    pub timeout_secs: i32,
    pub retry_count: i32,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl Job {
    pub fn get_progress(&self) -> JobProgress {
        JobProgress {
            percent: self.progress_percent,
            message: self.progress_message.clone(),
            updated_at: self.updated_at,
        }
    }

    pub fn get_result(&self) -> Option<JobResult> {
        self.result
            .as_ref()
            .and_then(|r| serde_json::from_value(r.clone()).ok())
    }

    pub fn get_payload(&self) -> Option<JobPayload> {
        serde_json::from_value(self.payload.clone()).ok()
    }

    pub fn get_webhook_config(&self) -> Option<WebhookConfig> {
        self.webhook_url.as_ref().map(|url| WebhookConfig {
            callback_url: url.clone(),
            headers: self
                .webhook_headers
                .as_ref()
                .and_then(|h| serde_json::from_value(h.clone()).ok()),
            secret: self.webhook_secret.clone(),
        })
    }
}

// ── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum JobError {
    #[error("Job not found: {0}")]
    NotFound(JobId),
    #[error("Job cannot be cancelled in status: {0:?}")]
    CannotCancel(JobStatus),
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Job processing failed: {0}")]
    ProcessingFailed(String),
    #[error("Webhook delivery failed: {0}")]
    WebhookFailed(String),
}

// ── Queue configuration ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct JobQueueConfig {
    pub job_timeout_secs: u64,
    pub cleanup_interval_secs: u64,
    pub retention_secs: u64,
    pub webhook_timeout_secs: u64,
    pub webhook_max_retries: u32,
    pub max_concurrent_jobs: usize,
    pub max_job_retries: i32,
    pub retry_queue_capacity: usize,
}

impl Default for JobQueueConfig {
    fn default() -> Self {
        Self {
            job_timeout_secs: 300,
            cleanup_interval_secs: 3600,
            retention_secs: 3600,
            webhook_timeout_secs: 10,
            webhook_max_retries: 3,
            max_concurrent_jobs: 10,
            max_job_retries: 3,
            retry_queue_capacity: 256,
        }
    }
}
