//! Database persistence layer for jobs.
//!
//! [`JobQueue`] is the sole owner of the SQL connection pool and the Redis
//! client.  Every method either reads from or writes to the database; side-
//! effects outside the DB (retry scheduling, worker loops) live in
//! [`crate::jobs::scheduler`].

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use redis::AsyncCommands;
use redis::Client as RedisClient;
use serde::{Deserialize, Serialize};
use sqlx::any::AnyQueryResult;
use sqlx::{PgPool, SqlitePool};
use std::str::FromStr;
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::task_queue::BoundedTaskDispatcher;
use crate::AppError;

use super::domain::{
    Job, JobError, JobId, JobListFilter, JobPayload, JobQueueConfig, JobResult, JobStatus,
    JobType, WebhookConfig,
};

// ── DbPool ────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub enum DbPool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

impl DbPool {
    pub async fn execute(&self, query: &str) -> Result<AnyQueryResult, sqlx::Error> {
        match self {
            DbPool::Postgres(pool) => {
                let result = sqlx::query(query).execute(pool).await?;
                Ok(AnyQueryResult {
                    rows_affected: result.rows_affected(),
                    last_insert_id: None,
                })
            }
            DbPool::Sqlite(pool) => {
                let result = sqlx::query(query).execute(pool).await?;
                Ok(AnyQueryResult {
                    rows_affected: result.rows_affected(),
                    last_insert_id: Some(result.last_insert_rowid()),
                })
            }
        }
    }
}

// ── JobQueue ──────────────────────────────────────────────────────────────────

pub struct JobQueue {
    pub(super) pool: DbPool,
    pub(super) redis: RedisClient,
    pub(super) config: JobQueueConfig,
    pub(super) retry_dispatcher: BoundedTaskDispatcher,
}

impl JobQueue {
    pub async fn new(
        database_url: &str,
        redis_url: &str,
        config: JobQueueConfig,
    ) -> Result<Self, JobError> {
        let pool = if database_url.starts_with("postgres://") {
            let pool = PgPool::connect(database_url).await?;
            DbPool::Postgres(pool)
        } else {
            let pool = SqlitePool::connect(database_url).await?;
            DbPool::Sqlite(pool)
        };

        let redis = RedisClient::open(redis_url).map_err(|e| {
            JobError::ProcessingFailed(format!("Failed to connect to Redis: {}", e))
        })?;

        Self::run_migrations(&pool).await?;

        let retry_dispatcher = BoundedTaskDispatcher::new(config.retry_queue_capacity);

        Ok(Self {
            pool,
            redis,
            config,
            retry_dispatcher,
        })
    }

    async fn run_migrations(pool: &DbPool) -> Result<(), JobError> {
        let migration_sql = include_str!("../../migrations/001_create_jobs_table.sql");

        for statement in migration_sql.split(';') {
            let stmt = statement.trim();
            if !stmt.is_empty() {
                pool.execute(stmt).await?;
            }
        }

        Ok(())
    }

    // ── Write operations ──────────────────────────────────────────────────────

    pub async fn submit(
        &self,
        job_type: JobType,
        payload: JobPayload,
        webhook: Option<WebhookConfig>,
    ) -> Result<JobId, JobError> {
        let id = JobId::new();
        let payload_json = serde_json::to_value(&payload).map_err(|e| {
            JobError::ProcessingFailed(format!("Failed to serialize payload: {}", e))
        })?;

        let (webhook_url, webhook_headers, webhook_secret) = match webhook {
            Some(w) => (
                Some(w.callback_url),
                w.headers
                    .map(|h| serde_json::to_value(h).unwrap_or_default()),
                w.secret,
            ),
            None => (None, None, None),
        };

        match &self.pool {
            DbPool::Postgres(pool) => {
                sqlx::query(
                    r#"
                    INSERT INTO jobs (id, job_type, status, payload, webhook_url, webhook_headers, webhook_secret, timeout_secs)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    "#,
                )
                .bind(&id)
                .bind(&job_type)
                .bind(&JobStatus::Queued)
                .bind(&payload_json)
                .bind(&webhook_url)
                .bind(&webhook_headers)
                .bind(&webhook_secret)
                .bind(self.config.job_timeout_secs as i32)
                .execute(pool)
                .await?;
            }
            DbPool::Sqlite(pool) => {
                sqlx::query(
                    r#"
                    INSERT INTO jobs (id, job_type, status, payload, webhook_url, webhook_headers, webhook_secret, timeout_secs)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                    "#,
                )
                .bind(&id.0.to_string())
                .bind(&job_type)
                .bind("QUEUED")
                .bind(&payload_json)
                .bind(&webhook_url)
                .bind(&webhook_headers)
                .bind(&webhook_secret)
                .bind(self.config.job_timeout_secs as i32)
                .execute(pool)
                .await?;
            }
        }

        let mut conn = self
            .redis
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                JobError::ProcessingFailed(format!("Failed to get Redis connection: {}", e))
            })?;

        conn.lpush::<_, _, ()>("soroscope:jobs:queue", id.0.to_string())
            .await
            .map_err(|e| JobError::ProcessingFailed(format!("Redis LPUSH failed: {}", e)))?;

        tracing::info!(job_id = %id, "Job submitted to Redis queue");
        Ok(id)
    }

    pub async fn mark_processing(&self, id: &JobId) -> Result<(), JobError> {
        match &self.pool {
            DbPool::Postgres(pool) => {
                sqlx::query(
                    "UPDATE jobs SET status = 'PROCESSING', started_at = NOW(), \
                     progress_percent = 10, progress_message = 'Processing started' \
                     WHERE id = $1",
                )
                .bind(id)
                .execute(pool)
                .await?;
            }
            DbPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE jobs SET status = 'PROCESSING', started_at = datetime('now'), \
                     progress_percent = 10, progress_message = 'Processing started' \
                     WHERE id = ?1",
                )
                .bind(id.0.to_string())
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn update_progress(
        &self,
        id: &JobId,
        percent: i32,
        message: &str,
    ) -> Result<(), JobError> {
        match &self.pool {
            DbPool::Postgres(pool) => {
                sqlx::query(
                    "UPDATE jobs SET progress_percent = $1, progress_message = $2 WHERE id = $3",
                )
                .bind(percent)
                .bind(message)
                .bind(id)
                .execute(pool)
                .await?;
            }
            DbPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE jobs SET progress_percent = ?1, progress_message = ?2 WHERE id = ?3",
                )
                .bind(percent)
                .bind(message)
                .bind(id.0.to_string())
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn complete(&self, id: &JobId, result: &JobResult) -> Result<(), JobError> {
        let result_json = serde_json::to_value(result).map_err(|e| {
            JobError::ProcessingFailed(format!("Failed to serialize result: {}", e))
        })?;

        match &self.pool {
            DbPool::Postgres(pool) => {
                sqlx::query(
                    "UPDATE jobs SET status = 'COMPLETED', result = $1, \
                     completed_at = NOW(), progress_percent = 100, \
                     progress_message = 'Completed' WHERE id = $2",
                )
                .bind(&result_json)
                .bind(id)
                .execute(pool)
                .await?;
            }
            DbPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE jobs SET status = 'COMPLETED', result = ?1, \
                     completed_at = datetime('now'), progress_percent = 100, \
                     progress_message = 'Completed' WHERE id = ?2",
                )
                .bind(&result_json)
                .bind(id.0.to_string())
                .execute(pool)
                .await?;
            }
        }

        tracing::info!(job_id = %id, "Job completed");
        Ok(())
    }

    pub async fn fail(&self, id: &JobId, error: &str, error_type: &str) -> Result<(), JobError> {
        let result = JobResult::Failed {
            error: error.to_string(),
            error_type: error_type.to_string(),
        };
        let result_json = serde_json::to_value(&result).unwrap_or_default();

        match &self.pool {
            DbPool::Postgres(pool) => {
                sqlx::query(
                    "UPDATE jobs SET status = 'FAILED', result = $1, \
                     error_message = $2, error_type = $3, \
                     completed_at = NOW(), progress_message = 'Failed' WHERE id = $4",
                )
                .bind(&result_json)
                .bind(error)
                .bind(error_type)
                .bind(id)
                .execute(pool)
                .await?;
            }
            DbPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE jobs SET status = 'FAILED', result = ?1, \
                     error_message = ?2, error_type = ?3, \
                     completed_at = datetime('now'), progress_message = 'Failed' WHERE id = ?4",
                )
                .bind(&result_json)
                .bind(error)
                .bind(error_type)
                .bind(id.0.to_string())
                .execute(pool)
                .await?;
            }
        }

        tracing::error!(job_id = %id, error = %error, "Job failed");
        Ok(())
    }

    pub async fn cancel(&self, id: &JobId) -> Result<Job, JobError> {
        let job = self.get(id).await?.ok_or(JobError::NotFound(*id))?;

        match job.status {
            JobStatus::Queued | JobStatus::Processing => {
                match &self.pool {
                    DbPool::Postgres(pool) => {
                        sqlx::query(
                            "UPDATE jobs SET status = 'CANCELLED', \
                             completed_at = NOW(), progress_message = 'Cancelled' \
                             WHERE id = $1",
                        )
                        .bind(id)
                        .execute(pool)
                        .await?;
                    }
                    DbPool::Sqlite(pool) => {
                        sqlx::query(
                            "UPDATE jobs SET status = 'CANCELLED', \
                             completed_at = datetime('now'), progress_message = 'Cancelled' \
                             WHERE id = ?1",
                        )
                        .bind(id.0.to_string())
                        .execute(pool)
                        .await?;
                    }
                }

                tracing::info!(job_id = %id, "Job cancelled");
                self.get(id).await?.ok_or(JobError::NotFound(*id))
            }
            status => Err(JobError::CannotCancel(status)),
        }
    }

    pub async fn cleanup(&self) -> Result<u64, JobError> {
        let deleted = match &self.pool {
            DbPool::Postgres(pool) => {
                let result = sqlx::query(
                    "DELETE FROM jobs \
                     WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED') \
                     AND completed_at < NOW() - INTERVAL '1 hour' * $1",
                )
                .bind(self.config.retention_secs as f64 / 3600.0)
                .execute(pool)
                .await?;
                result.rows_affected()
            }
            DbPool::Sqlite(pool) => {
                let result = sqlx::query(
                    "DELETE FROM jobs \
                     WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED') \
                     AND completed_at < datetime('now', '-' || ?1 || ' seconds')",
                )
                .bind(self.config.retention_secs as i64)
                .execute(pool)
                .await?;
                result.rows_affected()
            }
        };

        if deleted > 0 {
            tracing::info!(count = deleted, "Cleaned up old jobs");
        }
        Ok(deleted)
    }

    // ── Read operations ───────────────────────────────────────────────────────

    pub async fn queue_depth(&self) -> Result<i64, JobError> {
        let mut conn = self
            .redis
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                JobError::ProcessingFailed(format!("Failed to get Redis connection: {}", e))
            })?;

        let depth: i64 = conn
            .llen("soroscope:jobs:queue")
            .await
            .map_err(|e| JobError::ProcessingFailed(format!("Redis LLEN failed: {}", e)))?;

        Ok(depth)
    }

    pub async fn get(&self, id: &JobId) -> Result<Option<Job>, JobError> {
        let job = match &self.pool {
            DbPool::Postgres(pool) => {
                sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = $1")
                    .bind(id)
                    .fetch_optional(pool)
                    .await?
            }
            DbPool::Sqlite(pool) => {
                let row = sqlx::query("SELECT * FROM jobs WHERE id = ?1")
                    .bind(id.0.to_string())
                    .fetch_optional(pool)
                    .await?;

                row.map(|r| self.row_to_job(&r)).transpose()?
            }
        };

        Ok(job)
    }

    pub async fn get_next_queued(&self) -> Result<Option<Job>, JobError> {
        let job = match &self.pool {
            DbPool::Postgres(pool) => sqlx::query_as::<_, Job>(
                "SELECT * FROM jobs WHERE status = 'QUEUED' ORDER BY created_at ASC LIMIT 1",
            )
            .fetch_optional(pool)
            .await?,
            DbPool::Sqlite(pool) => {
                let row = sqlx::query(
                    "SELECT * FROM jobs WHERE status = 'QUEUED' ORDER BY created_at ASC LIMIT 1",
                )
                .fetch_optional(pool)
                .await?;

                row.map(|r| self.row_to_job(&r)).transpose()?
            }
        };

        Ok(job)
    }

    pub async fn list(
        &self,
        filter: &JobListFilter,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Job>, JobError> {
        let limit = limit.clamp(1, 200);
        let offset = offset.max(0);

        let mut jobs = match &self.pool {
            DbPool::Postgres(pool) => match (&filter.status, &filter.job_type) {
                (Some(status), Some(job_type)) => sqlx::query_as::<_, Job>(
                    "SELECT * FROM jobs WHERE status = $1 AND job_type = $2 \
                     ORDER BY created_at DESC LIMIT $3 OFFSET $4",
                )
                .bind(status)
                .bind(job_type)
                .bind(limit)
                .bind(offset)
                .fetch_all(pool)
                .await?,
                (Some(status), None) => sqlx::query_as::<_, Job>(
                    "SELECT * FROM jobs WHERE status = $1 \
                     ORDER BY created_at DESC LIMIT $2 OFFSET $3",
                )
                .bind(status)
                .bind(limit)
                .bind(offset)
                .fetch_all(pool)
                .await?,
                (None, Some(job_type)) => sqlx::query_as::<_, Job>(
                    "SELECT * FROM jobs WHERE job_type = $1 \
                     ORDER BY created_at DESC LIMIT $2 OFFSET $3",
                )
                .bind(job_type)
                .bind(limit)
                .bind(offset)
                .fetch_all(pool)
                .await?,
                (None, None) => sqlx::query_as::<_, Job>(
                    "SELECT * FROM jobs ORDER BY created_at DESC LIMIT $1 OFFSET $2",
                )
                .bind(limit)
                .bind(offset)
                .fetch_all(pool)
                .await?,
            },
            DbPool::Sqlite(pool) => {
                let rows = match (&filter.status, &filter.job_type) {
                    (Some(status), Some(job_type)) => sqlx::query(
                        "SELECT * FROM jobs WHERE status = ?1 AND job_type = ?2 \
                         ORDER BY created_at DESC LIMIT ?3 OFFSET ?4",
                    )
                    .bind(status)
                    .bind(job_type)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(pool)
                    .await?,
                    (Some(status), None) => sqlx::query(
                        "SELECT * FROM jobs WHERE status = ?1 \
                         ORDER BY created_at DESC LIMIT ?2 OFFSET ?3",
                    )
                    .bind(status)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(pool)
                    .await?,
                    (None, Some(job_type)) => sqlx::query(
                        "SELECT * FROM jobs WHERE job_type = ?1 \
                         ORDER BY created_at DESC LIMIT ?2 OFFSET ?3",
                    )
                    .bind(job_type)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(pool)
                    .await?,
                    (None, None) => sqlx::query(
                        "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?1 OFFSET ?2",
                    )
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(pool)
                    .await?,
                };

                rows.iter()
                    .map(|row| self.row_to_job(row))
                    .collect::<Result<Vec<_>, _>>()?
            }
        };

        if let Some(contract_id) = &filter.contract_id {
            jobs.retain(|job| {
                job.get_payload()
                    .and_then(|p| p.contract_id().map(str::to_string))
                    .as_deref()
                    == Some(contract_id.as_str())
            });
        }

        Ok(jobs)
    }

    // ── SQLite row mapper ─────────────────────────────────────────────────────

    fn row_to_job(&self, row: &sqlx::sqlite::SqliteRow) -> Result<Job, JobError> {
        use sqlx::Row;

        let id_str: String = row.try_get("id")?;
        let id = JobId(
            Uuid::parse_str(&id_str)
                .map_err(|_| JobError::ProcessingFailed("Invalid UUID".to_string()))?,
        );

        let job_type_str: String = row.try_get("job_type")?;
        let job_type = match job_type_str.as_str() {
            "analyze" => JobType::Analyze,
            "compare" => JobType::Compare,
            "optimize_limits" => JobType::OptimizeLimits,
            other => {
                return Err(JobError::ProcessingFailed(format!(
                    "Unknown job_type '{}'",
                    other
                )))
            }
        };

        let status_str: String = row.try_get("status")?;
        let status = match status_str.as_str() {
            "QUEUED" => JobStatus::Queued,
            "PROCESSING" => JobStatus::Processing,
            "COMPLETED" => JobStatus::Completed,
            "FAILED" => JobStatus::Failed,
            "CANCELLED" => JobStatus::Cancelled,
            other => {
                return Err(JobError::ProcessingFailed(format!(
                    "Unknown status '{}'",
                    other
                )))
            }
        };

        Ok(Job {
            id,
            job_type,
            status,
            payload: row.try_get("payload").unwrap_or_default(),
            result: row.try_get("result")?,
            progress_percent: row.try_get("progress_percent")?,
            progress_message: row.try_get("progress_message")?,
            webhook_url: row.try_get("webhook_url")?,
            webhook_headers: row.try_get("webhook_headers")?,
            webhook_secret: row.try_get("webhook_secret")?,
            error_message: row.try_get("error_message")?,
            error_type: row.try_get("error_type")?,
            timeout_secs: row.try_get("timeout_secs")?,
            retry_count: row.try_get("retry_count")?,
            created_at: row.try_get("created_at")?,
            started_at: row.try_get("started_at")?,
            completed_at: row.try_get("completed_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl Clone for JobQueue {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            redis: self.redis.clone(),
            config: self.config.clone(),
            retry_dispatcher: self.retry_dispatcher.clone(),
        }
    }
}

// ── HTTP request / response DTOs ──────────────────────────────────────────────

#[derive(Debug, Deserialize, ToSchema)]
pub struct SubmitJobRequest {
    pub job_type: JobType,
    pub payload: JobPayload,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook: Option<WebhookConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SubmitJobResponse {
    pub job_id: String,
    pub status: JobStatus,
    pub message: String,
}

// ── Axum HTTP handlers ────────────────────────────────────────────────────────

#[utoipa::path(
    post,
    path = "/jobs/submit",
    request_body = SubmitJobRequest,
    responses(
        (status = 202, description = "Job accepted", body = SubmitJobResponse),
        (status = 500, description = "Internal server error")
    ),
    tag = "Jobs"
)]
pub async fn submit_job_handler(
    State(state): State<Arc<crate::AppState>>,
    Json(payload): Json<SubmitJobRequest>,
) -> Result<(StatusCode, Json<SubmitJobResponse>), AppError> {
    let job_id = state
        .job_queue
        .submit(payload.job_type, payload.payload, payload.webhook)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok((
        StatusCode::ACCEPTED,
        Json(SubmitJobResponse {
            job_id: job_id.to_string(),
            status: JobStatus::Queued,
            message: "Job submitted successfully".to_string(),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/jobs/{id}",
    responses(
        (status = 200, description = "Job details", body = Job),
        (status = 404, description = "Job not found")
    ),
    params(
        ("id" = String, Path, description = "Job ID")
    ),
    tag = "Jobs"
)]
pub async fn get_job_handler(
    State(state): State<Arc<crate::AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Job>, AppError> {
    let job_id =
        JobId::from_str(&id).map_err(|_| AppError::BadRequest("Invalid job ID".into()))?;
    let job = state
        .job_queue
        .get(&job_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound(format!("Job {} not found", id)))?;

    Ok(Json(job))
}

#[utoipa::path(
    post,
    path = "/jobs/{id}/cancel",
    responses(
        (status = 200, description = "Job cancelled", body = Job),
        (status = 400, description = "Job cannot be cancelled"),
        (status = 404, description = "Job not found")
    ),
    params(
        ("id" = String, Path, description = "Job ID")
    ),
    tag = "Jobs"
)]
pub async fn cancel_job_handler(
    State(state): State<Arc<crate::AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Job>, AppError> {
    let job_id =
        JobId::from_str(&id).map_err(|_| AppError::BadRequest("Invalid job ID".into()))?;
    let job = state
        .job_queue
        .cancel(&job_id)
        .await
        .map_err(|e| match e {
            JobError::NotFound(_) => AppError::NotFound(format!("Job {} not found", id)),
            JobError::CannotCancel(_) => AppError::BadRequest(e.to_string()),
            _ => AppError::Internal(e.to_string()),
        })?;

    Ok(Json(job))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use redis::Client as RedisClient;

    async fn sqlite_pool_with_jobs_table() -> sqlx::SqlitePool {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite pool");
        sqlx::query(
            r#"
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'QUEUED',
                payload TEXT NOT NULL,
                result TEXT,
                progress_percent INTEGER NOT NULL DEFAULT 0,
                progress_message TEXT NOT NULL DEFAULT 'Queued',
                webhook_url TEXT,
                webhook_headers TEXT,
                webhook_secret TEXT,
                error_message TEXT,
                error_type TEXT,
                timeout_secs INTEGER NOT NULL DEFAULT 300,
                retry_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                updated_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await
        .expect("create jobs table");
        pool
    }

    fn test_queue(pool: sqlx::SqlitePool) -> JobQueue {
        JobQueue {
            pool: DbPool::Sqlite(pool),
            redis: RedisClient::open("redis://127.0.0.1:6379").expect("parse redis url"),
            config: JobQueueConfig::default(),
            retry_dispatcher: crate::task_queue::BoundedTaskDispatcher::new(8),
        }
    }

    async fn insert_job(
        pool: &sqlx::SqlitePool,
        job_type: &JobType,
        status: &str,
        payload: &JobPayload,
    ) -> JobId {
        let id = JobId::new();
        let now = Utc::now().to_rfc3339();
        let payload_json = serde_json::to_value(payload).unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, job_type, status, payload, progress_message, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, 'Queued', ?5, ?5)",
        )
        .bind(id.0.to_string())
        .bind(job_type)
        .bind(status)
        .bind(payload_json)
        .bind(now)
        .execute(pool)
        .await
        .expect("insert job");
        id
    }

    fn analyze_payload(contract_id: &str) -> JobPayload {
        JobPayload::Analyze {
            contract_id: contract_id.to_string(),
            function_name: "hello".to_string(),
            args: None,
            ledger_overrides: None,
        }
    }

    #[tokio::test]
    async fn list_round_trips_job_type_and_status_from_sqlite() {
        let pool = sqlite_pool_with_jobs_table().await;
        insert_job(
            &pool,
            &JobType::OptimizeLimits,
            "COMPLETED",
            &JobPayload::OptimizeLimits {
                contract_id: "CABC".into(),
                function_name: "swap".into(),
                args: vec![],
                safety_margin: 0.05,
            },
        )
        .await;
        let queue = test_queue(pool);

        let jobs = queue
            .list(&JobListFilter::default(), 10, 0)
            .await
            .expect("list should succeed");

        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].job_type, JobType::OptimizeLimits);
        assert_eq!(jobs[0].status, JobStatus::Completed);
    }

    #[tokio::test]
    async fn list_filters_by_status_and_job_type() {
        let pool = sqlite_pool_with_jobs_table().await;
        insert_job(&pool, &JobType::Analyze, "COMPLETED", &analyze_payload("CABC")).await;
        insert_job(&pool, &JobType::Analyze, "FAILED", &analyze_payload("CXYZ")).await;
        let queue = test_queue(pool);

        let completed = queue
            .list(
                &JobListFilter {
                    status: Some(JobStatus::Completed),
                    ..Default::default()
                },
                10,
                0,
            )
            .await
            .unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].status, JobStatus::Completed);
    }

    #[tokio::test]
    async fn list_filters_by_contract_id() {
        let pool = sqlite_pool_with_jobs_table().await;
        insert_job(&pool, &JobType::Analyze, "COMPLETED", &analyze_payload("CABC")).await;
        insert_job(&pool, &JobType::Analyze, "COMPLETED", &analyze_payload("CXYZ")).await;
        let queue = test_queue(pool);

        let filtered = queue
            .list(
                &JobListFilter {
                    contract_id: Some("CXYZ".to_string()),
                    ..Default::default()
                },
                10,
                0,
            )
            .await
            .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered[0].get_payload().unwrap().contract_id(),
            Some("CXYZ".to_string()).as_deref()
        );
    }

    #[tokio::test]
    async fn list_respects_limit_and_offset() {
        let pool = sqlite_pool_with_jobs_table().await;
        for i in 0..5 {
            insert_job(
                &pool,
                &JobType::Analyze,
                "COMPLETED",
                &analyze_payload(&format!("C{i}")),
            )
            .await;
        }
        let queue = test_queue(pool);

        let page = queue.list(&JobListFilter::default(), 2, 1).await.unwrap();
        assert_eq!(page.len(), 2);
    }

    #[test]
    fn job_payload_contract_id_and_function_name_accessors() {
        let analyze = analyze_payload("CABC");
        assert_eq!(analyze.contract_id(), Some("CABC"));
        assert_eq!(analyze.function_name(), Some("hello"));

        let compare_local = JobPayload::Compare {
            mode: "local_vs_local".into(),
            current_wasm: None,
            base_wasm: None,
            contract_id: None,
            function_name: None,
            args: vec![],
        };
        assert_eq!(compare_local.contract_id(), None);
    }

    #[tokio::test]
    async fn job_state_transitions_complete_predictably_with_channels() {
        let pool = sqlite_pool_with_jobs_table().await;
        let queue = test_queue(pool.clone());
        let job_id =
            insert_job(&pool, &JobType::Analyze, "QUEUED", &analyze_payload("CABC")).await;

        let (processing_tx, processing_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();

        let queue_clone = queue.clone();
        let task_id = job_id;
        tokio::spawn(async move {
            queue_clone.mark_processing(&task_id).await.unwrap();
            let _ = processing_tx.send(());

            let _ = continue_rx.await;
            queue_clone
                .update_progress(&task_id, 50, "Halfway done")
                .await
                .unwrap();
            let result = JobResult::Success {
                resources: None,
                simulation_result: None,
                optimization: None,
                comparison: None,
            };
            queue_clone.complete(&task_id, &result).await.unwrap();
            let _ = completed_tx.send(());
        });

        processing_rx.await.expect("processing signal received");
        let job = queue.get(&job_id).await.unwrap().expect("job exists");
        assert_eq!(job.status, JobStatus::Processing);
        assert_eq!(job.progress_percent, 10);
        assert_eq!(job.progress_message, "Processing started");

        let _ = continue_tx.send(());
        completed_rx.await.expect("completed signal received");

        let finished_job = queue.get(&job_id).await.unwrap().expect("job exists");
        assert_eq!(finished_job.status, JobStatus::Completed);
        assert_eq!(finished_job.progress_percent, 100);
        assert_eq!(finished_job.progress_message, "Completed");
        assert!(finished_job.result.is_some());
    }

    #[tokio::test]
    async fn job_cancellation_lifecycle_deterministic() {
        let pool = sqlite_pool_with_jobs_table().await;
        let queue = test_queue(pool.clone());
        let job_id =
            insert_job(&pool, &JobType::Analyze, "QUEUED", &analyze_payload("CABC")).await;

        let cancelled = queue.cancel(&job_id).await.expect("cancellation succeeds");
        assert_eq!(cancelled.status, JobStatus::Cancelled);

        let err = queue.cancel(&job_id).await;
        assert!(matches!(
            err,
            Err(JobError::CannotCancel(JobStatus::Cancelled))
        ));
    }

    #[tokio::test]
    async fn job_failure_state_transition() {
        let pool = sqlite_pool_with_jobs_table().await;
        let queue = test_queue(pool.clone());
        let job_id = insert_job(
            &pool,
            &JobType::Analyze,
            "PROCESSING",
            &analyze_payload("CABC"),
        )
        .await;

        queue
            .fail(&job_id, "Simulation host error", "HostTrap")
            .await
            .expect("fail succeeds");

        let failed_job = queue.get(&job_id).await.unwrap().expect("job exists");
        assert_eq!(failed_job.status, JobStatus::Failed);
        assert_eq!(
            failed_job.error_message,
            Some("Simulation host error".to_string())
        );
        assert_eq!(failed_job.error_type, Some("HostTrap".to_string()));
    }

    #[tokio::test]
    async fn cleanup_task_lifecycle_with_shutdown_signal() {
        let pool = sqlite_pool_with_jobs_table().await;
        let queue = test_queue(pool);
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        let handle = {
            use crate::jobs::scheduler::SchedulerExt;
            queue.spawn_cleanup_task(shutdown_rx)
        };

        let _ = shutdown_tx.send(());

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        assert!(
            result.is_ok(),
            "cleanup task must exit promptly upon shutdown signal"
        );
    }
}
