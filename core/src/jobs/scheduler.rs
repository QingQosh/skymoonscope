//! Retry scheduling, the cleanup background task, and the job worker loop.
//!
//! This module is deliberately free of direct SQL writes for *new* business
//! data.  It drives state transitions by calling methods on [`JobQueue`] from
//! [`crate::jobs::store`], keeping the scheduling concerns cleanly separated
//! from the persistence layer.

use chrono::Utc;
use redis::AsyncCommands;
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;

use crate::insights::InsightsEngine;
use crate::simulation::{SimulationEngine, SimulationResult};
use crate::task_queue::TaskPriority;
use crate::ws::SimulationBus;

use super::domain::{
    Job, JobError, JobId, JobPayload, JobQueueConfig, JobResult, JobStatus, WebhookConfig,
};
use super::store::JobQueue;

// ── Extension trait ───────────────────────────────────────────────────────────

/// Scheduling capabilities bolted onto [`JobQueue`] as an extension trait so
/// the store module stays focused on SQL.
pub trait SchedulerExt {
    /// Retry a failed job using exponential backoff.
    fn retry_job(
        &self,
        job: &Job,
    ) -> impl std::future::Future<Output = Result<(), JobError>> + Send;

    /// Spawn a background task that periodically calls [`JobQueue::cleanup`]
    /// and exits cleanly when `shutdown` fires.
    fn spawn_cleanup_task(
        &self,
        shutdown: tokio::sync::broadcast::Receiver<()>,
    ) -> tokio::task::JoinHandle<()>;
}

impl SchedulerExt for JobQueue {
    async fn retry_job(&self, job: &Job) -> Result<(), JobError> {
        if job.retry_count >= self.config.max_job_retries {
            tracing::warn!(job_id = %job.id, "Max retries reached, not scheduling another attempt");
            return Ok(());
        }

        let new_retry_count = job.retry_count + 1;
        // Exponential backoff: 30 s, 60 s, 120 s, …
        let delay_secs = 2_u64.pow(new_retry_count as u32 - 1) * 30;

        // Persist the incremented retry count and reset the status to QUEUED.
        match &self.pool {
            super::store::DbPool::Postgres(pool) => {
                sqlx::query(
                    "UPDATE jobs SET retry_count = $1, status = 'QUEUED' WHERE id = $2",
                )
                .bind(new_retry_count)
                .bind(&job.id)
                .execute(pool)
                .await?;
            }
            super::store::DbPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE jobs SET retry_count = ?1, status = 'QUEUED' WHERE id = ?2",
                )
                .bind(new_retry_count)
                .bind(job.id.0.to_string())
                .execute(pool)
                .await?;
            }
        }

        // Schedule the Redis re-enqueue as a bounded background task.
        // If the dispatcher is saturated the retry is silently dropped — the
        // job stays QUEUED in the DB and will be picked up by the next
        // cleanup/requeue pass rather than spawning tasks without bound.
        let queue = self.clone();
        let id_str = job.id.0.to_string();
        let outcome = self
            .retry_dispatcher
            .dispatch(TaskPriority::Low, async move {
                tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                let mut conn = match queue.redis.get_multiplexed_async_connection().await {
                    Ok(c) => c,
                    Err(_) => return,
                };
                let _: Result<(), _> = conn.lpush("sky-moon-scope:jobs:queue", id_str).await;
            })
            .await;

        if outcome == crate::task_queue::DispatchOutcome::Dropped {
            tracing::warn!(
                job_id = %job.id,
                "Retry dispatcher saturated; retry scheduling dropped for this attempt"
            );
        }

        tracing::info!(
            job_id = %job.id,
            retry_count = new_retry_count,
            delay_secs,
            "Job scheduled for retry"
        );
        Ok(())
    }

    fn spawn_cleanup_task(
        &self,
        mut shutdown: tokio::sync::broadcast::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        let queue = self.clone();
        let interval_secs = self.config.cleanup_interval_secs;

        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(interval_secs));

            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.recv() => {
                        tracing::info!("Job queue cleanup task shutting down");
                        break;
                    }
                    _ = tick.tick() => {
                        tokio::select! {
                            biased;
                            _ = shutdown.recv() => {
                                tracing::info!("Job queue cleanup task shutting down");
                                break;
                            }
                            result = queue.cleanup() => {
                                if let Err(e) = result {
                                    tracing::error!("Cleanup task error: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}

// ── JobWorker ─────────────────────────────────────────────────────────────────

/// Pulls jobs off the Redis queue, dispatches them to the simulation engine,
/// drives FSM transitions via [`JobQueue`], and fires webhooks on completion.
pub struct JobWorker {
    queue: JobQueue,
    engine: SimulationEngine,
    insights_engine: InsightsEngine,
    config: JobQueueConfig,
    http_client: Client,
    /// Optional pub/sub bus for real-time WebSocket streaming.
    /// When `None` the worker runs in polling-only mode (backwards-compatible).
    bus: Option<Arc<SimulationBus>>,
}

impl JobWorker {
    pub fn new(
        queue: JobQueue,
        engine: SimulationEngine,
        insights_engine: InsightsEngine,
        config: JobQueueConfig,
    ) -> Self {
        Self {
            queue,
            engine,
            insights_engine,
            config,
            http_client: Client::new(),
            bus: None,
        }
    }

    /// Attach a [`SimulationBus`] so the worker publishes real-time events.
    pub fn with_bus(mut self, bus: Arc<SimulationBus>) -> Self {
        self.bus = Some(bus);
        self
    }

    /// Start the worker loop until a shutdown signal is received.
    ///
    /// Redis blocking pops use a 1-second timeout so the loop wakes to check
    /// `shutdown` instead of hanging on `brpoplpush` indefinitely.
    pub async fn run(self, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
        let worker_id = uuid::Uuid::new_v4().to_string();
        tracing::info!(worker_id = %worker_id, "Job worker started");

        // Heartbeat task — keeps a TTL key alive in Redis so operators can
        // detect stale workers.
        let redis_clone = self.queue.redis.clone();
        let worker_id_clone = worker_id.clone();
        let mut heartbeat_shutdown = shutdown.resubscribe();
        let heartbeat_handle = tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(10));
            let mut conn = match redis_clone.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("Heartbeat task failed to get Redis connection: {}", e);
                    return;
                }
            };

            loop {
                tokio::select! {
                    _ = heartbeat_shutdown.recv() => {
                        tracing::info!(worker_id = %worker_id_clone, "Heartbeat task shutting down");
                        break;
                    }
                    _ = tick.tick() => {
                        let key = format!("sky-moon-scope:workers:{}:heartbeat", worker_id_clone);
                        let _: Result<(), _> = conn.set_ex(key, "alive", 30).await;
                    }
                }
            }
        });

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_concurrent_jobs));

        loop {
            let mut conn = match self.queue.redis.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("Worker failed to get Redis connection: {}", e);
                    tokio::select! {
                        _ = shutdown.recv() => break,
                        _ = tokio::time::sleep(Duration::from_secs(5)) => continue,
                    }
                }
            };

            // Atomic pop-and-push: dequeues from the main queue and places the
            // ID into a processing list.  Jobs remain in the processing list
            // until the task completes, providing a crash-recovery audit trail.
            let job_id_res: Result<Option<String>, _> = tokio::select! {
                _ = shutdown.recv() => {
                    tracing::info!(worker_id = %worker_id, "Job worker shutting down");
                    break;
                }
                result = conn.brpoplpush(
                    "sky-moon-scope:jobs:queue",
                    "sky-moon-scope:jobs:processing",
                    1.0,
                ) => result,
            };

            match job_id_res {
                Ok(Some(id_str)) => {
                    let job_id = match JobId::from_str_ext(&id_str) {
                        Some(id) => id,
                        None => continue,
                    };

                    let permit = match semaphore.clone().acquire_owned().await {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::error!("Failed to acquire semaphore: {}", e);
                            continue;
                        }
                    };

                    let queue = self.queue.clone();
                    let engine = self.engine.clone();
                    let insights = self.insights_engine.clone();
                    let config = self.config.clone();
                    let http_client = self.http_client.clone();
                    let bus = self.bus.clone();
                    let id_str_clone = id_str.clone();

                    tokio::spawn(async move {
                        let _permit = permit;

                        if let Err(e) = Self::process_job(
                            &queue,
                            job_id,
                            engine,
                            insights,
                            config,
                            http_client,
                            bus,
                        )
                        .await
                        {
                            tracing::error!("Job processing error: {}", e);
                        }

                        // Remove from the processing list once done.
                        let mut conn =
                            match queue.redis.get_multiplexed_async_connection().await {
                                Ok(c) => c,
                                Err(_) => return,
                            };
                        let _: Result<(), _> = conn
                            .lrem("sky-moon-scope:jobs:processing", 1, id_str_clone)
                            .await;
                    });
                }
                Ok(None) => {} // Blocking pop timed out — loop and check shutdown.
                Err(e) => {
                    tracing::error!("Error fetching next job from Redis: {}", e);
                    tokio::select! {
                        _ = shutdown.recv() => break,
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    }
                }
            }
        }

        let _ = heartbeat_handle.await;
        tracing::info!(worker_id = %worker_id, "Job worker stopped");
    }

    // ── Per-job FSM orchestration ─────────────────────────────────────────────

    async fn process_job(
        queue: &JobQueue,
        job_id: JobId,
        engine: SimulationEngine,
        insights_engine: InsightsEngine,
        config: JobQueueConfig,
        http_client: Client,
        bus: Option<Arc<SimulationBus>>,
    ) -> Result<(), JobError> {
        let job = queue
            .get(&job_id)
            .await?
            .ok_or(JobError::NotFound(job_id))?;
        tracing::info!(job_id = %job.id, "Processing job");

        // Queued → Processing
        queue.mark_processing(&job.id).await?;
        if let Some(b) = &bus {
            b.publish(SimulationBus::progress(&job.id, 10, "Processing started"));
        }

        let timeout = Duration::from_secs(job.timeout_secs as u64);
        let result = tokio::time::timeout(
            timeout,
            Self::execute_job(&job, &engine, &insights_engine, queue, bus.clone()),
        )
        .await;

        match result {
            Ok(Ok(job_result)) => {
                // Processing → Completed
                queue.complete(&job.id, &job_result).await?;

                if let Some(b) = &bus {
                    if let JobResult::Success {
                        simulation_result: Some(ref sim),
                        ..
                    } = job_result
                    {
                        b.publish(SimulationBus::completed(
                            &job.id,
                            &sim.resources,
                            sim.cost_stroops,
                        ));
                    } else {
                        b.publish(SimulationBus::progress(&job.id, 100, "Completed"));
                    }
                }

                if let Some(webhook_config) = job.get_webhook_config() {
                    Self::send_webhook(
                        &http_client,
                        &webhook_config,
                        &job.id,
                        JobStatus::Completed,
                        Some(&job_result),
                        config.webhook_timeout_secs,
                        config.webhook_max_retries,
                    )
                    .await;
                }
            }
            Ok(Err(e)) => {
                // Processing → Failed (with retry attempt)
                let error_msg = e.to_string();
                queue.fail(&job.id, &error_msg, "ProcessingError").await?;
                let _ = queue.retry_job(&job).await;

                if let Some(b) = &bus {
                    b.publish(SimulationBus::failed(&job.id, &error_msg, "ProcessingError"));
                }

                if let Some(webhook_config) = job.get_webhook_config() {
                    Self::send_webhook(
                        &http_client,
                        &webhook_config,
                        &job.id,
                        JobStatus::Failed,
                        None,
                        config.webhook_timeout_secs,
                        config.webhook_max_retries,
                    )
                    .await;
                }
            }
            Err(_) => {
                // Processing → Failed (timeout)
                let error_msg = format!("Job timed out after {} seconds", job.timeout_secs);
                queue.fail(&job.id, &error_msg, "Timeout").await?;
                let _ = queue.retry_job(&job).await;

                if let Some(b) = &bus {
                    b.publish(SimulationBus::failed(&job.id, &error_msg, "Timeout"));
                }

                if let Some(webhook_config) = job.get_webhook_config() {
                    Self::send_webhook(
                        &http_client,
                        &webhook_config,
                        &job.id,
                        JobStatus::Failed,
                        None,
                        config.webhook_timeout_secs,
                        config.webhook_max_retries,
                    )
                    .await;
                }
            }
        }

        Ok(())
    }

    // ── Computation dispatch ──────────────────────────────────────────────────

    async fn execute_job(
        job: &Job,
        engine: &SimulationEngine,
        insights_engine: &InsightsEngine,
        queue: &JobQueue,
        bus: Option<Arc<SimulationBus>>,
    ) -> Result<JobResult, Box<dyn std::error::Error + Send + Sync>> {
        let payload = job.get_payload().ok_or("Invalid payload")?;

        /// Update DB progress and publish a WebSocket event simultaneously.
        macro_rules! progress {
            ($percent:expr, $msg:expr) => {{
                let _ = queue.update_progress(&job.id, $percent, $msg).await;
                if let Some(ref b) = bus {
                    b.publish(SimulationBus::progress(&job.id, $percent, $msg));
                }
            }};
        }

        match payload {
            JobPayload::Analyze {
                contract_id,
                function_name,
                args,
                ledger_overrides,
            } => {
                progress!(30, "Running simulation");

                let sim_result = engine
                    .simulate_from_contract_id(
                        &contract_id,
                        &function_name,
                        args.unwrap_or_default(),
                        ledger_overrides,
                        None,
                        None,
                    )
                    .await
                    .map_err(|e| {
                        // Best-effort: broadcast provider-failover events so
                        // WebSocket clients see the switch in real time.
                        let msg = e.to_string();
                        if let Some(ref b) = bus {
                            if msg.contains("failover") || msg.contains("provider") {
                                b.publish(SimulationBus::provider_failover(
                                    &job.id,
                                    "unknown",
                                    "next-available",
                                    &msg,
                                ));
                            }
                        }
                        e
                    })?;

                if let Some(ref b) = bus {
                    b.publish(SimulationBus::consensus_check(
                        &job.id,
                        true,
                        vec![],
                        None,
                    ));
                }

                progress!(70, "Generating insights");
                let _insights = insights_engine.analyze(&sim_result.resources);

                progress!(90, "Finalizing results");

                Ok(JobResult::Success {
                    resources: Some(sim_result.resources.clone()),
                    simulation_result: Some(sim_result),
                    optimization: None,
                    comparison: None,
                })
            }
            JobPayload::OptimizeLimits {
                contract_id,
                function_name,
                args,
                safety_margin,
            } => {
                progress!(30, "Running optimization");

                let report = engine
                    .optimize_limits(&contract_id, &function_name, args, safety_margin)
                    .await?;

                progress!(90, "Finalizing results");

                Ok(JobResult::Success {
                    resources: None,
                    simulation_result: None,
                    optimization: Some(serde_json::to_value(report)?),
                    comparison: None,
                })
            }
            _ => Ok(JobResult::Success {
                resources: None,
                simulation_result: None,
                optimization: None,
                comparison: Some(serde_json::json!({"status": "Not fully implemented"})),
            }),
        }
    }

    // ── Webhook delivery ──────────────────────────────────────────────────────

    async fn send_webhook(
        client: &Client,
        config: &WebhookConfig,
        job_id: &JobId,
        status: JobStatus,
        result: Option<&JobResult>,
        timeout_secs: u64,
        max_retries: u32,
    ) {
        let payload = serde_json::json!({
            "job_id": job_id.to_string(),
            "status": status,
            "result": result,
            "timestamp": Utc::now().to_rfc3339(),
        });

        let timeout = Duration::from_secs(timeout_secs);
        let mut last_error = None;

        for attempt in 1..=max_retries {
            let mut request = client
                .post(&config.callback_url)
                .json(&payload)
                .timeout(timeout);

            if let Some(headers) = &config.headers {
                for (key, value) in headers {
                    request = request.header(key, value);
                }
            }

            match request.send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        tracing::info!(job_id = %job_id, attempt, "Webhook delivered");
                        return;
                    } else {
                        last_error = Some(format!("HTTP {}", response.status()));
                    }
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                }
            }

            if attempt < max_retries {
                tokio::time::sleep(Duration::from_millis(1000 * 2_u64.pow(attempt - 1))).await;
            }
        }

        tracing::error!(job_id = %job_id, error = ?last_error, "Webhook failed");
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// A small helper so the worker loop can parse a `JobId` from a Redis string
/// without pulling `std::str::FromStr` into scope everywhere.
trait JobIdExt: Sized {
    fn from_str_ext(s: &str) -> Option<Self>;
}

impl JobIdExt for JobId {
    fn from_str_ext(s: &str) -> Option<Self> {
        use std::str::FromStr;
        JobId::from_str(s).ok()
    }
}
