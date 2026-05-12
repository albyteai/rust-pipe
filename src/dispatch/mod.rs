use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, RwLock};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::schema::{Task, TaskResult};
use crate::transport::websocket::WebSocketTransport;
use crate::transport::{Message, Transport, TransportConfig, TransportError};
use crate::worker::{PoolError, WorkerInfo, WorkerPool, WorkerStatus};

/// Builder for configuring a [`Dispatcher`].
pub struct DispatcherBuilder {
    config: TransportConfig,
    heartbeat_timeout_ms: u64,
    dead_worker_check_interval_ms: u64,
    max_pool_size: Option<u32>,
    min_pool_size: Option<u32>,
    on_pool_below_min: Option<Arc<dyn Fn(u32) + Send + Sync>>,
}

impl std::fmt::Debug for DispatcherBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatcherBuilder")
            .field("config", &self.config)
            .field("heartbeat_timeout_ms", &self.heartbeat_timeout_ms)
            .field(
                "dead_worker_check_interval_ms",
                &self.dead_worker_check_interval_ms,
            )
            .field("max_pool_size", &self.max_pool_size)
            .field("min_pool_size", &self.min_pool_size)
            .field("on_pool_below_min", &self.on_pool_below_min.is_some())
            .finish()
    }
}

impl Default for DispatcherBuilder {
    fn default() -> Self {
        Self {
            config: TransportConfig::default(),
            heartbeat_timeout_ms: 15_000,
            dead_worker_check_interval_ms: 5_000,
            max_pool_size: None,
            min_pool_size: None,
            on_pool_below_min: None,
        }
    }
}

impl DispatcherBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.config.host = host.into();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    pub fn max_connections(mut self, max: u32) -> Self {
        self.config.max_connections = max;
        self
    }

    pub fn heartbeat_interval(mut self, ms: u64) -> Self {
        self.config.heartbeat_interval_ms = ms;
        self
    }

    pub fn heartbeat_timeout(mut self, ms: u64) -> Self {
        self.heartbeat_timeout_ms = ms;
        self
    }

    /// Set the maximum number of workers allowed in the pool.
    /// Workers connecting beyond this limit will be rejected.
    pub fn max_pool_size(mut self, max: u32) -> Self {
        self.max_pool_size = Some(max);
        self
    }

    /// Set the minimum pool size. When the pool drops below this threshold,
    /// the `on_pool_below_min` callback is invoked.
    pub fn min_pool_size(mut self, min: u32) -> Self {
        self.min_pool_size = Some(min);
        self
    }

    /// Set a callback to be invoked when the pool size drops below `min_pool_size`.
    /// The callback receives the current pool size.
    pub fn on_pool_below_min(mut self, cb: impl Fn(u32) + Send + Sync + 'static) -> Self {
        self.on_pool_below_min = Some(Arc::new(cb));
        self
    }

    pub fn build(self) -> Dispatcher {
        Dispatcher {
            pool: Arc::new(WorkerPool::with_limits(
                self.heartbeat_timeout_ms,
                self.max_pool_size,
                self.min_pool_size,
                self.on_pool_below_min,
            )),
            pending: Arc::new(DashMap::new()),
            transport: Arc::new(RwLock::new(None)),
            config: self.config,
            dead_worker_check_interval_ms: self.dead_worker_check_interval_ms,
            started: AtomicBool::new(false),
            _dead_worker_task: RwLock::new(None),
        }
    }
}

/// Central task dispatcher. Manages worker connections and routes tasks.
pub struct Dispatcher {
    pool: Arc<WorkerPool>,
    pending: Arc<DashMap<Uuid, PendingTask>>,
    transport: Arc<RwLock<Option<Arc<WebSocketTransport>>>>,
    config: TransportConfig,
    dead_worker_check_interval_ms: u64,
    started: AtomicBool,
    _dead_worker_task: RwLock<Option<JoinHandle<()>>>,
}

struct PendingTask {
    sender: oneshot::Sender<TaskResult>,
    worker_id: String,
}

/// Handle to an in-flight task. Await it to get the worker's result.
#[must_use = "dropping a DispatchResult discards the task result"]
pub struct DispatchResult {
    pub task_id: Uuid,
    pub(crate) receiver: oneshot::Receiver<TaskResult>,
}

impl std::fmt::Debug for DispatchResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatchResult")
            .field("task_id", &self.task_id)
            .finish()
    }
}

impl DispatchResult {
    pub async fn await_result(self) -> Result<TaskResult, DispatchError> {
        self.receiver
            .await
            .map_err(|_| DispatchError::WorkerDisconnected)
    }

    pub async fn await_with_timeout(self, timeout: Duration) -> Result<TaskResult, DispatchError> {
        tokio::time::timeout(timeout, self.receiver)
            .await
            .map_err(|_| DispatchError::Timeout)?
            .map_err(|_| DispatchError::WorkerDisconnected)
    }
}

impl Dispatcher {
    pub fn builder() -> DispatcherBuilder {
        DispatcherBuilder::new()
    }

    pub async fn start(&self) -> Result<(), DispatchError> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let pool = self.pool.clone();
        let pending = self.pending.clone();

        let on_message = move |worker_id: String, message: Message| {
            let pool = pool.clone();
            let pending = pending.clone();

            tokio::spawn(async move {
                match message {
                    Message::WorkerRegister { registration: reg } => {
                        pool.register(WorkerInfo {
                            id: reg.worker_id,
                            language: reg.language,
                            supported_tasks: reg.supported_tasks,
                            max_concurrency: reg.max_concurrency,
                            status: WorkerStatus::Active,
                            active_tasks: 0,
                            registered_at: chrono::Utc::now(),
                            last_heartbeat: chrono::Utc::now(),
                            tags: reg.tags.unwrap_or_default(),
                        });
                    }
                    Message::TaskResult { result } => {
                        pool.mark_task_completed(&worker_id);
                        if let Some((_, pending_task)) = pending.remove(&result.task_id) {
                            let _ = pending_task.sender.send(result);
                        }
                    }
                    Message::Heartbeat { payload: hb } => {
                        pool.heartbeat(&hb.worker_id, hb.active_tasks);
                    }
                    Message::Backpressure { signal: bp } => {
                        tracing::warn!(
                            worker_id = %bp.worker_id,
                            load = bp.current_load,
                            "Worker signaled backpressure"
                        );
                    }
                    _ => {}
                }
            });
        };

        let transport = Arc::new(WebSocketTransport::new(self.config.clone(), on_message));
        transport
            .start()
            .await
            .map_err(DispatchError::TransportError)?;

        *self.transport.write().await = Some(transport);

        // Dead worker detection loop — drops pending senders for dead workers
        // so that await_result() returns WorkerDisconnected instead of hanging.
        let pool = self.pool.clone();
        let pending = self.pending.clone();
        let interval = self.dead_worker_check_interval_ms;
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(interval)).await;
                let dead = pool.detect_dead_workers();
                if !dead.is_empty() {
                    for worker_id in &dead {
                        tracing::warn!(worker_id = %worker_id, "Dead worker detected");
                    }
                    // Drop pending senders for dead workers — receiver gets RecvError
                    // which maps to DispatchError::WorkerDisconnected
                    pending
                        .retain(|_task_id, pending_task| !dead.contains(&pending_task.worker_id));
                }
            }
        });

        *self._dead_worker_task.write().await = Some(handle);

        Ok(())
    }

    /// Gracefully stops the dispatcher. Cancels background tasks and shuts down transport.
    pub async fn stop(&self) {
        self.started.store(false, Ordering::SeqCst);
        // Cancel the dead worker detection task
        if let Some(handle) = self._dead_worker_task.write().await.take() {
            handle.abort();
        }
        // Shut down transport
        if let Some(transport) = self.transport.read().await.as_ref() {
            let _ = transport.stop().await;
        }
        // Fail all pending tasks
        self.pending.clear();
    }

    pub async fn dispatch(&self, task: Task) -> Result<DispatchResult, DispatchError> {
        // select_and_reserve atomically picks a worker and increments active_tasks
        let worker_id = self.pool.select_and_reserve(&task.task_type).ok_or(
            DispatchError::NoWorkerAvailable {
                task_type: task.task_type.clone(),
            },
        )?;

        let (tx, rx) = oneshot::channel();
        let task_id = task.id;

        self.pending.insert(
            task_id,
            PendingTask {
                sender: tx,
                worker_id: worker_id.clone(),
            },
        );

        // Send task to worker via transport
        let transport_guard = self.transport.read().await;
        let transport = transport_guard.as_ref().ok_or_else(|| {
            // Rollback: remove pending and release worker capacity
            self.pending.remove(&task_id);
            self.pool.mark_task_completed(&worker_id);
            DispatchError::TransportNotStarted
        })?;

        if let Err(e) = transport
            .send(&worker_id, Message::TaskDispatch { task })
            .await
        {
            // Rollback: remove pending and release worker capacity
            self.pending.remove(&task_id);
            self.pool.mark_task_completed(&worker_id);
            return Err(DispatchError::TransportError(e));
        }

        tracing::debug!(task_id = %task_id, worker_id = %worker_id, "Task dispatched");

        Ok(DispatchResult {
            task_id,
            receiver: rx,
        })
    }

    pub fn pool_stats(&self) -> crate::worker::PoolStats {
        self.pool.stats()
    }

    /// List all connected workers with their full info.
    pub fn workers(&self) -> Vec<WorkerInfo> {
        self.pool.workers()
    }

    /// Set a worker's status to Draining. No new tasks will be routed to it,
    /// but existing in-flight tasks will finish normally.
    pub fn drain_worker(&self, worker_id: &str) -> Result<(), PoolError> {
        self.pool.drain_worker(worker_id)
    }

    /// Force-remove a worker from the pool and fail all pending tasks assigned to it.
    pub fn remove_worker(&self, worker_id: &str) -> Result<(), PoolError> {
        self.pool.remove_worker(worker_id)?;
        // Fail all pending tasks for this worker by dropping their senders
        self.pending
            .retain(|_task_id, pending_task| pending_task.worker_id != worker_id);
        Ok(())
    }

    /// Dispatch a task to a specific worker by ID, bypassing least-loaded selection.
    pub async fn dispatch_to(
        &self,
        worker_id: &str,
        task: Task,
    ) -> Result<DispatchResult, DispatchError> {
        self.pool.reserve_specific_worker(worker_id)?;

        let (tx, rx) = oneshot::channel();
        let task_id = task.id;

        self.pending.insert(
            task_id,
            PendingTask {
                sender: tx,
                worker_id: worker_id.to_string(),
            },
        );

        // Send task to worker via transport
        let transport_guard = self.transport.read().await;
        let transport = transport_guard.as_ref().ok_or_else(|| {
            self.pending.remove(&task_id);
            self.pool.mark_task_completed(worker_id);
            DispatchError::TransportNotStarted
        })?;

        if let Err(e) = transport
            .send(worker_id, Message::TaskDispatch { task })
            .await
        {
            self.pending.remove(&task_id);
            self.pool.mark_task_completed(worker_id);
            return Err(DispatchError::TransportError(e));
        }

        tracing::debug!(task_id = %task_id, worker_id = %worker_id, "Task dispatched to specific worker");

        Ok(DispatchResult {
            task_id,
            receiver: rx,
        })
    }

    /// Dispatch a task to a worker that has a matching tag.
    /// Routes to the least-loaded worker among those with the specified tag.
    pub async fn dispatch_with_tag(
        &self,
        tag: &str,
        task: Task,
    ) -> Result<DispatchResult, DispatchError> {
        let worker_id = self
            .pool
            .select_and_reserve_with_tag(tag, &task.task_type)
            .ok_or(DispatchError::NoWorkerAvailable {
                task_type: task.task_type.clone(),
            })?;

        let (tx, rx) = oneshot::channel();
        let task_id = task.id;

        self.pending.insert(
            task_id,
            PendingTask {
                sender: tx,
                worker_id: worker_id.clone(),
            },
        );

        // Send task to worker via transport
        let transport_guard = self.transport.read().await;
        let transport = transport_guard.as_ref().ok_or_else(|| {
            self.pending.remove(&task_id);
            self.pool.mark_task_completed(&worker_id);
            DispatchError::TransportNotStarted
        })?;

        if let Err(e) = transport
            .send(&worker_id, Message::TaskDispatch { task })
            .await
        {
            self.pending.remove(&task_id);
            self.pool.mark_task_completed(&worker_id);
            return Err(DispatchError::TransportError(e));
        }

        tracing::debug!(task_id = %task_id, worker_id = %worker_id, tag = %tag, "Task dispatched with tag");

        Ok(DispatchResult {
            task_id,
            receiver: rx,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("No worker available for task type: {task_type}")]
    NoWorkerAvailable { task_type: String },

    #[error("Worker disconnected before returning result")]
    WorkerDisconnected,

    #[error("Task timed out")]
    Timeout,

    #[error("Transport not started — call start() first")]
    TransportNotStarted,

    #[error("Transport error: {0}")]
    TransportError(#[from] TransportError),

    #[error("Pool error: {0}")]
    PoolError(#[from] PoolError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{TaskResult, TaskStatus};
    use serde_json::json;

    #[test]
    fn test_builder_default_port() {
        let builder = DispatcherBuilder::new();
        assert_eq!(builder.config.port, 9876);
    }

    #[test]
    fn test_builder_default_host() {
        let builder = DispatcherBuilder::new();
        assert_eq!(builder.config.host, "0.0.0.0");
    }

    #[test]
    fn test_builder_default_heartbeat_timeout() {
        let builder = DispatcherBuilder::new();
        assert_eq!(builder.heartbeat_timeout_ms, 15_000);
    }

    #[test]
    fn test_builder_host_sets_value() {
        let builder = DispatcherBuilder::new().host("10.0.0.1");
        assert_eq!(builder.config.host, "10.0.0.1");
    }

    #[test]
    fn test_builder_port_sets_value() {
        let builder = DispatcherBuilder::new().port(8080);
        assert_eq!(builder.config.port, 8080);
    }

    #[test]
    fn test_builder_max_connections_sets_value() {
        let builder = DispatcherBuilder::new().max_connections(50);
        assert_eq!(builder.config.max_connections, 50);
    }

    #[test]
    fn test_builder_heartbeat_interval_sets_value() {
        let builder = DispatcherBuilder::new().heartbeat_interval(2000);
        assert_eq!(builder.config.heartbeat_interval_ms, 2000);
    }

    #[test]
    fn test_builder_heartbeat_timeout_sets_value() {
        let builder = DispatcherBuilder::new().heartbeat_timeout(30000);
        assert_eq!(builder.heartbeat_timeout_ms, 30000);
    }

    #[test]
    fn test_builder_chaining() {
        let builder = DispatcherBuilder::new()
            .host("1.2.3.4")
            .port(9999)
            .max_connections(200)
            .heartbeat_interval(1000)
            .heartbeat_timeout(5000);
        assert_eq!(builder.config.host, "1.2.3.4");
        assert_eq!(builder.config.port, 9999);
        assert_eq!(builder.config.max_connections, 200);
        assert_eq!(builder.config.heartbeat_interval_ms, 1000);
        assert_eq!(builder.heartbeat_timeout_ms, 5000);
    }

    #[test]
    fn test_builder_build_pool_starts_empty() {
        let dispatcher = Dispatcher::builder().build();
        let stats = dispatcher.pool_stats();
        assert_eq!(stats.total, 0);
    }

    #[test]
    fn test_dispatcher_builder_shortcut() {
        let builder = Dispatcher::builder();
        assert_eq!(builder.config.port, 9876);
    }

    #[tokio::test]
    async fn test_dispatch_result_await_result_receives_value() {
        let (tx, rx) = oneshot::channel();
        let result = DispatchResult {
            task_id: Uuid::new_v4(),
            receiver: rx,
        };
        let task_result = TaskResult {
            task_id: result.task_id,
            status: TaskStatus::Completed,
            payload: Some(json!({"ok": true})),
            error: None,
            duration_ms: 50,
            worker_id: "test".to_string(),
        };
        tx.send(task_result.clone()).unwrap();
        let received = result.await_result().await.unwrap();
        assert_eq!(received.task_id, task_result.task_id);
        assert_eq!(received.status, TaskStatus::Completed);
    }

    #[tokio::test]
    async fn test_dispatch_result_worker_disconnected() {
        let (tx, rx) = oneshot::channel::<TaskResult>();
        let result = DispatchResult {
            task_id: Uuid::new_v4(),
            receiver: rx,
        };
        drop(tx);
        let err = result.await_result().await.unwrap_err();
        assert!(matches!(err, DispatchError::WorkerDisconnected));
    }

    #[tokio::test]
    async fn test_dispatch_result_timeout() {
        let (_tx, rx) = oneshot::channel::<TaskResult>();
        let result = DispatchResult {
            task_id: Uuid::new_v4(),
            receiver: rx,
        };
        let err = result
            .await_with_timeout(Duration::from_millis(10))
            .await
            .unwrap_err();
        assert!(matches!(err, DispatchError::Timeout));
    }

    #[test]
    fn test_dispatch_result_debug_format() {
        let (_tx, rx) = oneshot::channel::<TaskResult>();
        let id = Uuid::new_v4();
        let result = DispatchResult {
            task_id: id,
            receiver: rx,
        };
        let debug = format!("{:?}", result);
        assert!(debug.contains("DispatchResult"));
        assert!(debug.contains(&id.to_string()));
    }

    #[test]
    fn test_dispatch_error_display_no_worker() {
        let err = DispatchError::NoWorkerAvailable {
            task_type: "scan".into(),
        };
        assert_eq!(err.to_string(), "No worker available for task type: scan");
    }

    #[test]
    fn test_dispatch_error_display_worker_disconnected() {
        let err = DispatchError::WorkerDisconnected;
        assert_eq!(
            err.to_string(),
            "Worker disconnected before returning result"
        );
    }

    #[test]
    fn test_dispatch_error_display_timeout() {
        let err = DispatchError::Timeout;
        assert_eq!(err.to_string(), "Task timed out");
    }

    #[test]
    fn test_dispatch_error_display_transport_not_started() {
        let err = DispatchError::TransportNotStarted;
        assert!(err.to_string().contains("Transport not started"));
    }

    #[test]
    fn test_dispatch_error_from_transport_error() {
        let transport_err = TransportError::Closed;
        let dispatch_err: DispatchError = transport_err.into();
        assert!(matches!(
            dispatch_err,
            DispatchError::TransportError(TransportError::Closed)
        ));
    }

    // =========================================================================
    // Pool management builder tests
    // =========================================================================

    #[test]
    fn test_builder_max_pool_size() {
        let builder = DispatcherBuilder::new().max_pool_size(10);
        assert_eq!(builder.max_pool_size, Some(10));
    }

    #[test]
    fn test_builder_min_pool_size() {
        let builder = DispatcherBuilder::new().min_pool_size(2);
        assert_eq!(builder.min_pool_size, Some(2));
    }

    #[test]
    fn test_builder_on_pool_below_min() {
        let builder = DispatcherBuilder::new().on_pool_below_min(|_| {});
        assert!(builder.on_pool_below_min.is_some());
    }

    #[test]
    fn test_builder_pool_limits_chaining() {
        let builder = DispatcherBuilder::new()
            .max_pool_size(50)
            .min_pool_size(5)
            .on_pool_below_min(|_| {});
        assert_eq!(builder.max_pool_size, Some(50));
        assert_eq!(builder.min_pool_size, Some(5));
        assert!(builder.on_pool_below_min.is_some());
    }

    #[test]
    fn test_dispatcher_workers_empty() {
        let dispatcher = Dispatcher::builder().build();
        assert!(dispatcher.workers().is_empty());
    }

    #[test]
    fn test_dispatcher_drain_worker_not_found() {
        let dispatcher = Dispatcher::builder().build();
        let err = dispatcher.drain_worker("ghost").unwrap_err();
        assert!(matches!(err, PoolError::WorkerNotFound { .. }));
    }

    #[test]
    fn test_dispatcher_remove_worker_not_found() {
        let dispatcher = Dispatcher::builder().build();
        let err = dispatcher.remove_worker("ghost").unwrap_err();
        assert!(matches!(err, PoolError::WorkerNotFound { .. }));
    }

    #[test]
    fn test_builder_debug_format() {
        let builder = DispatcherBuilder::new()
            .max_pool_size(10)
            .min_pool_size(2)
            .on_pool_below_min(|_| {});
        let debug = format!("{:?}", builder);
        assert!(debug.contains("DispatcherBuilder"));
        assert!(debug.contains("max_pool_size"));
        assert!(debug.contains("min_pool_size"));
    }
}
