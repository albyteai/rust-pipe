/// Worker pool management and least-loaded selection.
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::transport::WorkerLanguage;

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Information about a connected worker.
pub struct WorkerInfo {
    pub id: String,
    pub language: WorkerLanguage,
    pub supported_tasks: Vec<String>,
    pub max_concurrency: u32,
    pub status: WorkerStatus,
    pub active_tasks: u32,
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    /// Tags/metadata for targeted dispatch routing.
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
/// Current status of a worker in the pool.
pub enum WorkerStatus {
    Active,
    Busy,
    Draining,
    Dead,
}

/// Errors related to pool management operations.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum PoolError {
    #[error("Pool is at maximum capacity ({max}), cannot register worker")]
    PoolFull { max: u32 },

    #[error("Worker not found: {worker_id}")]
    WorkerNotFound { worker_id: String },

    #[error("Worker '{worker_id}' is at capacity")]
    WorkerAtCapacity { worker_id: String },

    #[error("Worker '{worker_id}' is draining or dead")]
    WorkerUnavailable { worker_id: String },
}

/// Pool of connected workers with capacity-aware task routing.
pub struct WorkerPool {
    workers: DashMap<String, WorkerInfo>,
    heartbeat_timeout_ms: u64,
    max_pool_size: Option<u32>,
    min_pool_size: Option<u32>,
    on_pool_below_min: Option<Arc<dyn Fn(u32) + Send + Sync>>,
}

impl WorkerPool {
    pub fn new(heartbeat_timeout_ms: u64) -> Self {
        Self {
            workers: DashMap::new(),
            heartbeat_timeout_ms,
            max_pool_size: None,
            min_pool_size: None,
            on_pool_below_min: None,
        }
    }

    pub fn with_limits(
        heartbeat_timeout_ms: u64,
        max_pool_size: Option<u32>,
        min_pool_size: Option<u32>,
        on_pool_below_min: Option<Arc<dyn Fn(u32) + Send + Sync>>,
    ) -> Self {
        Self {
            workers: DashMap::new(),
            heartbeat_timeout_ms,
            max_pool_size,
            min_pool_size,
            on_pool_below_min,
        }
    }

    /// Register a worker. If the pool is at max capacity (and this is not a
    /// re-registration of an existing worker), the registration is silently rejected.
    /// Use [`try_register`](Self::try_register) for explicit error handling.
    pub fn register(&self, info: WorkerInfo) {
        if let Err(e) = self.try_register(info) {
            tracing::warn!(error = %e, "Worker registration rejected");
        }
    }

    /// Register a worker, returning an error if the pool is at max capacity.
    /// Re-registration of an existing worker (same ID) always succeeds (updates in place).
    pub fn try_register(&self, info: WorkerInfo) -> Result<(), PoolError> {
        if let Some(max) = self.max_pool_size {
            let is_reregistration = self.workers.contains_key(&info.id);
            if !is_reregistration && self.workers.len() as u32 >= max {
                return Err(PoolError::PoolFull { max });
            }
        }
        tracing::info!(
            worker_id = %info.id,
            language = ?info.language,
            tasks = ?info.supported_tasks,
            "Registering worker"
        );
        self.workers.insert(info.id.clone(), info);
        Ok(())
    }

    pub fn deregister(&self, worker_id: &str) {
        self.workers.remove(worker_id);
        self.check_below_min();
    }

    pub fn heartbeat(&self, worker_id: &str, active_tasks: u32) {
        if let Some(mut worker) = self.workers.get_mut(worker_id) {
            worker.last_heartbeat = Utc::now();
            worker.active_tasks = active_tasks;
            // Don't overwrite Draining status — drain must be explicit
            if worker.status != WorkerStatus::Draining {
                worker.status = if active_tasks >= worker.max_concurrency {
                    WorkerStatus::Busy
                } else {
                    WorkerStatus::Active
                };
            }
        }
    }

    /// Atomically selects a worker and reserves capacity.
    /// Returns the worker ID if one is available, or None.
    /// This avoids the TOCTOU race between select and dispatch.
    pub fn select_and_reserve(&self, task_type: &str) -> Option<String> {
        let mut best_id: Option<String> = None;
        let mut best_capacity: u32 = 0;

        for entry in self.workers.iter() {
            let worker = entry.value();
            if worker.status == WorkerStatus::Dead || worker.status == WorkerStatus::Draining {
                continue;
            }
            if !worker.supported_tasks.iter().any(|t| t == task_type) {
                continue;
            }
            if worker.active_tasks >= worker.max_concurrency {
                continue;
            }

            let available = worker.max_concurrency - worker.active_tasks;
            if best_id.is_none() || available > best_capacity {
                best_id = Some(worker.id.clone());
                best_capacity = available;
            }
        }

        let worker_id = best_id?;

        // Atomically increment under the entry lock
        if let Some(mut worker) = self.workers.get_mut(&worker_id) {
            if worker.active_tasks >= worker.max_concurrency {
                return None;
            }
            worker.active_tasks += 1;
            if worker.active_tasks >= worker.max_concurrency {
                worker.status = WorkerStatus::Busy;
            }
            Some(worker_id)
        } else {
            None
        }
    }

    /// Selects the least-loaded worker for a task type WITHOUT modifying state.
    /// Use `select_and_reserve` for dispatch to avoid TOCTOU races.
    pub fn select_worker(&self, task_type: &str) -> Option<String> {
        let mut best_id: Option<String> = None;
        let mut best_capacity: u32 = 0;

        for entry in self.workers.iter() {
            let worker = entry.value();
            if worker.status == WorkerStatus::Dead || worker.status == WorkerStatus::Draining {
                continue;
            }
            if !worker.supported_tasks.iter().any(|t| t == task_type) {
                continue;
            }
            if worker.active_tasks >= worker.max_concurrency {
                continue;
            }

            let available = worker.max_concurrency - worker.active_tasks;
            if best_id.is_none() || available > best_capacity {
                best_id = Some(worker.id.clone());
                best_capacity = available;
            }
        }

        best_id
    }

    pub fn mark_task_dispatched(&self, worker_id: &str) {
        if let Some(mut worker) = self.workers.get_mut(worker_id) {
            worker.active_tasks += 1;
            if worker.active_tasks >= worker.max_concurrency {
                worker.status = WorkerStatus::Busy;
            }
        }
    }

    pub fn mark_task_completed(&self, worker_id: &str) {
        if let Some(mut worker) = self.workers.get_mut(worker_id) {
            worker.active_tasks = worker.active_tasks.saturating_sub(1);
            if worker.active_tasks < worker.max_concurrency && worker.status == WorkerStatus::Busy {
                worker.status = WorkerStatus::Active;
            }
        }
    }

    pub fn detect_dead_workers(&self) -> Vec<String> {
        let now = Utc::now();
        let mut dead = Vec::new();

        for mut entry in self.workers.iter_mut() {
            let elapsed_ms = (now - entry.last_heartbeat).num_milliseconds().max(0) as u64;
            if elapsed_ms > self.heartbeat_timeout_ms {
                entry.status = WorkerStatus::Dead;
                dead.push(entry.id.clone());
            }
        }

        dead
    }

    pub fn active_workers(&self) -> Vec<WorkerInfo> {
        self.workers
            .iter()
            .filter(|w| w.status == WorkerStatus::Active || w.status == WorkerStatus::Busy)
            .map(|w| w.value().clone())
            .collect()
    }

    pub fn count(&self) -> usize {
        self.workers.len()
    }

    pub fn stats(&self) -> PoolStats {
        let mut stats = PoolStats::default();
        for entry in self.workers.iter() {
            stats.total += 1;
            match entry.status {
                WorkerStatus::Active => stats.active += 1,
                WorkerStatus::Busy => stats.busy += 1,
                WorkerStatus::Draining => stats.draining += 1,
                WorkerStatus::Dead => stats.dead += 1,
            }
            stats.total_capacity += entry.max_concurrency;
            stats.used_capacity += entry.active_tasks;
        }
        stats
    }

    /// List all connected workers with their full info.
    pub fn workers(&self) -> Vec<WorkerInfo> {
        self.workers.iter().map(|w| w.value().clone()).collect()
    }

    /// Set a worker's status to Draining so no new tasks are routed to it.
    /// Existing tasks will finish normally.
    pub fn drain_worker(&self, worker_id: &str) -> Result<(), PoolError> {
        if let Some(mut worker) = self.workers.get_mut(worker_id) {
            worker.status = WorkerStatus::Draining;
            tracing::info!(worker_id = %worker_id, "Worker set to draining");
            Ok(())
        } else {
            Err(PoolError::WorkerNotFound {
                worker_id: worker_id.to_string(),
            })
        }
    }

    /// Force-remove a worker from the pool. Returns the list of pending task IDs
    /// that were assigned to this worker (caller is responsible for failing them).
    pub fn remove_worker(&self, worker_id: &str) -> Result<(), PoolError> {
        if self.workers.remove(worker_id).is_some() {
            tracing::info!(worker_id = %worker_id, "Worker force-removed from pool");
            self.check_below_min();
            Ok(())
        } else {
            Err(PoolError::WorkerNotFound {
                worker_id: worker_id.to_string(),
            })
        }
    }

    /// Select a worker that has a matching tag and reserve capacity on it.
    pub fn select_and_reserve_with_tag(&self, tag: &str, task_type: &str) -> Option<String> {
        let mut best_id: Option<String> = None;
        let mut best_capacity: u32 = 0;

        for entry in self.workers.iter() {
            let worker = entry.value();
            if worker.status == WorkerStatus::Dead || worker.status == WorkerStatus::Draining {
                continue;
            }
            if !worker.tags.iter().any(|t| t == tag) {
                continue;
            }
            if !worker.supported_tasks.iter().any(|t| t == task_type) {
                continue;
            }
            if worker.active_tasks >= worker.max_concurrency {
                continue;
            }

            let available = worker.max_concurrency - worker.active_tasks;
            if best_id.is_none() || available > best_capacity {
                best_id = Some(worker.id.clone());
                best_capacity = available;
            }
        }

        let worker_id = best_id?;

        // Atomically increment under the entry lock
        if let Some(mut worker) = self.workers.get_mut(&worker_id) {
            if worker.active_tasks >= worker.max_concurrency {
                return None;
            }
            worker.active_tasks += 1;
            if worker.active_tasks >= worker.max_concurrency {
                worker.status = WorkerStatus::Busy;
            }
            Some(worker_id)
        } else {
            None
        }
    }

    /// Reserve capacity on a specific worker by ID.
    /// Returns Ok(()) if capacity was reserved, or a specific error explaining why not.
    pub fn reserve_specific_worker(&self, worker_id: &str) -> Result<(), PoolError> {
        if let Some(mut worker) = self.workers.get_mut(worker_id) {
            if worker.status == WorkerStatus::Dead || worker.status == WorkerStatus::Draining {
                return Err(PoolError::WorkerUnavailable {
                    worker_id: worker_id.to_string(),
                });
            }
            if worker.active_tasks >= worker.max_concurrency {
                return Err(PoolError::WorkerAtCapacity {
                    worker_id: worker_id.to_string(),
                });
            }
            worker.active_tasks += 1;
            if worker.active_tasks >= worker.max_concurrency {
                worker.status = WorkerStatus::Busy;
            }
            Ok(())
        } else {
            Err(PoolError::WorkerNotFound {
                worker_id: worker_id.to_string(),
            })
        }
    }

    /// Check if the pool is below the minimum size and fire the callback if so.
    fn check_below_min(&self) {
        if let Some(min) = self.min_pool_size {
            let current = self.workers.len() as u32;
            if current < min {
                if let Some(ref cb) = self.on_pool_below_min {
                    cb(current);
                }
            }
        }
    }
}

#[derive(Debug, Default, Serialize)]
/// Aggregate statistics about the worker pool.
pub struct PoolStats {
    pub total: u32,
    pub active: u32,
    pub busy: u32,
    pub draining: u32,
    pub dead: u32,
    pub total_capacity: u32,
    pub used_capacity: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::WorkerLanguage;

    fn make_worker(id: &str, tasks: Vec<&str>, max_concurrency: u32) -> WorkerInfo {
        WorkerInfo {
            id: id.to_string(),
            language: WorkerLanguage::TypeScript,
            supported_tasks: tasks.into_iter().map(String::from).collect(),
            max_concurrency,
            status: WorkerStatus::Active,
            active_tasks: 0,
            registered_at: Utc::now(),
            last_heartbeat: Utc::now(),
            tags: vec![],
        }
    }

    fn make_tagged_worker(
        id: &str,
        tasks: Vec<&str>,
        max_concurrency: u32,
        tags: Vec<&str>,
    ) -> WorkerInfo {
        WorkerInfo {
            id: id.to_string(),
            language: WorkerLanguage::TypeScript,
            supported_tasks: tasks.into_iter().map(String::from).collect(),
            max_concurrency,
            status: WorkerStatus::Active,
            active_tasks: 0,
            registered_at: Utc::now(),
            last_heartbeat: Utc::now(),
            tags: tags.into_iter().map(String::from).collect(),
        }
    }

    #[test]
    fn test_pool_new_empty() {
        let pool = WorkerPool::new(15_000);
        assert_eq!(pool.count(), 0);
    }

    #[test]
    fn test_register_single_worker() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["task-a"], 5));
        assert_eq!(pool.count(), 1);
    }

    #[test]
    fn test_register_multiple_workers() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.register(make_worker("w2", vec!["b"], 5));
        pool.register(make_worker("w3", vec!["c"], 5));
        assert_eq!(pool.count(), 3);
    }

    #[test]
    fn test_register_overwrites_same_id() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.register(make_worker("w1", vec!["b", "c"], 10));
        assert_eq!(pool.count(), 1);
    }

    #[test]
    fn test_deregister_existing_worker() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.deregister("w1");
        assert_eq!(pool.count(), 0);
    }

    #[test]
    fn test_deregister_nonexistent_worker() {
        let pool = WorkerPool::new(15_000);
        pool.deregister("ghost");
        assert_eq!(pool.count(), 0);
    }

    #[test]
    fn test_heartbeat_updates_active_tasks() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.heartbeat("w1", 3);
        let stats = pool.stats();
        assert_eq!(stats.used_capacity, 3);
    }

    #[test]
    fn test_heartbeat_sets_busy_when_at_capacity() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 2));
        pool.heartbeat("w1", 2);
        let stats = pool.stats();
        assert_eq!(stats.busy, 1);
        assert_eq!(stats.active, 0);
    }

    #[test]
    fn test_heartbeat_sets_active_when_below_capacity() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.heartbeat("w1", 3);
        let stats = pool.stats();
        assert_eq!(stats.active, 1);
    }

    #[test]
    fn test_heartbeat_nonexistent_worker_is_noop() {
        let pool = WorkerPool::new(15_000);
        pool.heartbeat("ghost", 1);
        assert_eq!(pool.count(), 0);
    }

    #[test]
    fn test_select_worker_single_matching() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["build"], 5));
        assert_eq!(pool.select_worker("build"), Some("w1".to_string()));
    }

    #[test]
    fn test_select_worker_returns_none_when_no_matching_type() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["build"], 5));
        assert_eq!(pool.select_worker("deploy"), None);
    }

    #[test]
    fn test_select_worker_returns_none_when_pool_empty() {
        let pool = WorkerPool::new(15_000);
        assert_eq!(pool.select_worker("any"), None);
    }

    #[test]
    fn test_select_worker_picks_least_loaded() {
        let pool = WorkerPool::new(15_000);
        let mut w1 = make_worker("w1", vec!["build"], 5);
        w1.active_tasks = 4;
        let mut w2 = make_worker("w2", vec!["build"], 5);
        w2.active_tasks = 1;
        pool.register(w1);
        pool.register(w2);
        assert_eq!(pool.select_worker("build"), Some("w2".to_string()));
    }

    #[test]
    fn test_select_worker_skips_dead_worker() {
        let pool = WorkerPool::new(15_000);
        let mut w = make_worker("w1", vec!["build"], 5);
        w.status = WorkerStatus::Dead;
        pool.register(w);
        assert_eq!(pool.select_worker("build"), None);
    }

    #[test]
    fn test_select_worker_skips_draining_worker() {
        let pool = WorkerPool::new(15_000);
        let mut w = make_worker("w1", vec!["build"], 5);
        w.status = WorkerStatus::Draining;
        pool.register(w);
        assert_eq!(pool.select_worker("build"), None);
    }

    #[test]
    fn test_select_worker_skips_at_capacity() {
        let pool = WorkerPool::new(15_000);
        let mut w = make_worker("w1", vec!["build"], 2);
        w.active_tasks = 2;
        pool.register(w);
        assert_eq!(pool.select_worker("build"), None);
    }

    #[test]
    fn test_select_worker_multiple_task_types() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a", "b", "c"], 5));
        assert_eq!(pool.select_worker("b"), Some("w1".to_string()));
    }

    #[test]
    fn test_mark_task_dispatched_increments() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.mark_task_dispatched("w1");
        let stats = pool.stats();
        assert_eq!(stats.used_capacity, 1);
    }

    #[test]
    fn test_mark_task_dispatched_sets_busy_at_capacity() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 1));
        pool.mark_task_dispatched("w1");
        let stats = pool.stats();
        assert_eq!(stats.busy, 1);
    }

    #[test]
    fn test_mark_task_dispatched_nonexistent_is_noop() {
        let pool = WorkerPool::new(15_000);
        pool.mark_task_dispatched("ghost");
    }

    #[test]
    fn test_mark_task_completed_decrements() {
        let pool = WorkerPool::new(15_000);
        let mut w = make_worker("w1", vec!["a"], 5);
        w.active_tasks = 2;
        pool.register(w);
        pool.mark_task_completed("w1");
        let stats = pool.stats();
        assert_eq!(stats.used_capacity, 1);
    }

    #[test]
    fn test_mark_task_completed_saturating_at_zero() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.mark_task_completed("w1");
        let stats = pool.stats();
        assert_eq!(stats.used_capacity, 0);
    }

    #[test]
    fn test_mark_task_completed_transitions_busy_to_active() {
        let pool = WorkerPool::new(15_000);
        let mut w = make_worker("w1", vec!["a"], 2);
        w.active_tasks = 2;
        w.status = WorkerStatus::Busy;
        pool.register(w);
        pool.mark_task_completed("w1");
        let stats = pool.stats();
        assert_eq!(stats.active, 1);
        assert_eq!(stats.busy, 0);
    }

    #[test]
    fn test_detect_dead_workers_marks_stale() {
        let pool = WorkerPool::new(100); // 100ms timeout
        let mut w = make_worker("w1", vec!["a"], 5);
        w.last_heartbeat = Utc::now() - chrono::Duration::seconds(1);
        pool.register(w);
        let dead = pool.detect_dead_workers();
        assert_eq!(dead, vec!["w1".to_string()]);
    }

    #[test]
    fn test_detect_dead_workers_spares_fresh() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        let dead = pool.detect_dead_workers();
        assert!(dead.is_empty());
    }

    #[test]
    fn test_detect_dead_workers_empty_pool() {
        let pool = WorkerPool::new(15_000);
        let dead = pool.detect_dead_workers();
        assert!(dead.is_empty());
    }

    #[test]
    fn test_stats_empty_pool() {
        let pool = WorkerPool::new(15_000);
        let stats = pool.stats();
        assert_eq!(stats.total, 0);
        assert_eq!(stats.active, 0);
        assert_eq!(stats.total_capacity, 0);
    }

    #[test]
    fn test_stats_counts_all_statuses() {
        let pool = WorkerPool::new(15_000);
        let mut w1 = make_worker("w1", vec!["a"], 5);
        w1.status = WorkerStatus::Active;
        let mut w2 = make_worker("w2", vec!["a"], 5);
        w2.status = WorkerStatus::Busy;
        let mut w3 = make_worker("w3", vec!["a"], 5);
        w3.status = WorkerStatus::Draining;
        let mut w4 = make_worker("w4", vec!["a"], 5);
        w4.status = WorkerStatus::Dead;
        pool.register(w1);
        pool.register(w2);
        pool.register(w3);
        pool.register(w4);
        let stats = pool.stats();
        assert_eq!(stats.total, 4);
        assert_eq!(stats.active, 1);
        assert_eq!(stats.busy, 1);
        assert_eq!(stats.draining, 1);
        assert_eq!(stats.dead, 1);
    }

    #[test]
    fn test_stats_capacity_tracking() {
        let pool = WorkerPool::new(15_000);
        let mut w = make_worker("w1", vec!["a"], 10);
        w.active_tasks = 3;
        pool.register(w);
        let stats = pool.stats();
        assert_eq!(stats.total_capacity, 10);
        assert_eq!(stats.used_capacity, 3);
    }

    #[test]
    fn test_active_workers_includes_active_and_busy() {
        let pool = WorkerPool::new(15_000);
        let mut w1 = make_worker("w1", vec!["a"], 5);
        w1.status = WorkerStatus::Active;
        let mut w2 = make_worker("w2", vec!["a"], 5);
        w2.status = WorkerStatus::Busy;
        pool.register(w1);
        pool.register(w2);
        assert_eq!(pool.active_workers().len(), 2);
    }

    #[test]
    fn test_active_workers_excludes_dead_and_draining() {
        let pool = WorkerPool::new(15_000);
        let mut w1 = make_worker("w1", vec!["a"], 5);
        w1.status = WorkerStatus::Dead;
        let mut w2 = make_worker("w2", vec!["a"], 5);
        w2.status = WorkerStatus::Draining;
        pool.register(w1);
        pool.register(w2);
        assert_eq!(pool.active_workers().len(), 0);
    }

    // =========================================================================
    // Pool size limits tests
    // =========================================================================

    #[test]
    fn test_max_pool_size_rejects_over_limit() {
        let pool = WorkerPool::with_limits(15_000, Some(2), None, None);
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.register(make_worker("w2", vec!["a"], 5));
        pool.register(make_worker("w3", vec!["a"], 5)); // should be rejected
        assert_eq!(pool.count(), 2);
    }

    #[test]
    fn test_max_pool_size_none_allows_unlimited() {
        let pool = WorkerPool::with_limits(15_000, None, None, None);
        for i in 0..100 {
            pool.register(make_worker(&format!("w{}", i), vec!["a"], 5));
        }
        assert_eq!(pool.count(), 100);
    }

    #[test]
    fn test_try_register_returns_error_on_full() {
        let pool = WorkerPool::with_limits(15_000, Some(1), None, None);
        pool.register(make_worker("w1", vec!["a"], 5));
        let result = pool.try_register(make_worker("w2", vec!["a"], 5));
        assert_eq!(result, Err(PoolError::PoolFull { max: 1 }));
    }

    #[test]
    fn test_try_register_succeeds_under_limit() {
        let pool = WorkerPool::with_limits(15_000, Some(3), None, None);
        let result = pool.try_register(make_worker("w1", vec!["a"], 5));
        assert!(result.is_ok());
        assert_eq!(pool.count(), 1);
    }

    #[test]
    fn test_min_pool_size_does_not_fire_on_register() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let call_count = Arc::new(AtomicU32::new(0));
        let c = call_count.clone();
        let pool = WorkerPool::with_limits(
            15_000,
            None,
            Some(3),
            Some(Arc::new(move |_current| {
                c.fetch_add(1, Ordering::SeqCst);
            })),
        );
        pool.register(make_worker("w1", vec!["a"], 5));
        // Callback should NOT fire during registration — only on worker loss
        assert_eq!(call_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_min_pool_size_fires_callback_on_deregister() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let called_with = Arc::new(AtomicU32::new(999));
        let called_clone = called_with.clone();
        let pool = WorkerPool::with_limits(
            15_000,
            None,
            Some(2),
            Some(Arc::new(move |current| {
                called_clone.store(current, Ordering::SeqCst);
            })),
        );
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.register(make_worker("w2", vec!["a"], 5));
        // Pool at min, callback was fired during register but with current=1 then current=2
        // After second register, pool is at 2 = min, so no callback
        // Now deregister one
        pool.deregister("w1");
        // Pool has 1, min is 2 => callback fires with 1
        assert_eq!(called_with.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_min_pool_size_no_callback_when_above_min() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let was_called = Arc::new(AtomicBool::new(false));
        let was_called_clone = was_called.clone();
        let pool = WorkerPool::with_limits(
            15_000,
            None,
            Some(1),
            Some(Arc::new(move |_| {
                was_called_clone.store(true, Ordering::SeqCst);
            })),
        );
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.register(make_worker("w2", vec!["a"], 5));
        // Pool at 2, min 1 => second register should NOT fire callback
        // But first register: pool goes to 1, which is NOT below min (1 < 1 is false)
        assert!(!was_called.load(Ordering::SeqCst));
    }

    // =========================================================================
    // Worker listing tests
    // =========================================================================

    #[test]
    fn test_workers_returns_all() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.register(make_worker("w2", vec!["b"], 3));
        let workers = pool.workers();
        assert_eq!(workers.len(), 2);
        let ids: Vec<&str> = workers.iter().map(|w| w.id.as_str()).collect();
        assert!(ids.contains(&"w1"));
        assert!(ids.contains(&"w2"));
    }

    #[test]
    fn test_workers_empty_pool() {
        let pool = WorkerPool::new(15_000);
        assert!(pool.workers().is_empty());
    }

    // =========================================================================
    // Drain worker tests
    // =========================================================================

    #[test]
    fn test_drain_worker_sets_status() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.drain_worker("w1").unwrap();
        let workers = pool.workers();
        assert_eq!(workers[0].status, WorkerStatus::Draining);
    }

    #[test]
    fn test_drain_worker_not_found() {
        let pool = WorkerPool::new(15_000);
        let err = pool.drain_worker("ghost").unwrap_err();
        assert_eq!(
            err,
            PoolError::WorkerNotFound {
                worker_id: "ghost".to_string()
            }
        );
    }

    #[test]
    fn test_drain_worker_not_selected() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.drain_worker("w1").unwrap();
        // Draining workers should not be selected
        assert_eq!(pool.select_worker("a"), None);
        assert_eq!(pool.select_and_reserve("a"), None);
    }

    // =========================================================================
    // Remove worker tests
    // =========================================================================

    #[test]
    fn test_remove_worker_removes_from_pool() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.remove_worker("w1").unwrap();
        assert_eq!(pool.count(), 0);
    }

    #[test]
    fn test_remove_worker_not_found() {
        let pool = WorkerPool::new(15_000);
        let err = pool.remove_worker("ghost").unwrap_err();
        assert_eq!(
            err,
            PoolError::WorkerNotFound {
                worker_id: "ghost".to_string()
            }
        );
    }

    #[test]
    fn test_remove_worker_fires_below_min_callback() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let called_with = Arc::new(AtomicU32::new(999));
        let called_clone = called_with.clone();
        let pool = WorkerPool::with_limits(
            15_000,
            None,
            Some(2),
            Some(Arc::new(move |current| {
                called_clone.store(current, Ordering::SeqCst);
            })),
        );
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.register(make_worker("w2", vec!["a"], 5));
        pool.remove_worker("w1").unwrap();
        assert_eq!(called_with.load(Ordering::SeqCst), 1);
    }

    // =========================================================================
    // Worker tags and tag-based dispatch tests
    // =========================================================================

    #[test]
    fn test_worker_info_tags_default_empty() {
        let w = make_worker("w1", vec!["a"], 5);
        assert!(w.tags.is_empty());
    }

    #[test]
    fn test_worker_info_tags_set() {
        let w = make_tagged_worker("w1", vec!["a"], 5, vec!["gpu", "us-east-1"]);
        assert_eq!(w.tags, vec!["gpu".to_string(), "us-east-1".to_string()]);
    }

    #[test]
    fn test_select_and_reserve_with_tag_finds_matching() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_tagged_worker("w1", vec!["build"], 5, vec!["gpu"]));
        pool.register(make_tagged_worker("w2", vec!["build"], 5, vec!["cpu"]));
        let selected = pool.select_and_reserve_with_tag("gpu", "build");
        assert_eq!(selected, Some("w1".to_string()));
    }

    #[test]
    fn test_select_and_reserve_with_tag_none_when_no_match() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_tagged_worker("w1", vec!["build"], 5, vec!["cpu"]));
        let selected = pool.select_and_reserve_with_tag("gpu", "build");
        assert_eq!(selected, None);
    }

    #[test]
    fn test_select_and_reserve_with_tag_skips_draining() {
        let pool = WorkerPool::new(15_000);
        let mut w = make_tagged_worker("w1", vec!["build"], 5, vec!["gpu"]);
        w.status = WorkerStatus::Draining;
        pool.register(w);
        let selected = pool.select_and_reserve_with_tag("gpu", "build");
        assert_eq!(selected, None);
    }

    #[test]
    fn test_select_and_reserve_with_tag_reserves_capacity() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_tagged_worker("w1", vec!["build"], 2, vec!["gpu"]));
        pool.select_and_reserve_with_tag("gpu", "build");
        let stats = pool.stats();
        assert_eq!(stats.used_capacity, 1);
    }

    #[test]
    fn test_select_and_reserve_with_tag_requires_task_type_match() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_tagged_worker("w1", vec!["build"], 5, vec!["gpu"]));
        let selected = pool.select_and_reserve_with_tag("gpu", "deploy");
        assert_eq!(selected, None);
    }

    // =========================================================================
    // Reserve specific worker tests
    // =========================================================================

    #[test]
    fn test_reserve_specific_worker_success() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        assert!(pool.reserve_specific_worker("w1").is_ok());
        let stats = pool.stats();
        assert_eq!(stats.used_capacity, 1);
    }

    #[test]
    fn test_reserve_specific_worker_not_found() {
        let pool = WorkerPool::new(15_000);
        let err = pool.reserve_specific_worker("ghost").unwrap_err();
        assert_eq!(
            err,
            PoolError::WorkerNotFound {
                worker_id: "ghost".to_string()
            }
        );
    }

    #[test]
    fn test_reserve_specific_worker_at_capacity_existing() {
        let pool = WorkerPool::new(15_000);
        let mut w = make_worker("w1", vec!["a"], 1);
        w.active_tasks = 1;
        w.status = WorkerStatus::Busy;
        pool.register(w);
        let err = pool.reserve_specific_worker("w1").unwrap_err();
        assert!(matches!(err, PoolError::WorkerAtCapacity { .. }));
    }

    #[test]
    fn test_reserve_specific_worker_draining() {
        let pool = WorkerPool::new(15_000);
        let mut w = make_worker("w1", vec!["a"], 5);
        w.status = WorkerStatus::Draining;
        pool.register(w);
        let err = pool.reserve_specific_worker("w1").unwrap_err();
        assert!(matches!(err, PoolError::WorkerUnavailable { .. }));
    }

    #[test]
    fn test_reserve_specific_worker_at_capacity() {
        let pool = WorkerPool::new(15_000);
        let mut w = make_worker("w1", vec!["a"], 1);
        w.active_tasks = 1;
        pool.register(w);
        let err = pool.reserve_specific_worker("w1").unwrap_err();
        assert!(matches!(err, PoolError::WorkerAtCapacity { .. }));
    }

    #[test]
    fn test_heartbeat_does_not_overwrite_draining() {
        let pool = WorkerPool::new(15_000);
        pool.register(make_worker("w1", vec!["a"], 5));
        pool.drain_worker("w1").unwrap();
        pool.heartbeat("w1", 1);
        let workers = pool.workers();
        assert_eq!(workers[0].status, WorkerStatus::Draining);
    }

    #[test]
    fn test_reregistration_allowed_at_max_capacity() {
        let pool = WorkerPool::with_limits(15_000, Some(1), None, None);
        pool.register(make_worker("w1", vec!["a"], 5));
        // Re-register same worker — should succeed even at max
        pool.register(make_worker("w1", vec!["a", "b"], 10));
        assert_eq!(pool.count(), 1);
    }

    #[test]
    fn test_register_does_not_fire_below_min_callback() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();
        let pool = WorkerPool::with_limits(
            15_000,
            None,
            Some(5),
            Some(Arc::new(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
            })),
        );
        pool.register(make_worker("w1", vec!["a"], 5));
        // Should NOT fire on register — only on worker loss
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    // =========================================================================
    // WorkerInfo serde with tags tests
    // =========================================================================

    #[test]
    fn test_worker_info_serde_with_tags() {
        let w = make_tagged_worker("w1", vec!["build"], 5, vec!["gpu", "region-a"]);
        let json = serde_json::to_string(&w).unwrap();
        let de: WorkerInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(de.tags, vec!["gpu".to_string(), "region-a".to_string()]);
    }

    #[test]
    fn test_worker_info_serde_without_tags_defaults_empty() {
        // Simulate old JSON without tags field
        let json = r#"{
            "id": "w1",
            "language": "TypeScript",
            "supported_tasks": ["a"],
            "max_concurrency": 5,
            "status": "Active",
            "active_tasks": 0,
            "registered_at": "2024-01-01T00:00:00Z",
            "last_heartbeat": "2024-01-01T00:00:00Z"
        }"#;
        let de: WorkerInfo = serde_json::from_str(json).unwrap();
        assert!(de.tags.is_empty());
    }

    // =========================================================================
    // PoolError display tests
    // =========================================================================

    #[test]
    fn test_pool_error_display_pool_full() {
        let err = PoolError::PoolFull { max: 10 };
        assert!(err.to_string().contains("maximum capacity"));
        assert!(err.to_string().contains("10"));
    }

    #[test]
    fn test_pool_error_display_worker_not_found() {
        let err = PoolError::WorkerNotFound {
            worker_id: "abc".to_string(),
        };
        assert!(err.to_string().contains("abc"));
    }

    // =========================================================================
    // with_limits constructor tests
    // =========================================================================

    #[test]
    fn test_with_limits_sets_max() {
        let pool = WorkerPool::with_limits(15_000, Some(5), None, None);
        pool.register(make_worker("w1", vec!["a"], 1));
        pool.register(make_worker("w2", vec!["a"], 1));
        pool.register(make_worker("w3", vec!["a"], 1));
        pool.register(make_worker("w4", vec!["a"], 1));
        pool.register(make_worker("w5", vec!["a"], 1));
        pool.register(make_worker("w6", vec!["a"], 1)); // rejected
        assert_eq!(pool.count(), 5);
    }
}
