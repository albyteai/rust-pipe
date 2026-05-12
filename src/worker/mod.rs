/// Worker pool management and least-loaded selection.
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

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
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
/// Current status of a worker in the pool.
pub enum WorkerStatus {
    Active,
    Busy,
    Draining,
    Dead,
}

/// Pool of connected workers with capacity-aware task routing.
pub struct WorkerPool {
    workers: DashMap<String, WorkerInfo>,
    heartbeat_timeout_ms: u64,
}

impl WorkerPool {
    pub fn new(heartbeat_timeout_ms: u64) -> Self {
        Self {
            workers: DashMap::new(),
            heartbeat_timeout_ms,
        }
    }

    pub fn register(&self, info: WorkerInfo) {
        tracing::info!(
            worker_id = %info.id,
            language = ?info.language,
            tasks = ?info.supported_tasks,
            "Registering worker"
        );
        self.workers.insert(info.id.clone(), info);
    }

    pub fn deregister(&self, worker_id: &str) {
        self.workers.remove(worker_id);
    }

    pub fn heartbeat(&self, worker_id: &str, active_tasks: u32) {
        if let Some(mut worker) = self.workers.get_mut(worker_id) {
            worker.last_heartbeat = Utc::now();
            worker.active_tasks = active_tasks;
            worker.status = if active_tasks >= worker.max_concurrency {
                WorkerStatus::Busy
            } else {
                WorkerStatus::Active
            };
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
}
