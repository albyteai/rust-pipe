use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A unit of work dispatched to a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub id: Uuid,
    pub task_type: String,
    pub payload: serde_json::Value,
    pub metadata: TaskMetadata,
}

/// Metadata attached to every task (timeouts, priority, retries).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskMetadata {
    pub created_at: DateTime<Utc>,
    pub timeout_ms: u64,
    pub priority: Priority,
    pub retry_count: u32,
    pub max_retries: u32,
    pub trace_id: Option<String>,
}

/// Task priority level. Higher priority tasks are dispatched first.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Result returned by a worker after executing a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskResult {
    pub task_id: Uuid,
    pub status: TaskStatus,
    pub payload: Option<serde_json::Value>,
    pub error: Option<TaskError>,
    pub duration_ms: u64,
    pub worker_id: String,
}

/// Lifecycle state of a task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Pending,
    Dispatched,
    Running,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
}

/// Error information from a failed task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl Task {
    pub fn new(task_type: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            task_type: task_type.into(),
            payload,
            metadata: TaskMetadata {
                created_at: Utc::now(),
                timeout_ms: 300_000,
                priority: Priority::Normal,
                retry_count: 0,
                max_retries: 3,
                trace_id: None,
            },
        }
    }

    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.metadata.timeout_ms = timeout_ms;
        self
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.metadata.priority = priority;
        self
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.metadata.max_retries = max_retries;
        self
    }

    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.metadata.trace_id = Some(trace_id.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_task_new_sets_uuid() {
        let task = Task::new("test", json!({}));
        assert_ne!(task.id, Uuid::nil());
    }

    #[test]
    fn test_task_new_sets_task_type() {
        let task = Task::new("scan-target", json!({}));
        assert_eq!(task.task_type, "scan-target");
    }

    #[test]
    fn test_task_new_stores_payload() {
        let payload = json!({"key": "value", "num": 42});
        let task = Task::new("t", payload.clone());
        assert_eq!(task.payload, payload);
    }

    #[test]
    fn test_task_new_default_timeout() {
        let task = Task::new("t", json!({}));
        assert_eq!(task.metadata.timeout_ms, 300_000);
    }

    #[test]
    fn test_task_new_default_priority() {
        let task = Task::new("t", json!({}));
        assert_eq!(task.metadata.priority, Priority::Normal);
    }

    #[test]
    fn test_task_new_default_retry_count() {
        let task = Task::new("t", json!({}));
        assert_eq!(task.metadata.retry_count, 0);
    }

    #[test]
    fn test_task_new_default_max_retries() {
        let task = Task::new("t", json!({}));
        assert_eq!(task.metadata.max_retries, 3);
    }

    #[test]
    fn test_task_new_default_trace_id_none() {
        let task = Task::new("t", json!({}));
        assert_eq!(task.metadata.trace_id, None);
    }

    #[test]
    fn test_task_new_created_at_is_recent() {
        let before = Utc::now();
        let task = Task::new("t", json!({}));
        let after = Utc::now();
        assert!(task.metadata.created_at >= before);
        assert!(task.metadata.created_at <= after);
    }

    #[test]
    fn test_task_with_timeout() {
        let task = Task::new("t", json!({})).with_timeout(5000);
        assert_eq!(task.metadata.timeout_ms, 5000);
    }

    #[test]
    fn test_task_with_priority() {
        let task = Task::new("t", json!({})).with_priority(Priority::Critical);
        assert_eq!(task.metadata.priority, Priority::Critical);
    }

    #[test]
    fn test_task_with_max_retries() {
        let task = Task::new("t", json!({})).with_max_retries(10);
        assert_eq!(task.metadata.max_retries, 10);
    }

    #[test]
    fn test_task_with_trace_id() {
        let task = Task::new("t", json!({})).with_trace_id("abc-123");
        assert_eq!(task.metadata.trace_id, Some("abc-123".to_string()));
    }

    #[test]
    fn test_task_builder_chaining() {
        let task = Task::new("scan", json!({"url": "http://x.com"}))
            .with_timeout(1000)
            .with_priority(Priority::High)
            .with_max_retries(5)
            .with_trace_id("trace-1");
        assert_eq!(task.task_type, "scan");
        assert_eq!(task.metadata.timeout_ms, 1000);
        assert_eq!(task.metadata.priority, Priority::High);
        assert_eq!(task.metadata.max_retries, 5);
        assert_eq!(task.metadata.trace_id, Some("trace-1".to_string()));
    }

    #[test]
    fn test_task_with_timeout_zero() {
        let task = Task::new("t", json!({})).with_timeout(0);
        assert_eq!(task.metadata.timeout_ms, 0);
    }

    #[test]
    fn test_task_with_max_retries_zero() {
        let task = Task::new("t", json!({})).with_max_retries(0);
        assert_eq!(task.metadata.max_retries, 0);
    }

    #[test]
    fn test_task_new_from_owned_string() {
        let task = Task::new(String::from("owned-type"), json!({}));
        assert_eq!(task.task_type, "owned-type");
    }

    #[test]
    fn test_task_serde_roundtrip() {
        let task = Task::new("roundtrip", json!({"a": 1})).with_trace_id("t1");
        let json = serde_json::to_string(&task).unwrap();
        let deserialized: Task = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, task.id);
        assert_eq!(deserialized.task_type, task.task_type);
        assert_eq!(deserialized.payload, task.payload);
        assert_eq!(deserialized.metadata.timeout_ms, task.metadata.timeout_ms);
        assert_eq!(deserialized.metadata.trace_id, task.metadata.trace_id);
    }

    #[test]
    fn test_task_result_serde_roundtrip() {
        let result = TaskResult {
            task_id: Uuid::new_v4(),
            status: TaskStatus::Completed,
            payload: Some(json!({"vulns": 3})),
            error: None,
            duration_ms: 1500,
            worker_id: "w1".to_string(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let de: TaskResult = serde_json::from_str(&json).unwrap();
        assert_eq!(de.task_id, result.task_id);
        assert_eq!(de.status, TaskStatus::Completed);
        assert_eq!(de.duration_ms, 1500);
    }

    #[test]
    fn test_task_result_serde_null_optionals() {
        let result = TaskResult {
            task_id: Uuid::new_v4(),
            status: TaskStatus::Failed,
            payload: None,
            error: None,
            duration_ms: 0,
            worker_id: "w".to_string(),
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("null"));
        let de: TaskResult = serde_json::from_str(&json).unwrap();
        assert_eq!(de.payload, None);
        assert_eq!(de.error, None);
    }

    #[test]
    fn test_task_error_serde_roundtrip() {
        let err = TaskError {
            code: "E1".into(),
            message: "fail".into(),
            retryable: true,
        };
        let json = serde_json::to_string(&err).unwrap();
        let de: TaskError = serde_json::from_str(&json).unwrap();
        assert_eq!(de.code, "E1");
        assert!(de.retryable);
    }

    #[test]
    fn test_priority_ordering() {
        assert!(Priority::Low < Priority::Normal);
        assert!(Priority::Normal < Priority::High);
        assert!(Priority::High < Priority::Critical);
    }

    #[test]
    fn test_priority_serde_all_variants() {
        for p in [
            Priority::Low,
            Priority::Normal,
            Priority::High,
            Priority::Critical,
        ] {
            let json = serde_json::to_string(&p).unwrap();
            let de: Priority = serde_json::from_str(&json).unwrap();
            assert_eq!(de, p);
        }
    }

    #[test]
    fn test_task_status_serde_all_variants() {
        let variants = [
            TaskStatus::Pending,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Completed,
            TaskStatus::Failed,
            TaskStatus::TimedOut,
            TaskStatus::Cancelled,
        ];
        for v in variants {
            let json = serde_json::to_string(&v).unwrap();
            let de: TaskStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(de, v);
        }
    }

    #[test]
    fn test_task_json_uses_camel_case() {
        let task = Task::new("t", json!({}));
        let json = serde_json::to_string(&task).unwrap();
        assert!(json.contains("taskType"));
        assert!(json.contains("createdAt"));
        assert!(json.contains("timeoutMs"));
        assert!(json.contains("retryCount"));
        assert!(json.contains("maxRetries"));
        assert!(json.contains("traceId"));
        assert!(!json.contains("task_type"));
        assert!(!json.contains("created_at"));
    }

    #[test]
    fn test_task_result_json_uses_camel_case() {
        let result = TaskResult {
            task_id: Uuid::new_v4(),
            status: TaskStatus::Completed,
            payload: None,
            error: None,
            duration_ms: 100,
            worker_id: "w".to_string(),
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("taskId"));
        assert!(json.contains("durationMs"));
        assert!(json.contains("workerId"));
        assert!(!json.contains("task_id"));
        assert!(!json.contains("duration_ms"));
    }

    #[test]
    fn test_task_payload_nested_json() {
        let payload = json!({"a": {"b": {"c": [1, 2, {"d": true}]}}});
        let task = Task::new("t", payload.clone());
        let json = serde_json::to_string(&task).unwrap();
        let de: Task = serde_json::from_str(&json).unwrap();
        assert_eq!(de.payload, payload);
    }

    #[test]
    fn test_task_payload_empty_object() {
        let task = Task::new("t", json!({}));
        let json = serde_json::to_string(&task).unwrap();
        let de: Task = serde_json::from_str(&json).unwrap();
        assert_eq!(de.payload, json!({}));
    }

    #[test]
    fn test_task_payload_array() {
        let task = Task::new("t", json!([1, 2, 3]));
        let json = serde_json::to_string(&task).unwrap();
        let de: Task = serde_json::from_str(&json).unwrap();
        assert_eq!(de.payload, json!([1, 2, 3]));
    }
}
