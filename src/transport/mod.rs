pub mod docker;
pub mod ssh;
pub mod stdio;
pub mod wasm;
pub mod websocket;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::schema::{Task, TaskResult};

/// Configuration for the transport layer (WebSocket server settings).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: u32,
    pub heartbeat_interval_ms: u64,
    pub reconnect_delay_ms: u64,
    pub max_reconnect_attempts: u32,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 9876,
            max_connections: 100,
            heartbeat_interval_ms: 5_000,
            reconnect_delay_ms: 1_000,
            max_reconnect_attempts: 10,
        }
    }
}

/// Wire protocol message exchanged between dispatcher and workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Message {
    TaskDispatch { task: Task },
    TaskResult { result: TaskResult },
    Heartbeat { payload: HeartbeatPayload },
    HeartbeatAck,
    WorkerRegister { registration: WorkerRegistration },
    WorkerRegistered { worker_id: String },
    Backpressure { signal: BackpressureSignal },
    Kill { task_id: uuid::Uuid, reason: String },
    Shutdown { graceful: bool },
}

/// Heartbeat data sent periodically by workers to indicate liveness.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatPayload {
    pub worker_id: String,
    pub active_tasks: u32,
    pub capacity: u32,
    pub uptime_seconds: u64,
}

/// Registration payload sent by a worker when it connects.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerRegistration {
    pub worker_id: String,
    pub supported_tasks: Vec<String>,
    pub max_concurrency: u32,
    pub language: WorkerLanguage,
}

/// Programming language of a connected worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerLanguage {
    TypeScript,
    Python,
    Go,
    Other(String),
}

/// Signal from a worker indicating it is under heavy load.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackpressureSignal {
    pub worker_id: String,
    pub current_load: f64,
    pub should_throttle: bool,
}

/// Trait implemented by all transport backends (WebSocket, stdio, Docker, SSH, WASM).
#[async_trait]
pub trait Transport: Send + Sync {
    async fn start(&self) -> Result<(), TransportError>;
    async fn stop(&self) -> Result<(), TransportError>;
    async fn send(&self, worker_id: &str, message: Message) -> Result<(), TransportError>;
    async fn broadcast(&self, message: Message) -> Result<(), TransportError>;
}

/// Errors that can occur in any transport backend.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    #[error("Worker not found: {0}")]
    WorkerNotFound(String),
    #[error("Send failed: {0}")]
    SendFailed(String),
    #[error("Transport closed")]
    Closed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn test_transport_config_defaults() {
        let config = TransportConfig::default();
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 9876);
        assert_eq!(config.max_connections, 100);
        assert_eq!(config.heartbeat_interval_ms, 5_000);
        assert_eq!(config.reconnect_delay_ms, 1_000);
        assert_eq!(config.max_reconnect_attempts, 10);
    }

    #[test]
    fn test_message_serde_task_dispatch() {
        let task = Task::new("scan", json!({"url": "http://x.com"}));
        let msg = Message::TaskDispatch { task };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""type":"TaskDispatch""#));
        let de: Message = serde_json::from_str(&json).unwrap();
        if let Message::TaskDispatch { task: t } = de {
            assert_eq!(t.task_type, "scan");
        } else {
            panic!("Wrong variant");
        }
    }

    #[test]
    fn test_message_serde_task_result() {
        let result = TaskResult {
            task_id: Uuid::new_v4(),
            status: TaskStatus::Completed,
            payload: Some(json!({"v": 1})),
            error: None,
            duration_ms: 100,
            worker_id: "w1".into(),
        };
        let msg = Message::TaskResult { result };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""type":"TaskResult""#));
        let de: Message = serde_json::from_str(&json).unwrap();
        assert!(matches!(de, Message::TaskResult { .. }));
    }

    #[test]
    fn test_message_serde_heartbeat() {
        let msg = Message::Heartbeat {
            payload: HeartbeatPayload {
                worker_id: "w1".into(),
                active_tasks: 3,
                capacity: 10,
                uptime_seconds: 120,
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("activeTasks"));
        assert!(json.contains("uptimeSeconds"));
        let de: Message = serde_json::from_str(&json).unwrap();
        assert!(matches!(de, Message::Heartbeat { .. }));
    }

    #[test]
    fn test_message_serde_heartbeat_ack() {
        let msg = Message::HeartbeatAck;
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(json, r#"{"type":"HeartbeatAck"}"#);
        let de: Message = serde_json::from_str(&json).unwrap();
        assert!(matches!(de, Message::HeartbeatAck));
    }

    #[test]
    fn test_message_serde_worker_register() {
        let msg = Message::WorkerRegister {
            registration: WorkerRegistration {
                worker_id: "w1".into(),
                supported_tasks: vec!["a".into(), "b".into()],
                max_concurrency: 5,
                language: WorkerLanguage::Python,
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("supportedTasks"));
        assert!(json.contains("maxConcurrency"));
        let de: Message = serde_json::from_str(&json).unwrap();
        if let Message::WorkerRegister { registration: reg } = de {
            assert_eq!(reg.worker_id, "w1");
            assert_eq!(reg.supported_tasks.len(), 2);
        } else {
            panic!("Wrong variant");
        }
    }

    #[test]
    fn test_message_serde_worker_registered() {
        let msg = Message::WorkerRegistered {
            worker_id: "w1".into(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: Message = serde_json::from_str(&json).unwrap();
        if let Message::WorkerRegistered { worker_id } = de {
            assert_eq!(worker_id, "w1");
        } else {
            panic!("Wrong variant");
        }
    }

    #[test]
    fn test_message_serde_backpressure() {
        let msg = Message::Backpressure {
            signal: BackpressureSignal {
                worker_id: "w1".into(),
                current_load: 0.85,
                should_throttle: true,
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("currentLoad"));
        assert!(json.contains("shouldThrottle"));
        let de: Message = serde_json::from_str(&json).unwrap();
        if let Message::Backpressure { signal } = de {
            assert!((signal.current_load - 0.85).abs() < f64::EPSILON);
            assert!(signal.should_throttle);
        } else {
            panic!("Wrong variant");
        }
    }

    #[test]
    fn test_message_serde_kill() {
        let id = Uuid::new_v4();
        let msg = Message::Kill {
            task_id: id,
            reason: "timeout".into(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: Message = serde_json::from_str(&json).unwrap();
        if let Message::Kill { task_id, reason } = de {
            assert_eq!(task_id, id);
            assert_eq!(reason, "timeout");
        } else {
            panic!("Wrong variant");
        }
    }

    #[test]
    fn test_message_serde_shutdown_graceful() {
        let msg = Message::Shutdown { graceful: true };
        let json = serde_json::to_string(&msg).unwrap();
        let de: Message = serde_json::from_str(&json).unwrap();
        assert!(matches!(de, Message::Shutdown { graceful: true }));
    }

    #[test]
    fn test_message_serde_shutdown_not_graceful() {
        let msg = Message::Shutdown { graceful: false };
        let json = serde_json::to_string(&msg).unwrap();
        let de: Message = serde_json::from_str(&json).unwrap();
        assert!(matches!(de, Message::Shutdown { graceful: false }));
    }

    #[test]
    fn test_message_internally_tagged() {
        let msg = Message::HeartbeatAck;
        let json = serde_json::to_string(&msg).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(val.get("type").is_some());
    }

    #[test]
    fn test_message_deserialization_rejects_unknown_type() {
        let bad = r#"{"type":"UnknownMessage","data":{}}"#;
        let result = serde_json::from_str::<Message>(bad);
        assert!(result.is_err());
    }

    #[test]
    fn test_worker_language_serde_all_variants() {
        for lang in [
            WorkerLanguage::TypeScript,
            WorkerLanguage::Python,
            WorkerLanguage::Go,
        ] {
            let json = serde_json::to_string(&lang).unwrap();
            let de: WorkerLanguage = serde_json::from_str(&json).unwrap();
            assert_eq!(format!("{:?}", de), format!("{:?}", lang));
        }
    }

    #[test]
    fn test_worker_language_serde_other() {
        let lang = WorkerLanguage::Other("ruby".into());
        let json = serde_json::to_string(&lang).unwrap();
        let de: WorkerLanguage = serde_json::from_str(&json).unwrap();
        if let WorkerLanguage::Other(s) = de {
            assert_eq!(s, "ruby");
        } else {
            panic!("Expected Other variant");
        }
    }

    #[test]
    fn test_transport_error_display() {
        assert_eq!(
            TransportError::ConnectionFailed("timeout".into()).to_string(),
            "Connection failed: timeout"
        );
        assert_eq!(
            TransportError::WorkerNotFound("w1".into()).to_string(),
            "Worker not found: w1"
        );
        assert_eq!(
            TransportError::SendFailed("broken".into()).to_string(),
            "Send failed: broken"
        );
        assert_eq!(TransportError::Closed.to_string(), "Transport closed");
    }

    #[test]
    fn test_heartbeat_payload_camel_case() {
        let p = HeartbeatPayload {
            worker_id: "w".into(),
            active_tasks: 1,
            capacity: 5,
            uptime_seconds: 60,
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("workerId"));
        assert!(json.contains("activeTasks"));
        assert!(json.contains("uptimeSeconds"));
        assert!(!json.contains("worker_id"));
    }

    #[test]
    fn test_worker_registration_serde_roundtrip() {
        let reg = WorkerRegistration {
            worker_id: "w1".into(),
            supported_tasks: vec!["a".into(), "b".into()],
            max_concurrency: 10,
            language: WorkerLanguage::Go,
        };
        let json = serde_json::to_string(&reg).unwrap();
        let de: WorkerRegistration = serde_json::from_str(&json).unwrap();
        assert_eq!(de.worker_id, "w1");
        assert_eq!(de.supported_tasks.len(), 2);
        assert_eq!(de.max_concurrency, 10);
    }

    #[test]
    fn test_worker_registration_empty_tasks() {
        let reg = WorkerRegistration {
            worker_id: "w".into(),
            supported_tasks: vec![],
            max_concurrency: 1,
            language: WorkerLanguage::TypeScript,
        };
        let json = serde_json::to_string(&reg).unwrap();
        let de: WorkerRegistration = serde_json::from_str(&json).unwrap();
        assert!(de.supported_tasks.is_empty());
    }

    #[test]
    fn test_backpressure_signal_serde() {
        let sig = BackpressureSignal {
            worker_id: "w1".into(),
            current_load: 0.95,
            should_throttle: true,
        };
        let json = serde_json::to_string(&sig).unwrap();
        assert!(json.contains("currentLoad"));
        assert!(json.contains("shouldThrottle"));
        let de: BackpressureSignal = serde_json::from_str(&json).unwrap();
        assert!((de.current_load - 0.95).abs() < f64::EPSILON);
    }
}
