use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, RwLock};

use super::{Message, Transport, TransportError};
use crate::validation;

/// Configuration for a remote SSH worker.
pub struct SshWorkerConfig {
    pub host: String,
    pub user: String,
    pub port: u16,
    pub worker_id: String,
    pub supported_tasks: Vec<String>,
    pub remote_command: String,
    pub identity_file: Option<String>,
    pub connect_timeout_secs: u32,
}

/// Transport that dispatches tasks to remote machines over SSH.
pub struct SshTransport {
    configs: Vec<SshWorkerConfig>,
    connections: Arc<RwLock<HashMap<String, SshConnection>>>,
    on_message: Arc<dyn Fn(String, Message) + Send + Sync>,
}

struct SshConnection {
    stdin_tx: mpsc::UnboundedSender<String>,
}

impl SshTransport {
    pub fn new(
        configs: Vec<SshWorkerConfig>,
        on_message: impl Fn(String, Message) + Send + Sync + 'static,
    ) -> Self {
        Self {
            configs,
            connections: Arc::new(RwLock::new(HashMap::new())),
            on_message: Arc::new(on_message),
        }
    }
}

#[async_trait]
impl Transport for SshTransport {
    async fn start(&self) -> Result<(), TransportError> {
        for config in &self.configs {
            // Validate inputs to prevent command injection
            validation::validate_worker_id(&config.worker_id)
                .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
            validation::validate_hostname(&config.host)
                .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
            validation::validate_username(&config.user)
                .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
            validation::validate_no_shell_metacharacters(&config.remote_command, "remote_command")
                .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

            if let Some(ref key) = config.identity_file {
                validation::validate_file_path(key, "identity_file")
                    .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
            }

            let worker_id = config.worker_id.clone();
            let on_message = self.on_message.clone();
            let connections = self.connections.clone();

            let mut ssh_args = vec![
                "-o".to_string(),
                "StrictHostKeyChecking=yes".to_string(),
                "-o".to_string(),
                format!("ConnectTimeout={}", config.connect_timeout_secs),
                "-o".to_string(),
                "ServerAliveInterval=5".to_string(),
                "-o".to_string(),
                "ServerAliveCountMax=3".to_string(),
                "-p".to_string(),
                config.port.to_string(),
            ];

            if let Some(ref key) = config.identity_file {
                ssh_args.push("-i".to_string());
                ssh_args.push(key.clone());
            }

            ssh_args.push("--".to_string());
            ssh_args.push(format!("{}@{}", config.user, config.host));
            ssh_args.push(config.remote_command.clone());

            let mut child = Command::new("ssh")
                .args(&ssh_args)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .map_err(|e| {
                    TransportError::ConnectionFailed(format!(
                        "SSH to {}@{}:{} failed: {}",
                        config.user, config.host, config.port, e
                    ))
                })?;

            let stdin = child.stdin.take().expect("stdin piped");
            let stdout = child.stdout.take().expect("stdout piped");
            let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel::<String>();

            // Register worker
            let reg_msg = Message::WorkerRegister {
                registration: super::WorkerRegistration {
                    worker_id: worker_id.clone(),
                    supported_tasks: config.supported_tasks.clone(),
                    max_concurrency: 1,
                    language: super::WorkerLanguage::Other("ssh".to_string()),
                    tags: None,
                },
            };
            on_message(worker_id.clone(), reg_msg);

            // Stdin writer
            let wid = worker_id.clone();
            tokio::spawn(async move {
                let mut stdin = stdin;
                while let Some(line) = stdin_rx.recv().await {
                    if stdin.write_all(line.as_bytes()).await.is_err() {
                        tracing::error!(worker_id = %wid, "SSH stdin write failed");
                        break;
                    }
                    if stdin.write_all(b"\n").await.is_err() {
                        break;
                    }
                    let _ = stdin.flush().await;
                }
            });

            // Stdout reader
            let wid = worker_id.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if line.trim().is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Message>(&line) {
                        Ok(msg) => on_message(wid.clone(), msg),
                        Err(e) => {
                            tracing::debug!(
                                worker_id = %wid,
                                error = %e,
                                "Non-JSON from SSH worker"
                            );
                        }
                    }
                }
                tracing::info!(worker_id = %wid, "SSH connection closed");
            });

            connections
                .write()
                .await
                .insert(worker_id.clone(), SshConnection { stdin_tx });

            tracing::info!(
                worker_id = %worker_id,
                host = %config.host,
                command = %config.remote_command,
                "SSH worker connected"
            );
        }

        Ok(())
    }

    async fn stop(&self) -> Result<(), TransportError> {
        let connections = self.connections.read().await;
        for (worker_id, conn) in connections.iter() {
            let shutdown =
                serde_json::to_string(&Message::Shutdown { graceful: true }).unwrap_or_default();
            let _ = conn.stdin_tx.send(shutdown);
            tracing::info!(worker_id = %worker_id, "SSH worker shutdown sent");
        }
        Ok(())
    }

    async fn send(&self, worker_id: &str, message: Message) -> Result<(), TransportError> {
        let connections = self.connections.read().await;
        let conn = connections
            .get(worker_id)
            .ok_or_else(|| TransportError::WorkerNotFound(worker_id.to_string()))?;

        let json = serde_json::to_string(&message)
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        conn.stdin_tx
            .send(json)
            .map_err(|e| TransportError::SendFailed(e.to_string()))
    }

    async fn broadcast(&self, message: Message) -> Result<(), TransportError> {
        let connections = self.connections.read().await;
        let json = serde_json::to_string(&message)
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        for (_, conn) in connections.iter() {
            let _ = conn.stdin_tx.send(json.clone());
        }
        Ok(())
    }
}
