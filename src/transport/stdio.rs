use async_trait::async_trait;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, RwLock};
use tracing;

use super::{Message, Transport, TransportError};

/// Configuration for a stdio-based worker process.
pub struct StdioProcess {
    pub command: String,
    pub args: Vec<String>,
    pub worker_id: String,
    pub supported_tasks: Vec<String>,
}

/// Transport that communicates with workers via stdin/stdout pipes.
pub struct StdioTransport {
    processes: Arc<RwLock<HashMap<String, StdioWorker>>>,
    configs: Vec<StdioProcess>,
    on_message: Arc<dyn Fn(String, Message) + Send + Sync>,
}

struct StdioWorker {
    stdin_tx: mpsc::UnboundedSender<String>,
}

impl StdioTransport {
    pub fn new(
        configs: Vec<StdioProcess>,
        on_message: impl Fn(String, Message) + Send + Sync + 'static,
    ) -> Self {
        Self {
            processes: Arc::new(RwLock::new(HashMap::new())),
            configs,
            on_message: Arc::new(on_message),
        }
    }
}

#[async_trait]
impl Transport for StdioTransport {
    async fn start(&self) -> Result<(), TransportError> {
        for config in &self.configs {
            let worker_id = config.worker_id.clone();
            let on_message = self.on_message.clone();
            let processes = self.processes.clone();

            let mut child = Command::new(&config.command)
                .args(&config.args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| {
                    TransportError::ConnectionFailed(format!(
                        "Failed to spawn '{}': {}",
                        config.command, e
                    ))
                })?;

            let stdin = child.stdin.take().expect("stdin piped");
            let stdout = child.stdout.take().expect("stdout piped");

            let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel::<String>();

            // Register as worker
            let reg_msg = Message::WorkerRegister {
                registration: super::WorkerRegistration {
                    worker_id: worker_id.clone(),
                    supported_tasks: config.supported_tasks.clone(),
                    max_concurrency: 1,
                    language: super::WorkerLanguage::Other("stdio".to_string()),
                },
            };
            on_message(worker_id.clone(), reg_msg);

            // Stdin writer task
            let wid = worker_id.clone();
            tokio::spawn(async move {
                let mut stdin = stdin;
                while let Some(line) = stdin_rx.recv().await {
                    if stdin.write_all(line.as_bytes()).await.is_err() {
                        tracing::error!(worker_id = %wid, "Failed to write to stdin");
                        break;
                    }
                    if stdin.write_all(b"\n").await.is_err() {
                        break;
                    }
                    let _ = stdin.flush().await;
                }
            });

            // Stdout reader task
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
                                line = %line,
                                error = %e,
                                "Non-JSON line from worker, ignoring"
                            );
                        }
                    }
                }
                tracing::info!(worker_id = %wid, "Stdio worker stdout closed");
            });

            processes
                .write()
                .await
                .insert(worker_id.clone(), StdioWorker { stdin_tx });

            tracing::info!(
                worker_id = %worker_id,
                command = %config.command,
                "Stdio worker spawned"
            );
        }

        Ok(())
    }

    async fn stop(&self) -> Result<(), TransportError> {
        let processes = self.processes.read().await;
        for (worker_id, worker) in processes.iter() {
            let shutdown = Message::Shutdown { graceful: true };
            let json = serde_json::to_string(&shutdown).unwrap_or_default();
            let _ = worker.stdin_tx.send(json);
            tracing::info!(worker_id = %worker_id, "Sent shutdown to stdio worker");
        }
        Ok(())
    }

    async fn send(&self, worker_id: &str, message: Message) -> Result<(), TransportError> {
        let processes = self.processes.read().await;
        let worker = processes
            .get(worker_id)
            .ok_or_else(|| TransportError::WorkerNotFound(worker_id.to_string()))?;

        let json = serde_json::to_string(&message)
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        worker
            .stdin_tx
            .send(json)
            .map_err(|e| TransportError::SendFailed(e.to_string()))
    }

    async fn broadcast(&self, message: Message) -> Result<(), TransportError> {
        let processes = self.processes.read().await;
        let json = serde_json::to_string(&message)
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        for (_, worker) in processes.iter() {
            let _ = worker.stdin_tx.send(json.clone());
        }
        Ok(())
    }
}
