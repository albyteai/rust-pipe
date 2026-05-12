use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, RwLock};

use super::{Message, Transport, TransportError};
use crate::validation;

/// Configuration for a Docker container worker.
pub struct DockerWorkerConfig {
    pub image: String,
    pub worker_id: String,
    pub supported_tasks: Vec<String>,
    pub env: HashMap<String, String>,
    pub volumes: Vec<String>,
    pub network: Option<String>,
    pub memory_limit: Option<String>,
    pub cpu_limit: Option<String>,
}

/// Transport that runs workers inside Docker containers.
pub struct DockerTransport {
    configs: Vec<DockerWorkerConfig>,
    containers: Arc<RwLock<HashMap<String, DockerContainer>>>,
    on_message: Arc<dyn Fn(String, Message) + Send + Sync>,
}

struct DockerContainer {
    container_id: String,
    stdin_tx: mpsc::UnboundedSender<String>,
}

impl DockerTransport {
    pub fn new(
        configs: Vec<DockerWorkerConfig>,
        on_message: impl Fn(String, Message) + Send + Sync + 'static,
    ) -> Self {
        Self {
            configs,
            containers: Arc::new(RwLock::new(HashMap::new())),
            on_message: Arc::new(on_message),
        }
    }

    async fn start_container(config: &DockerWorkerConfig) -> Result<String, TransportError> {
        // Validate inputs to prevent command injection
        validation::validate_worker_id(&config.worker_id)
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
        validation::validate_docker_image(&config.image)
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

        for (key, val) in &config.env {
            validation::validate_no_shell_metacharacters(key, "env key")
                .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
            validation::validate_no_shell_metacharacters(val, "env value")
                .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
        }

        for vol in &config.volumes {
            validation::validate_no_shell_metacharacters(vol, "volume")
                .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
        }

        let mut args = vec![
            "run".to_string(),
            "-d".to_string(),
            "-i".to_string(),
            "--rm".to_string(),
            "--name".to_string(),
            format!("rust-pipe-{}", config.worker_id),
        ];

        if let Some(ref mem) = config.memory_limit {
            args.push("--memory".to_string());
            args.push(mem.clone());
        }

        if let Some(ref cpu) = config.cpu_limit {
            args.push("--cpus".to_string());
            args.push(cpu.clone());
        }

        if let Some(ref network) = config.network {
            args.push("--network".to_string());
            args.push(network.clone());
        }

        for vol in &config.volumes {
            args.push("-v".to_string());
            args.push(vol.clone());
        }

        for (key, val) in &config.env {
            args.push("-e".to_string());
            args.push(format!("{}={}", key, val));
        }

        args.push(config.image.clone());

        let output = Command::new("docker")
            .args(&args)
            .output()
            .await
            .map_err(|e| TransportError::ConnectionFailed(format!("docker run failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(TransportError::ConnectionFailed(format!(
                "docker run failed: {}",
                stderr
            )));
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(container_id)
    }
}

#[async_trait]
impl Transport for DockerTransport {
    async fn start(&self) -> Result<(), TransportError> {
        for config in &self.configs {
            let container_id = Self::start_container(config).await?;
            let worker_id = config.worker_id.clone();
            let on_message = self.on_message.clone();
            let containers = self.containers.clone();

            // Attach to container stdin/stdout
            let mut attach = Command::new("docker")
                .args(["attach", "--no-stdin=false", &container_id])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .spawn()
                .map_err(|e| {
                    TransportError::ConnectionFailed(format!("docker attach failed: {}", e))
                })?;

            let stdin = attach.stdin.take().expect("stdin piped");
            let stdout = attach.stdout.take().expect("stdout piped");
            let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel::<String>();

            // Register worker
            let reg_msg = Message::WorkerRegister {
                registration: super::WorkerRegistration {
                    worker_id: worker_id.clone(),
                    supported_tasks: config.supported_tasks.clone(),
                    max_concurrency: 1,
                    language: super::WorkerLanguage::Other("docker".to_string()),
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
                        tracing::error!(worker_id = %wid, "Docker stdin write failed");
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
                            tracing::debug!(worker_id = %wid, error = %e, "Non-JSON from docker worker");
                        }
                    }
                }
            });

            containers.write().await.insert(
                worker_id.clone(),
                DockerContainer {
                    container_id,
                    stdin_tx,
                },
            );

            tracing::info!(
                worker_id = %worker_id,
                image = %config.image,
                "Docker worker container started"
            );
        }

        Ok(())
    }

    async fn stop(&self) -> Result<(), TransportError> {
        let containers = self.containers.read().await;
        for (worker_id, container) in containers.iter() {
            let _ = container.stdin_tx.send(
                serde_json::to_string(&Message::Shutdown { graceful: true }).unwrap_or_default(),
            );

            // Stop container
            let _ = Command::new("docker")
                .args(["stop", "-t", "10", &container.container_id])
                .output()
                .await;

            tracing::info!(worker_id = %worker_id, "Docker container stopped");
        }
        Ok(())
    }

    async fn send(&self, worker_id: &str, message: Message) -> Result<(), TransportError> {
        let containers = self.containers.read().await;
        let container = containers
            .get(worker_id)
            .ok_or_else(|| TransportError::WorkerNotFound(worker_id.to_string()))?;

        let json = serde_json::to_string(&message)
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        container
            .stdin_tx
            .send(json)
            .map_err(|e| TransportError::SendFailed(e.to_string()))
    }

    async fn broadcast(&self, message: Message) -> Result<(), TransportError> {
        let containers = self.containers.read().await;
        let json = serde_json::to_string(&message)
            .map_err(|e| TransportError::SendFailed(e.to_string()))?;

        for (_, container) in containers.iter() {
            let _ = container.stdin_tx.send(json.clone());
        }
        Ok(())
    }
}
