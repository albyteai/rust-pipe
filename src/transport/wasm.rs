use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::{mpsc, RwLock};

use super::{Message, Transport, TransportError};
use crate::schema::{Task, TaskError, TaskResult, TaskStatus};

/// Configuration for a WASM module worker.
pub struct WasmWorkerConfig {
    pub module_path: PathBuf,
    pub worker_id: String,
    pub supported_tasks: Vec<String>,
    pub max_memory_pages: u32,
    pub max_execution_time_ms: u64,
    pub allowed_env: Vec<String>,
}

/// Transport that executes tasks in sandboxed WebAssembly modules.
pub struct WasmTransport {
    configs: Vec<WasmWorkerConfig>,
    workers: Arc<RwLock<HashMap<String, WasmWorker>>>,
    on_message: Arc<dyn Fn(String, Message) + Send + Sync>,
}

struct WasmWorker {
    #[allow(dead_code)]
    config: WasmWorkerConfig,
    task_tx: mpsc::UnboundedSender<Task>,
}

impl WasmTransport {
    pub fn new(
        configs: Vec<WasmWorkerConfig>,
        on_message: impl Fn(String, Message) + Send + Sync + 'static,
    ) -> Self {
        Self {
            configs,
            workers: Arc::new(RwLock::new(HashMap::new())),
            on_message: Arc::new(on_message),
        }
    }
}

#[async_trait]
impl Transport for WasmTransport {
    async fn start(&self) -> Result<(), TransportError> {
        for config in &self.configs {
            let worker_id = config.worker_id.clone();
            let on_message = self.on_message.clone();
            let workers = self.workers.clone();

            // Verify module exists
            if !config.module_path.exists() {
                return Err(TransportError::ConnectionFailed(format!(
                    "WASM module not found: {}",
                    config.module_path.display()
                )));
            }

            let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task>();

            // Register worker
            let reg_msg = Message::WorkerRegister {
                registration: super::WorkerRegistration {
                    worker_id: worker_id.clone(),
                    supported_tasks: config.supported_tasks.clone(),
                    max_concurrency: 1,
                    language: super::WorkerLanguage::Other("wasm".to_string()),
                },
            };
            on_message(worker_id.clone(), reg_msg);

            // WASM execution loop
            let wid = worker_id.clone();
            let module_path = config.module_path.clone();
            let max_time = config.max_execution_time_ms;

            tokio::spawn(async move {
                while let Some(task) = task_rx.recv().await {
                    let start = std::time::Instant::now();
                    let task_id = task.id;

                    // Execute WASM module via wasmtime CLI
                    // The WASM module reads task JSON from stdin, writes result JSON to stdout
                    let task_json = serde_json::to_string(&task).unwrap_or_default();

                    let result = tokio::time::timeout(
                        std::time::Duration::from_millis(max_time),
                        execute_wasm_module(&module_path, &task_json),
                    )
                    .await;

                    let duration_ms = start.elapsed().as_millis() as u64;

                    let task_result = match result {
                        Ok(Ok(output)) => {
                            match serde_json::from_str::<serde_json::Value>(&output) {
                                Ok(payload) => TaskResult {
                                    task_id,
                                    status: TaskStatus::Completed,
                                    payload: Some(payload),
                                    error: None,
                                    duration_ms,
                                    worker_id: wid.clone(),
                                },
                                Err(e) => TaskResult {
                                    task_id,
                                    status: TaskStatus::Failed,
                                    payload: None,
                                    error: Some(TaskError {
                                        code: "PARSE_ERROR".to_string(),
                                        message: format!("Failed to parse WASM output: {}", e),
                                        retryable: false,
                                    }),
                                    duration_ms,
                                    worker_id: wid.clone(),
                                },
                            }
                        }
                        Ok(Err(e)) => TaskResult {
                            task_id,
                            status: TaskStatus::Failed,
                            payload: None,
                            error: Some(TaskError {
                                code: "EXECUTION_ERROR".to_string(),
                                message: e,
                                retryable: true,
                            }),
                            duration_ms,
                            worker_id: wid.clone(),
                        },
                        Err(_) => TaskResult {
                            task_id,
                            status: TaskStatus::TimedOut,
                            payload: None,
                            error: Some(TaskError {
                                code: "TIMEOUT".to_string(),
                                message: format!("WASM execution exceeded {}ms", max_time),
                                retryable: true,
                            }),
                            duration_ms,
                            worker_id: wid.clone(),
                        },
                    };

                    on_message(
                        wid.clone(),
                        Message::TaskResult {
                            result: task_result,
                        },
                    );
                }
            });

            workers.write().await.insert(
                worker_id.clone(),
                WasmWorker {
                    config: WasmWorkerConfig {
                        module_path: config.module_path.clone(),
                        worker_id: config.worker_id.clone(),
                        supported_tasks: config.supported_tasks.clone(),
                        max_memory_pages: config.max_memory_pages,
                        max_execution_time_ms: config.max_execution_time_ms,
                        allowed_env: config.allowed_env.clone(),
                    },
                    task_tx,
                },
            );

            tracing::info!(
                worker_id = %worker_id,
                module = %config.module_path.display(),
                "WASM worker registered"
            );
        }

        Ok(())
    }

    async fn stop(&self) -> Result<(), TransportError> {
        self.workers.write().await.clear();
        Ok(())
    }

    async fn send(&self, worker_id: &str, message: Message) -> Result<(), TransportError> {
        let workers = self.workers.read().await;
        let worker = workers
            .get(worker_id)
            .ok_or_else(|| TransportError::WorkerNotFound(worker_id.to_string()))?;

        if let Message::TaskDispatch { task } = message {
            worker
                .task_tx
                .send(task)
                .map_err(|e| TransportError::SendFailed(e.to_string()))?;
        }

        Ok(())
    }

    async fn broadcast(&self, message: Message) -> Result<(), TransportError> {
        if let Message::TaskDispatch { ref task } = message {
            let workers = self.workers.read().await;
            for (_, worker) in workers.iter() {
                let _ = worker.task_tx.send(task.clone());
            }
        }
        Ok(())
    }
}

async fn execute_wasm_module(
    module_path: &std::path::Path,
    input_json: &str,
) -> Result<String, String> {
    use tokio::io::AsyncWriteExt;

    let mut child = Command::new("wasmtime")
        .args(["run", &module_path.to_string_lossy()])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("Failed to spawn wasmtime: {}", e))?;

    // Write task JSON to the module's stdin
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(input_json.as_bytes())
            .await
            .map_err(|e| format!("Failed to write to wasmtime stdin: {}", e))?;
        drop(stdin);
    }

    let output = child
        .wait_with_output()
        .await
        .map_err(|e| format!("wasmtime execution failed: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("WASM module failed: {}", stderr));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}
