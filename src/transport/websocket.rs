use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::accept_async;

use super::{Message, Transport, TransportConfig, TransportError};

type WorkerSender = mpsc::UnboundedSender<Message>;

pub struct WebSocketTransport {
    config: TransportConfig,
    workers: Arc<RwLock<HashMap<String, WorkerSender>>>,
    on_message: Arc<dyn Fn(String, Message) + Send + Sync>,
}

impl WebSocketTransport {
    pub fn new(
        config: TransportConfig,
        on_message: impl Fn(String, Message) + Send + Sync + 'static,
    ) -> Self {
        Self {
            config,
            workers: Arc::new(RwLock::new(HashMap::new())),
            on_message: Arc::new(on_message),
        }
    }
}

#[async_trait]
impl Transport for WebSocketTransport {
    async fn start(&self) -> Result<(), TransportError> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

        tracing::info!("rust-pipe transport listening on {}", addr);

        let workers = self.workers.clone();
        let on_message = self.on_message.clone();

        tokio::spawn(async move {
            while let Ok((stream, peer_addr)) = listener.accept().await {
                let workers = workers.clone();
                let on_message = on_message.clone();

                tokio::spawn(async move {
                    let ws_stream = match accept_async(stream).await {
                        Ok(ws) => ws,
                        Err(e) => {
                            tracing::error!("WebSocket handshake failed from {}: {}", peer_addr, e);
                            return;
                        }
                    };

                    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
                    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

                    let mut worker_id = String::new();

                    // Outbound message forwarder
                    let send_task = tokio::spawn(async move {
                        while let Some(msg) = rx.recv().await {
                            let json = match serde_json::to_string(&msg) {
                                Ok(j) => j,
                                Err(e) => {
                                    tracing::error!(error = %e, "Failed to serialize outbound message");
                                    continue;
                                }
                            };
                            if ws_sender
                                .send(tokio_tungstenite::tungstenite::Message::Text(json))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    });

                    // Inbound message handler
                    while let Some(Ok(msg)) = ws_receiver.next().await {
                        if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                            let message = match serde_json::from_str::<Message>(&text) {
                                Ok(m) => m,
                                Err(e) => {
                                    tracing::warn!(
                                        error = %e,
                                        raw = %&text[..text.len().min(200)],
                                        "Failed to parse inbound message"
                                    );
                                    continue;
                                }
                            };
                            if let Message::WorkerRegister {
                                registration: ref reg,
                            } = message
                            {
                                worker_id = reg.worker_id.clone();
                                workers.write().await.insert(worker_id.clone(), tx.clone());
                                tracing::info!(
                                    worker_id = %worker_id,
                                    language = ?reg.language,
                                    tasks = ?reg.supported_tasks,
                                    "Worker registered"
                                );
                            }
                            on_message(worker_id.clone(), message);
                        }
                    }

                    // Cleanup on disconnect
                    if !worker_id.is_empty() {
                        workers.write().await.remove(&worker_id);
                        tracing::info!(worker_id = %worker_id, "Worker disconnected");
                    }
                    send_task.abort();
                });
            }
        });

        Ok(())
    }

    async fn stop(&self) -> Result<(), TransportError> {
        let workers = self.workers.read().await;
        for (_, sender) in workers.iter() {
            let _ = sender.send(Message::Shutdown { graceful: true });
        }
        Ok(())
    }

    async fn send(&self, worker_id: &str, message: Message) -> Result<(), TransportError> {
        let workers = self.workers.read().await;
        let sender = workers
            .get(worker_id)
            .ok_or_else(|| TransportError::WorkerNotFound(worker_id.to_string()))?;

        sender
            .send(message)
            .map_err(|e| TransportError::SendFailed(e.to_string()))
    }

    async fn broadcast(&self, message: Message) -> Result<(), TransportError> {
        let workers = self.workers.read().await;
        for (_, sender) in workers.iter() {
            let _ = sender.send(message.clone());
        }
        Ok(())
    }
}
