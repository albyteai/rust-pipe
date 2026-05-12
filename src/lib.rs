//! # rust-pipe
//!
//! Lightweight typed task dispatch from Rust to polyglot workers.
//!
//! rust-pipe lets you dispatch tasks from a Rust orchestrator to workers written in
//! TypeScript, Python, Go, Java, C#, Ruby, Elixir, Swift, PHP, or any CLI tool.
//!
//! ## Quick start
//!
//! ```no_run
//! use rust_pipe::prelude::*;
//! use serde_json::json;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() {
//!     let dispatcher = Dispatcher::builder()
//!         .host("0.0.0.0")
//!         .port(9876)
//!         .build();
//!
//!     dispatcher.start().await.unwrap();
//!
//!     let task = Task::new("my-task", json!({"key": "value"}))
//!         .with_timeout(30_000)
//!         .with_priority(Priority::High);
//!
//!     let handle = dispatcher.dispatch(task).await.unwrap();
//!     let result = handle.await_with_timeout(Duration::from_secs(30)).await.unwrap();
//!     println!("Done: {:?}", result.payload);
//! }
//! ```

pub mod dispatch;
pub mod schema;
pub mod transport;
pub mod validation;
pub mod worker;

/// Common types re-exported for convenience.
pub mod prelude {
    pub use crate::dispatch::{DispatchError, DispatchResult, Dispatcher, DispatcherBuilder};
    pub use crate::schema::{Priority, Task, TaskResult, TaskStatus};
    pub use crate::transport::TransportConfig;
    pub use crate::worker::{WorkerPool, WorkerStatus};
}
