use rust_pipe::prelude::*;
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Create a dispatcher that listens for workers on port 9876
    let dispatcher = Dispatcher::builder()
        .host("0.0.0.0")
        .port(9876)
        .heartbeat_timeout(15_000)
        .build();

    // Start accepting worker connections
    dispatcher.start().await?;

    println!("rust-pipe dispatcher running on ws://0.0.0.0:9876");
    println!("Waiting for workers to connect...");

    // Wait for workers to register
    tokio::time::sleep(Duration::from_secs(5)).await;

    let stats = dispatcher.pool_stats();
    println!("Connected workers: {}", stats.total);

    // Dispatch a task to any available worker that handles "scan-target"
    let task = Task::new(
        "scan-target",
        json!({
            "url": "https://example.com",
            "checks": ["xss", "sqli", "ssrf"]
        }),
    )
    .with_timeout(60_000)
    .with_priority(Priority::High);

    let handle = dispatcher.dispatch(task).await?;
    println!("Task dispatched: {}", handle.task_id);

    // Wait for result with timeout
    let result = handle.await_with_timeout(Duration::from_secs(60)).await?;

    match result.status {
        TaskStatus::Completed => {
            println!("Task completed in {}ms", result.duration_ms);
            println!("Result: {:?}", result.payload);
        }
        TaskStatus::Failed => {
            println!("Task failed: {:?}", result.error);
        }
        _ => {}
    }

    Ok(())
}
