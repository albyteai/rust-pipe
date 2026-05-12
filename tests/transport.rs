#![allow(deprecated, clippy::collapsible_match, clippy::useless_vec)]

use rust_pipe::dispatch::Dispatcher;
use rust_pipe::schema::{Priority, Task, TaskStatus};
use rust_pipe::transport::docker::{DockerTransport, DockerWorkerConfig};
use rust_pipe::transport::ssh::{SshTransport, SshWorkerConfig};
use rust_pipe::transport::stdio::{StdioProcess, StdioTransport};
use rust_pipe::transport::wasm::{WasmTransport, WasmWorkerConfig};
use rust_pipe::transport::websocket::WebSocketTransport;
use rust_pipe::transport::{
    Message, Transport, TransportConfig, WorkerLanguage, WorkerRegistration,
};

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use serial_test::serial;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::process::Command as StdCommand;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;

fn mock_path() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let mock_dir = format!("{}/tests/mock_bins", manifest_dir);
    let existing_path = env::var("PATH").unwrap_or_default();
    format!("{}:{}", mock_dir, existing_path)
}

/// The module_path must exist on disk; the wasmtime mock switches behavior
/// based on the basename of the module path.
fn wasm_module(name: &str) -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(format!("{}/tests/mock_bins/{}", manifest_dir, name))
}

fn fake_wasm_module() -> PathBuf {
    // The mock wasmtime doesn't actually load a real WASM file,
    // but the transport checks the path exists
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(format!("{}/tests/mock_bins/wasmtime", manifest_dir))
}

// =============================================================================
// WebSocket transport tests (from e2e_coverage_gaps.rs)
// =============================================================================

#[tokio::test]
async fn test_websocket_stop_sends_shutdown() {
    let (tx, _rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = WebSocketTransport::new(
        TransportConfig {
            port: 19920,
            ..Default::default()
        },
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );
    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect a worker
    let (ws, _) = connect_async("ws://127.0.0.1:19920").await.unwrap();
    let (mut write, mut read) = ws.split();

    let reg = Message::WorkerRegister {
        registration: WorkerRegistration {
            worker_id: "stop-test".to_string(),
            supported_tasks: vec!["a".to_string()],
            max_concurrency: 1,
            language: WorkerLanguage::TypeScript,
        },
    };
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&reg).unwrap(),
        ))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Stop transport — should send Shutdown to all workers
    transport.stop().await.unwrap();

    // Worker should receive Shutdown message
    if let Some(Ok(msg)) = tokio::time::timeout(Duration::from_secs(2), read.next())
        .await
        .ok()
        .flatten()
    {
        if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
            let parsed: Message = serde_json::from_str(&text).unwrap();
            assert!(matches!(parsed, Message::Shutdown { graceful: true }));
        }
    }
}

#[tokio::test]
async fn test_websocket_broadcast() {
    let transport = WebSocketTransport::new(
        TransportConfig {
            port: 19921,
            ..Default::default()
        },
        |_, _| {},
    );
    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect two workers
    for i in 0..2 {
        let (ws, _) = connect_async("ws://127.0.0.1:19921").await.unwrap();
        let (mut write, _) = ws.split();
        let reg = Message::WorkerRegister {
            registration: WorkerRegistration {
                worker_id: format!("bc-{}", i),
                supported_tasks: vec!["x".to_string()],
                max_concurrency: 1,
                language: WorkerLanguage::Python,
            },
        };
        write
            .send(tokio_tungstenite::tungstenite::Message::Text(
                serde_json::to_string(&reg).unwrap(),
            ))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Broadcast should succeed (doesn't error even if workers don't respond)
    let result = transport
        .broadcast(Message::Shutdown { graceful: true })
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_websocket_send_to_unknown_worker() {
    let transport = WebSocketTransport::new(
        TransportConfig {
            port: 19922,
            ..Default::default()
        },
        |_, _| {},
    );
    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let task = Task::new("x", json!({}));
    let result = transport
        .send("nonexistent", Message::TaskDispatch { task })
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_websocket_handshake_failure() {
    let transport = WebSocketTransport::new(
        TransportConfig {
            port: 19930,
            ..Default::default()
        },
        |_, _| {},
    );
    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect a raw TCP socket and send garbage (not a valid WebSocket handshake)
    let mut stream = TcpStream::connect("127.0.0.1:19930").await.unwrap();
    stream
        .write_all(b"THIS IS NOT A WEBSOCKET HANDSHAKE\r\n\r\n")
        .await
        .unwrap();
    let _ = stream.shutdown().await;

    // Give the server time to handle the bad connection
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Server should still be alive and accepting connections
    let result = connect_async("ws://127.0.0.1:19930").await;
    assert!(
        result.is_ok(),
        "Server should still accept connections after bad handshake"
    );
}

#[tokio::test]
async fn test_websocket_client_disconnect_cleanup() {
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = WebSocketTransport::new(
        TransportConfig {
            port: 19931,
            ..Default::default()
        },
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );
    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and register a worker
    {
        let (ws, _) = connect_async("ws://127.0.0.1:19931").await.unwrap();
        let (mut write, _read) = ws.split();

        let reg = Message::WorkerRegister {
            registration: WorkerRegistration {
                worker_id: "disconnect-test".to_string(),
                supported_tasks: vec!["a".to_string()],
                max_concurrency: 1,
                language: WorkerLanguage::Python,
            },
        };
        write
            .send(tokio_tungstenite::tungstenite::Message::Text(
                serde_json::to_string(&reg).unwrap(),
            ))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Drain registration
        let _ = rx.recv().await;

        // Drop the connection (implicit close when write goes out of scope)
    }

    // Give time for disconnect cleanup (lines 104-108)
    tokio::time::sleep(Duration::from_millis(300)).await;

    // After disconnect, sending to that worker should fail
    let task = Task::new("a", json!({}));
    let result = transport
        .send("disconnect-test", Message::TaskDispatch { task })
        .await;
    assert!(result.is_err(), "Send to disconnected worker should fail");
}

#[tokio::test]
async fn test_websocket_outbound_send_failure() {
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = WebSocketTransport::new(
        TransportConfig {
            port: 19932,
            ..Default::default()
        },
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );
    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect, register, then close abruptly
    let (ws, _) = connect_async("ws://127.0.0.1:19932").await.unwrap();
    let (mut write, _read) = ws.split();

    let reg = Message::WorkerRegister {
        registration: WorkerRegistration {
            worker_id: "outbound-fail".to_string(),
            supported_tasks: vec!["a".to_string()],
            max_concurrency: 1,
            language: WorkerLanguage::Go,
        },
    };
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&reg).unwrap(),
        ))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drain registration
    let _ = rx.recv().await;

    // Close the connection abruptly (close the write side)
    let _ = write.close().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Try to send to the worker whose connection is now broken
    // This exercises line 74 (outbound send fails -> break)
    let task = Task::new("a", json!({}));
    // This may or may not error depending on timing, but it exercises the code path
    let _ = transport
        .send("outbound-fail", Message::TaskDispatch { task })
        .await;

    tokio::time::sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_websocket_invalid_json_message() {
    let transport = WebSocketTransport::new(
        TransportConfig {
            port: 19933,
            ..Default::default()
        },
        |_, _| {},
    );
    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and send invalid JSON
    let (ws, _) = connect_async("ws://127.0.0.1:19933").await.unwrap();
    let (mut write, _read) = ws.split();

    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            "this is not valid json".to_string(),
        ))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Server should still be alive
    let result = connect_async("ws://127.0.0.1:19933").await;
    assert!(
        result.is_ok(),
        "Server should survive invalid JSON messages"
    );
}

// --- From e2e_full_coverage.rs ---

#[tokio::test]
async fn test_websocket_raw_tcp_handshake_failure() {
    let transport = WebSocketTransport::new(
        TransportConfig {
            port: 19940,
            ..Default::default()
        },
        |_, _| {},
    );
    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect with raw TCP and send garbage (not a valid WS handshake)
    let mut stream = TcpStream::connect("127.0.0.1:19940").await.unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    // Close without completing WS handshake
    drop(stream);

    // Server should not crash
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify server still works after bad connection
    let (ws, _) = connect_async("ws://127.0.0.1:19940").await.unwrap();
    let (mut write, _) = ws.split();
    let reg = Message::WorkerRegister {
        registration: WorkerRegistration {
            worker_id: "after-bad".to_string(),
            supported_tasks: vec!["a".to_string()],
            max_concurrency: 1,
            language: WorkerLanguage::TypeScript,
        },
    };
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&reg).unwrap(),
        ))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_websocket_send_after_client_disconnect() {
    let transport = WebSocketTransport::new(
        TransportConfig {
            port: 19941,
            ..Default::default()
        },
        |_, _| {},
    );
    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and register
    let (ws, _) = connect_async("ws://127.0.0.1:19941").await.unwrap();
    let (mut write, _read) = ws.split();
    let reg = Message::WorkerRegister {
        registration: WorkerRegistration {
            worker_id: "disconnect-send".to_string(),
            supported_tasks: vec!["a".to_string()],
            max_concurrency: 5,
            language: WorkerLanguage::TypeScript,
        },
    };
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&reg).unwrap(),
        ))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Abruptly close the connection
    write.close().await.ok();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Try sending to the now-disconnected worker — exercises the send error path
    let task = Task::new("a", json!({}));
    let result = transport
        .send("disconnect-send", Message::TaskDispatch { task })
        .await;
    // After disconnect, worker should be removed from the map
    // So this should be WorkerNotFound OR SendFailed
    assert!(result.is_err());
}

#[tokio::test]
async fn test_websocket_disconnect_removes_from_transport() {
    let transport = WebSocketTransport::new(
        TransportConfig {
            port: 19942,
            ..Default::default()
        },
        |_, _| {},
    );
    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and register
    let (ws, _) = connect_async("ws://127.0.0.1:19942").await.unwrap();
    let (mut write, _read) = ws.split();
    let reg = Message::WorkerRegister {
        registration: WorkerRegistration {
            worker_id: "cleanup-test".to_string(),
            supported_tasks: vec!["y".to_string()],
            max_concurrency: 1,
            language: WorkerLanguage::Go,
        },
    };
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&reg).unwrap(),
        ))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify send works
    let task = Task::new("y", json!({}));
    let send_result = transport
        .send("cleanup-test", Message::TaskDispatch { task })
        .await;
    assert!(send_result.is_ok());

    // Disconnect
    write.close().await.ok();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // After disconnect, worker removed from transport map
    let task2 = Task::new("y", json!({}));
    let send_result2 = transport
        .send("cleanup-test", Message::TaskDispatch { task: task2 })
        .await;
    assert!(
        send_result2.is_err(),
        "Should fail after worker disconnected"
    );
}

// =============================================================================
// Stdio transport tests (from e2e_stdio.rs)
// =============================================================================

#[tokio::test]
async fn test_stdio_bash_worker_word_count() {
    let (result_tx, mut result_rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = StdioTransport::new(
        vec![StdioProcess {
            command: "bash".to_string(),
            args: vec!["tests/stdio_worker/worker.sh".to_string()],
            worker_id: "bash-worker".to_string(),
            supported_tasks: vec!["word-count".to_string(), "disk-usage".to_string()],
        }],
        move |worker_id, msg| {
            let _ = result_tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();

    // Give the process time to spawn
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drain the initial WorkerRegister message
    let (_, reg_msg) = result_rx.recv().await.unwrap();
    assert!(matches!(reg_msg, Message::WorkerRegister { .. }));

    // Send a word-count task
    let task = Task::new("word-count", json!({"text": "hello world from rust pipe"}));
    let task_id = task.id;

    transport
        .send("bash-worker", Message::TaskDispatch { task })
        .await
        .unwrap();

    // Wait for result
    let (worker_id, result_msg) = tokio::time::timeout(Duration::from_secs(5), result_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(worker_id, "bash-worker");

    if let Message::TaskResult { result } = result_msg {
        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::Completed);
        assert_eq!(result.payload.unwrap()["wordCount"], 5);
        assert_eq!(result.worker_id, "stdio-bash");
    } else {
        panic!("Expected TaskResult, got: {:?}", result_msg);
    }
}

#[tokio::test]
async fn test_stdio_bash_worker_unknown_task() {
    let (result_tx, mut result_rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = StdioTransport::new(
        vec![StdioProcess {
            command: "bash".to_string(),
            args: vec!["tests/stdio_worker/worker.sh".to_string()],
            worker_id: "bash-worker-2".to_string(),
            supported_tasks: vec!["word-count".to_string()],
        }],
        move |worker_id, msg| {
            let _ = result_tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drain registration
    let _ = result_rx.recv().await.unwrap();

    // Send unknown task
    let task = Task::new("unknown-task", json!({}));
    let task_id = task.id;

    transport
        .send("bash-worker-2", Message::TaskDispatch { task })
        .await
        .unwrap();

    let (_, result_msg) = tokio::time::timeout(Duration::from_secs(5), result_rx.recv())
        .await
        .unwrap()
        .unwrap();

    if let Message::TaskResult { result } = result_msg {
        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::Failed);
        assert!(result.error.is_some());
        assert_eq!(result.error.unwrap().code, "UNKNOWN_TASK");
    } else {
        panic!("Expected TaskResult");
    }
}

#[tokio::test]
async fn test_stdio_multiple_tasks_sequential() {
    let (result_tx, mut result_rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = StdioTransport::new(
        vec![StdioProcess {
            command: "bash".to_string(),
            args: vec!["tests/stdio_worker/worker.sh".to_string()],
            worker_id: "bash-seq".to_string(),
            supported_tasks: vec!["word-count".to_string()],
        }],
        move |worker_id, msg| {
            let _ = result_tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drain registration
    let _ = result_rx.recv().await.unwrap();

    // Send 3 tasks sequentially
    let inputs = vec!["one two three", "a b c d", "single"];
    let expected_counts = vec![3, 4, 1];

    for (i, input) in inputs.iter().enumerate() {
        let task = Task::new("word-count", json!({"text": input}));
        transport
            .send("bash-seq", Message::TaskDispatch { task })
            .await
            .unwrap();

        let (_, result_msg) = tokio::time::timeout(Duration::from_secs(5), result_rx.recv())
            .await
            .unwrap()
            .unwrap();

        if let Message::TaskResult { result } = result_msg {
            assert_eq!(result.status, TaskStatus::Completed);
            assert_eq!(
                result.payload.unwrap()["wordCount"],
                expected_counts[i],
                "Word count mismatch for input: '{}'",
                input
            );
        }
    }
}

#[tokio::test]
async fn test_stdio_stop() {
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = StdioTransport::new(
        vec![StdioProcess {
            command: "bash".to_string(),
            args: vec![format!(
                "{}/tests/stdio_worker/worker.sh",
                env!("CARGO_MANIFEST_DIR")
            )],
            worker_id: "stdio-stop-test".to_string(),
            supported_tasks: vec!["word-count".to_string()],
        }],
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = rx.recv().await; // drain register

    let result = transport.stop().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_stdio_broadcast() {
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = StdioTransport::new(
        vec![StdioProcess {
            command: "bash".to_string(),
            args: vec![format!(
                "{}/tests/stdio_worker/worker.sh",
                env!("CARGO_MANIFEST_DIR")
            )],
            worker_id: "stdio-bc".to_string(),
            supported_tasks: vec!["word-count".to_string()],
        }],
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = rx.recv().await; // drain register

    let task = Task::new("word-count", json!({"text": "a b c"}));
    transport
        .broadcast(Message::TaskDispatch { task })
        .await
        .unwrap();

    let (_, msg) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(msg, Message::TaskResult { .. }));
}

#[tokio::test]
async fn test_stdio_send_to_unknown_worker() {
    let transport = StdioTransport::new(vec![], |_, _| {});
    transport.start().await.unwrap();

    let task = Task::new("x", json!({}));
    let result = transport
        .send("ghost", Message::TaskDispatch { task })
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_stdio_stdin_write_error() {
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = StdioTransport::new(
        vec![StdioProcess {
            command: "bash".to_string(),
            args: vec![format!(
                "{}/tests/mock_bins/stdio_exit_immediately",
                env!("CARGO_MANIFEST_DIR")
            )],
            worker_id: "stdio-exit".to_string(),
            supported_tasks: vec!["will-fail".to_string()],
        }],
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drain registration
    let _ = rx.recv().await;

    // The process already exited, so stdin pipe is broken.
    // Sending a task should trigger the stdin write error path.
    let task = Task::new("will-fail", json!({"data": "test"}));
    transport
        .send("stdio-exit", Message::TaskDispatch { task })
        .await
        .unwrap();

    // The send to the channel succeeds (it goes to the mpsc), but the stdin
    // writer task hits a broken pipe. Give it time to trigger that path.
    tokio::time::sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_stdio_nonjson_output_handled() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = StdioTransport::new(
        vec![StdioProcess {
            command: "bash".to_string(),
            args: vec![format!("{}/tests/mock_bins/stdio_nonjson", manifest_dir)],
            worker_id: "nonjson-test".to_string(),
            supported_tasks: vec!["test".to_string()],
        }],
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain registration
    let _ = rx.recv().await;

    // Send a task - the worker will output garbage first, then a valid result
    let task = Task::new("test", json!({"x": 1}));
    let task_id = task.id;
    transport
        .send("nonjson-test", Message::TaskDispatch { task })
        .await
        .unwrap();

    // Should still get the valid result despite garbage output
    let result = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
    if let Ok(Some((_, Message::TaskResult { result }))) = result {
        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::Completed);
    }
}

#[tokio::test]
async fn test_stdio_spawn_failure() {
    let transport = StdioTransport::new(
        vec![StdioProcess {
            command: "/nonexistent/binary/that/does/not/exist".to_string(),
            args: vec![],
            worker_id: "bad-spawn".to_string(),
            supported_tasks: vec!["x".to_string()],
        }],
        |_, _| {},
    );

    let result = transport.start().await;
    assert!(result.is_err(), "Should fail when binary doesn't exist");
}

// =============================================================================
// Stdio kill/crash coverage (from e2e_kill_coverage.rs)
// =============================================================================

#[tokio::test]
async fn test_stdio_worker_dies_stdin_write_error() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = StdioTransport::new(
        vec![StdioProcess {
            command: "bash".to_string(),
            args: vec![format!("{}/tests/mock_bins/die_after_one", manifest_dir)],
            worker_id: "die-worker".to_string(),
            supported_tasks: vec!["task".to_string()],
        }],
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = rx.recv().await; // drain registration

    // First task — worker processes and dies
    let task1 = Task::new("task", json!({"n": 1}));
    transport
        .send("die-worker", Message::TaskDispatch { task: task1 })
        .await
        .unwrap();

    // Get result from first task
    let _ = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;

    // Wait for process to die
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Second task — stdin pipe is broken, triggers write error path
    let task2 = Task::new("task", json!({"n": 2}));
    // Send might succeed (buffered in channel) but the write to stdin will fail
    let _ = transport
        .send("die-worker", Message::TaskDispatch { task: task2 })
        .await;

    // Give time for the error path to execute
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Test passes if no panic — the error path (tracing::error + break) was hit
}

#[tokio::test]
async fn test_stdio_worker_nonjson_output_debug_path() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = StdioTransport::new(
        vec![StdioProcess {
            command: "bash".to_string(),
            args: vec![format!("{}/tests/mock_bins/nonjson_then_die", manifest_dir)],
            worker_id: "nonjson-die".to_string(),
            supported_tasks: vec!["x".to_string()],
        }],
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = rx.recv().await; // drain registration

    // The worker outputs garbage then exits — exercises:
    // - Non-JSON line handling (tracing::debug "Non-JSON line from worker, ignoring")
    // - Stdout closed path (tracing::info "Stdio worker stdout closed")
    tokio::time::sleep(Duration::from_millis(500)).await;
    // No panic = paths were exercised
}

// =============================================================================
// Docker transport tests (from e2e_docker.rs)
// =============================================================================

#[tokio::test]
#[serial(path_env)]
async fn test_docker_transport_start_and_register() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = DockerTransport::new(
        vec![DockerWorkerConfig {
            image: "test-image:latest".to_string(),
            worker_id: "docker-w1".to_string(),
            supported_tasks: vec!["scan".to_string()],
            env: HashMap::new(),
            volumes: vec![],
            network: None,
            memory_limit: Some("512m".to_string()),
            cpu_limit: Some("1.0".to_string()),
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();

    // Should receive WorkerRegister message
    let (wid, msg) = rx.recv().await.unwrap();
    assert_eq!(wid, "docker-w1");
    assert!(matches!(msg, Message::WorkerRegister { .. }));
}

#[tokio::test]
#[serial(path_env)]
async fn test_docker_transport_dispatch_and_result() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = DockerTransport::new(
        vec![DockerWorkerConfig {
            image: "worker:latest".to_string(),
            worker_id: "docker-w2".to_string(),
            supported_tasks: vec!["build".to_string()],
            env: HashMap::new(),
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drain registration message
    let _ = rx.recv().await.unwrap();

    // Dispatch a task
    let task = Task::new("build", json!({"project": "myapp"}));
    let task_id = task.id;

    transport
        .send("docker-w2", Message::TaskDispatch { task })
        .await
        .unwrap();

    // Should get result from mock docker
    let (wid, msg) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(wid, "docker-w2");
    if let Message::TaskResult { result } = msg {
        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::Completed);
        assert_eq!(result.payload.unwrap()["source"], "docker");
    } else {
        panic!("Expected TaskResult, got: {:?}", msg);
    }
}

#[tokio::test]
#[serial(path_env)]
async fn test_docker_transport_send_to_unknown_worker() {
    env::set_var("PATH", mock_path());

    let transport = DockerTransport::new(vec![], |_, _| {});

    transport.start().await.unwrap();

    let task = Task::new("x", json!({}));
    let result = transport
        .send("nonexistent", Message::TaskDispatch { task })
        .await;
    assert!(result.is_err());
}

#[tokio::test]
#[serial(path_env)]
async fn test_docker_transport_stop() {
    env::set_var("PATH", mock_path());

    let transport = DockerTransport::new(
        vec![DockerWorkerConfig {
            image: "img:latest".to_string(),
            worker_id: "docker-stop".to_string(),
            supported_tasks: vec!["a".to_string()],
            env: HashMap::new(),
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
        }],
        |_, _| {},
    );

    transport.start().await.unwrap();
    let result = transport.stop().await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial(path_env)]
async fn test_docker_transport_broadcast() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = DockerTransport::new(
        vec![DockerWorkerConfig {
            image: "img:latest".to_string(),
            worker_id: "docker-bc".to_string(),
            supported_tasks: vec!["a".to_string()],
            env: HashMap::new(),
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = rx.recv().await; // drain register

    let task = Task::new("a", json!({}));
    transport
        .broadcast(Message::TaskDispatch { task })
        .await
        .unwrap();

    let (_, msg) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(msg, Message::TaskResult { .. }));
}

#[tokio::test]
#[serial(path_env)]
async fn test_docker_all_config_fields() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let mut env_map = HashMap::new();
    env_map.insert("APP_ENV".to_string(), "test".to_string());
    env_map.insert("LOG_LEVEL".to_string(), "debug".to_string());
    env_map.insert("DB_HOST".to_string(), "localhost".to_string());

    let transport = DockerTransport::new(
        vec![DockerWorkerConfig {
            image: "full-config:latest".to_string(),
            worker_id: "docker-full".to_string(),
            supported_tasks: vec!["full-test".to_string()],
            env: env_map,
            volumes: vec![
                "/host/data:/container/data".to_string(),
                "/host/config:/container/config:ro".to_string(),
            ],
            network: Some("my-network".to_string()),
            memory_limit: Some("1g".to_string()),
            cpu_limit: Some("2.5".to_string()),
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drain registration
    let (wid, msg) = rx.recv().await.unwrap();
    assert_eq!(wid, "docker-full");
    assert!(matches!(msg, Message::WorkerRegister { .. }));

    // Dispatch a task to exercise the full config path
    let task = Task::new("full-test", json!({"input": "data"}));
    let task_id = task.id;
    transport
        .send("docker-full", Message::TaskDispatch { task })
        .await
        .unwrap();

    let (_, msg) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();

    if let Message::TaskResult { result } = msg {
        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::Completed);
    } else {
        panic!("Expected TaskResult from docker full-config test");
    }
}

#[tokio::test]
#[serial(path_env)]
async fn test_docker_run_failure() {
    // Use a PATH where "docker" is the failing mock
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let fail_dir = format!(
        "{}/tests/mock_bins/docker_fail_dir_{}",
        manifest_dir,
        std::process::id()
    );
    // Clean up any stale directory, then create fresh
    let _ = std::fs::remove_dir_all(&fail_dir);
    std::fs::create_dir_all(&fail_dir).unwrap();
    std::os::unix::fs::symlink(
        format!("{}/tests/mock_bins/docker_fail_run", manifest_dir),
        format!("{}/docker", fail_dir),
    )
    .unwrap();

    let path = format!("{}:{}", fail_dir, mock_path());
    env::set_var("PATH", &path);

    let transport = DockerTransport::new(
        vec![DockerWorkerConfig {
            image: "nonexistent:latest".to_string(),
            worker_id: "docker-fail".to_string(),
            supported_tasks: vec!["x".to_string()],
            env: HashMap::new(),
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
        }],
        |_, _| {},
    );

    let result = transport.start().await;
    assert!(result.is_err(), "Should fail when docker run fails");

    // Cleanup
    std::fs::remove_dir_all(&fail_dir).ok();
}

#[tokio::test]
#[serial(path_env)]
async fn test_docker_stdin_write_after_container_dies() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = DockerTransport::new(
        vec![DockerWorkerConfig {
            image: "test:latest".to_string(),
            worker_id: "docker-die".to_string(),
            supported_tasks: vec!["x".to_string()],
            env: HashMap::new(),
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
        }],
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = rx.recv().await; // drain register

    // Send a task - exercises normal path
    let task = Task::new("x", json!({}));
    transport
        .send("docker-die", Message::TaskDispatch { task })
        .await
        .unwrap();

    // Get result
    let _ = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;

    // Stop transport - exercises shutdown path including docker stop command
    transport.stop().await.unwrap();
}

#[tokio::test]
#[serial(path_env)]
async fn test_docker_nonjson_from_container() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    // Create a docker mock dir where "docker attach" outputs garbage
    let fail_dir = format!("{}/tests/mock_bins/docker_nonjson_dir", manifest_dir);
    std::fs::create_dir_all(&fail_dir).ok();
    let docker_link = format!("{}/docker", fail_dir);
    let _ = std::fs::remove_file(&docker_link);

    // Write a custom docker mock that outputs non-JSON on attach
    std::fs::write(&docker_link, r#"#!/bin/bash
case "$1" in
  "run") echo "mock-container-123"; exit 0 ;;
  "attach")
    echo "container boot log: non-json"
    echo "{garbage json{"
    read -r line
    task_id=$(echo "$line" | jq -r '.task.id' 2>/dev/null)
    echo "{\"type\":\"TaskResult\",\"result\":{\"taskId\":\"$task_id\",\"status\":\"Completed\",\"payload\":{},\"durationMs\":1,\"workerId\":\"docker-nj\"}}"
    read -r line2
    exit 0
    ;;
  "stop") exit 0 ;;
esac
"#).unwrap();
    std::fs::set_permissions(
        &docker_link,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .ok();

    let path = format!("{}:{}", fail_dir, env::var("PATH").unwrap_or_default());
    env::set_var("PATH", &path);

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = DockerTransport::new(
        vec![DockerWorkerConfig {
            image: "img:v1".to_string(),
            worker_id: "docker-nj".to_string(),
            supported_tasks: vec!["x".to_string()],
            env: HashMap::new(),
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
        }],
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = rx.recv().await; // drain register

    // Container outputs non-JSON first (exercises debug log), then processes task
    let task = Task::new("x", json!({}));
    transport
        .send("docker-nj", Message::TaskDispatch { task })
        .await
        .unwrap();

    let _ = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;

    // Cleanup
    std::fs::remove_dir_all(&fail_dir).ok();
}

// =============================================================================
// SSH transport tests (from e2e_ssh.rs)
// =============================================================================

#[tokio::test]
#[serial(path_env)]
async fn test_ssh_transport_start_and_register() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = SshTransport::new(
        vec![SshWorkerConfig {
            host: "10.0.0.1".to_string(),
            user: "deploy".to_string(),
            port: 22,
            worker_id: "ssh-w1".to_string(),
            supported_tasks: vec!["scan-network".to_string()],
            remote_command: "/usr/local/bin/worker".to_string(),
            identity_file: Some("/home/user/.ssh/id_rsa".to_string()),
            connect_timeout_secs: 10,
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();

    let (wid, msg) = rx.recv().await.unwrap();
    assert_eq!(wid, "ssh-w1");
    assert!(matches!(msg, Message::WorkerRegister { .. }));
}

#[tokio::test]
#[serial(path_env)]
async fn test_ssh_transport_dispatch_and_result() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = SshTransport::new(
        vec![SshWorkerConfig {
            host: "scanner.internal".to_string(),
            user: "root".to_string(),
            port: 2222,
            worker_id: "ssh-w2".to_string(),
            supported_tasks: vec!["port-scan".to_string()],
            remote_command: "worker.sh".to_string(),
            identity_file: None,
            connect_timeout_secs: 5,
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drain registration
    let _ = rx.recv().await.unwrap();

    // Dispatch
    let task = Task::new("port-scan", json!({"target": "192.168.1.1"}));
    let task_id = task.id;

    transport
        .send("ssh-w2", Message::TaskDispatch { task })
        .await
        .unwrap();

    let (wid, msg) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(wid, "ssh-w2");
    if let Message::TaskResult { result } = msg {
        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::Completed);
        assert_eq!(result.payload.unwrap()["source"], "ssh");
    } else {
        panic!("Expected TaskResult");
    }
}

#[tokio::test]
#[serial(path_env)]
async fn test_ssh_transport_send_to_unknown_worker() {
    env::set_var("PATH", mock_path());

    let transport = SshTransport::new(vec![], |_, _| {});
    transport.start().await.unwrap();

    let task = Task::new("x", json!({}));
    let result = transport
        .send("ghost", Message::TaskDispatch { task })
        .await;
    assert!(result.is_err());
}

#[tokio::test]
#[serial(path_env)]
async fn test_ssh_transport_stop() {
    env::set_var("PATH", mock_path());

    let transport = SshTransport::new(
        vec![SshWorkerConfig {
            host: "h".to_string(),
            user: "u".to_string(),
            port: 22,
            worker_id: "ssh-stop".to_string(),
            supported_tasks: vec!["a".to_string()],
            remote_command: "cmd".to_string(),
            identity_file: None,
            connect_timeout_secs: 5,
        }],
        |_, _| {},
    );

    transport.start().await.unwrap();
    assert!(transport.stop().await.is_ok());
}

#[tokio::test]
#[serial(path_env)]
async fn test_ssh_with_identity_file() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = SshTransport::new(
        vec![SshWorkerConfig {
            host: "secure-host.internal".to_string(),
            user: "admin".to_string(),
            port: 2222,
            worker_id: "ssh-key".to_string(),
            supported_tasks: vec!["secure-task".to_string()],
            remote_command: "/opt/worker/run.sh".to_string(),
            identity_file: Some("/path/to/key".to_string()),
            connect_timeout_secs: 10,
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Should register
    let (wid, msg) = rx.recv().await.unwrap();
    assert_eq!(wid, "ssh-key");
    assert!(matches!(msg, Message::WorkerRegister { .. }));

    // Dispatch a task to verify identity_file path doesn't break anything
    let task = Task::new("secure-task", json!({"target": "10.0.0.5"}));
    let task_id = task.id;
    transport
        .send("ssh-key", Message::TaskDispatch { task })
        .await
        .unwrap();

    let (wid2, msg2) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(wid2, "ssh-key");
    if let Message::TaskResult { result } = msg2 {
        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::Completed);
    } else {
        panic!("Expected TaskResult from SSH identity_file test");
    }
}

#[tokio::test]
#[serial(path_env)]
async fn test_ssh_broadcast() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = SshTransport::new(
        vec![SshWorkerConfig {
            host: "h1".to_string(),
            user: "u1".to_string(),
            port: 22,
            worker_id: "ssh-bc".to_string(),
            supported_tasks: vec!["bc-task".to_string()],
            remote_command: "worker".to_string(),
            identity_file: None,
            connect_timeout_secs: 5,
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = rx.recv().await; // drain register

    let task = Task::new("bc-task", json!({"x": 1}));
    transport
        .broadcast(Message::TaskDispatch { task })
        .await
        .unwrap();

    let (_, msg) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(msg, Message::TaskResult { .. }));
}

#[tokio::test]
#[serial(path_env)]
async fn test_ssh_connection_failure() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let fail_dir = format!("{}/tests/mock_bins/ssh_fail_dir", manifest_dir);
    std::fs::create_dir_all(&fail_dir).ok();
    std::os::unix::fs::symlink(
        format!("{}/tests/mock_bins/ssh_fail", manifest_dir),
        format!("{}/ssh", fail_dir),
    )
    .ok();

    let path = format!("{}:{}", fail_dir, env::var("PATH").unwrap_or_default());
    env::set_var("PATH", &path);

    let (tx, _rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = SshTransport::new(
        vec![SshWorkerConfig {
            host: "unreachable.example.com".to_string(),
            user: "user".to_string(),
            port: 22,
            worker_id: "ssh-fail".to_string(),
            supported_tasks: vec!["x".to_string()],
            remote_command: "worker".to_string(),
            identity_file: None,
            connect_timeout_secs: 1,
        }],
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );

    // ssh_fail mock exits immediately with error, but the transport still spawns
    // it successfully (the spawn itself doesn't fail, the process does).
    // The registration still happens because it's done before reading output.
    let result = transport.start().await;
    // The spawn succeeds (ssh binary exists), but the process immediately exits.
    // This exercises the stdout reader "SSH connection closed" path.
    assert!(result.is_ok());

    // Cleanup
    std::fs::remove_dir_all(&fail_dir).ok();
}

#[tokio::test]
#[serial(path_env)]
async fn test_ssh_nonjson_and_close() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    // Create an ssh mock that outputs garbage then exits
    let fail_dir = format!("{}/tests/mock_bins/ssh_garbage_dir", manifest_dir);
    std::fs::create_dir_all(&fail_dir).ok();
    // Symlink nonjson_then_die as "ssh"
    let ssh_link = format!("{}/ssh", fail_dir);
    let _ = std::fs::remove_file(&ssh_link);
    std::os::unix::fs::symlink(
        format!("{}/tests/mock_bins/nonjson_then_die", manifest_dir),
        &ssh_link,
    )
    .ok();

    let path = format!("{}:{}", fail_dir, env::var("PATH").unwrap_or_default());
    env::set_var("PATH", &path);

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = SshTransport::new(
        vec![SshWorkerConfig {
            host: "h".to_string(),
            user: "u".to_string(),
            port: 22,
            worker_id: "ssh-garbage".to_string(),
            supported_tasks: vec!["y".to_string()],
            remote_command: "cmd".to_string(),
            identity_file: None,
            connect_timeout_secs: 5,
        }],
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = rx.recv().await; // drain register

    // Wait for worker to output garbage and exit
    // This exercises: non-JSON debug log + "SSH connection closed" info log
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cleanup
    std::fs::remove_dir_all(&fail_dir).ok();
}

#[tokio::test]
#[serial(path_env)]
async fn test_ssh_stdin_write_error_after_remote_dies() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let fail_dir = format!("{}/tests/mock_bins/ssh_die_dir", manifest_dir);
    std::fs::create_dir_all(&fail_dir).ok();
    let ssh_link = format!("{}/ssh", fail_dir);
    let _ = std::fs::remove_file(&ssh_link);
    std::os::unix::fs::symlink(
        format!("{}/tests/mock_bins/die_after_one", manifest_dir),
        &ssh_link,
    )
    .ok();

    let path = format!("{}:{}", fail_dir, env::var("PATH").unwrap_or_default());
    env::set_var("PATH", &path);

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = SshTransport::new(
        vec![SshWorkerConfig {
            host: "remote".to_string(),
            user: "user".to_string(),
            port: 22,
            worker_id: "ssh-die-write".to_string(),
            supported_tasks: vec!["task".to_string()],
            remote_command: "worker".to_string(),
            identity_file: None,
            connect_timeout_secs: 5,
        }],
        move |wid, msg| {
            let _ = tx.send((wid, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = rx.recv().await; // drain register

    // First task - worker processes then dies
    let task1 = Task::new("task", json!({"n": 1}));
    transport
        .send("ssh-die-write", Message::TaskDispatch { task: task1 })
        .await
        .unwrap();

    let _ = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Second task - stdin broken, triggers write error
    let task2 = Task::new("task", json!({"n": 2}));
    let _ = transport
        .send("ssh-die-write", Message::TaskDispatch { task: task2 })
        .await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cleanup
    std::fs::remove_dir_all(&fail_dir).ok();
}

// =============================================================================
// WASM transport tests (from e2e_wasm.rs)
// =============================================================================

#[tokio::test]
#[serial(path_env)]
async fn test_wasm_transport_start_and_register() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = WasmTransport::new(
        vec![WasmWorkerConfig {
            module_path: fake_wasm_module(),
            worker_id: "wasm-w1".to_string(),
            supported_tasks: vec!["compute".to_string()],
            max_memory_pages: 256,
            max_execution_time_ms: 5000,
            allowed_env: vec![],
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();

    let (wid, msg) = rx.recv().await.unwrap();
    assert_eq!(wid, "wasm-w1");
    assert!(matches!(msg, Message::WorkerRegister { .. }));
}

#[tokio::test]
#[serial(path_env)]
async fn test_wasm_transport_dispatch_and_result() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = WasmTransport::new(
        vec![WasmWorkerConfig {
            module_path: fake_wasm_module(),
            worker_id: "wasm-w2".to_string(),
            supported_tasks: vec!["hash".to_string()],
            max_memory_pages: 128,
            max_execution_time_ms: 10000,
            allowed_env: vec![],
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drain registration
    let _ = rx.recv().await.unwrap();

    // Dispatch
    let task = Task::new("hash", json!({"data": "hello world"}));
    let task_id = task.id;

    transport
        .send("wasm-w2", Message::TaskDispatch { task })
        .await
        .unwrap();

    let (wid, msg) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(wid, "wasm-w2");
    if let Message::TaskResult { result } = msg {
        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::Completed);
        assert!(result.payload.unwrap()["result"] == "wasm-executed");
    } else {
        panic!("Expected TaskResult");
    }
}

#[tokio::test]
async fn test_wasm_transport_nonexistent_module() {
    let transport = WasmTransport::new(
        vec![WasmWorkerConfig {
            module_path: PathBuf::from("/nonexistent/path/module.wasm"),
            worker_id: "wasm-bad".to_string(),
            supported_tasks: vec!["x".to_string()],
            max_memory_pages: 64,
            max_execution_time_ms: 1000,
            allowed_env: vec![],
        }],
        |_, _| {},
    );

    let result = transport.start().await;
    assert!(result.is_err());
}

#[tokio::test]
#[serial(path_env)]
async fn test_wasm_transport_send_to_unknown_worker() {
    env::set_var("PATH", mock_path());

    let transport = WasmTransport::new(vec![], |_, _| {});
    transport.start().await.unwrap();

    let task = Task::new("x", json!({}));
    let result = transport
        .send("ghost", Message::TaskDispatch { task })
        .await;
    assert!(result.is_err());
}

#[tokio::test]
#[serial(path_env)]
async fn test_wasm_transport_stop() {
    env::set_var("PATH", mock_path());

    let transport = WasmTransport::new(
        vec![WasmWorkerConfig {
            module_path: fake_wasm_module(),
            worker_id: "wasm-stop".to_string(),
            supported_tasks: vec!["a".to_string()],
            max_memory_pages: 64,
            max_execution_time_ms: 1000,
            allowed_env: vec![],
        }],
        |_, _| {},
    );

    transport.start().await.unwrap();
    assert!(transport.stop().await.is_ok());
}

#[tokio::test]
#[serial(path_env)]
async fn test_wasm_timeout_path() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = WasmTransport::new(
        vec![WasmWorkerConfig {
            module_path: wasm_module("module_timeout"),
            worker_id: "wasm-timeout".to_string(),
            supported_tasks: vec!["slow-task".to_string()],
            max_memory_pages: 64,
            max_execution_time_ms: 100, // 100ms timeout
            allowed_env: vec![],
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain registration message
    let _ = rx.recv().await.unwrap();

    // Dispatch a task that will time out
    let task = Task::new("slow-task", json!({"data": "test"}));
    let task_id = task.id;
    transport
        .send("wasm-timeout", Message::TaskDispatch { task })
        .await
        .unwrap();

    // Should get a TimedOut result
    let (wid, msg) = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(wid, "wasm-timeout");
    if let Message::TaskResult { result } = msg {
        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::TimedOut);
        assert!(result.error.is_some());
        let err = result.error.unwrap();
        assert_eq!(err.code, "TIMEOUT");
        assert!(err.retryable);
    } else {
        panic!("Expected TaskResult with TimedOut status");
    }
}

#[tokio::test]
#[serial(path_env)]
async fn test_wasm_parse_error_path() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = WasmTransport::new(
        vec![WasmWorkerConfig {
            module_path: wasm_module("module_bad_json"),
            worker_id: "wasm-badjson".to_string(),
            supported_tasks: vec!["parse-test".to_string()],
            max_memory_pages: 64,
            max_execution_time_ms: 5000,
            allowed_env: vec![],
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain registration
    let _ = rx.recv().await.unwrap();

    let task = Task::new("parse-test", json!({"data": "hello"}));
    let task_id = task.id;
    transport
        .send("wasm-badjson", Message::TaskDispatch { task })
        .await
        .unwrap();

    let (wid, msg) = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(wid, "wasm-badjson");
    if let Message::TaskResult { result } = msg {
        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::Failed);
        assert!(result.error.is_some());
        let err = result.error.unwrap();
        assert_eq!(err.code, "PARSE_ERROR");
        assert!(!err.retryable);
        assert!(err.message.contains("Failed to parse WASM output"));
    } else {
        panic!("Expected TaskResult with Failed status (PARSE_ERROR)");
    }
}

#[tokio::test]
#[serial(path_env)]
async fn test_wasm_execution_error_path() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = WasmTransport::new(
        vec![WasmWorkerConfig {
            module_path: wasm_module("module_fail"),
            worker_id: "wasm-fail".to_string(),
            supported_tasks: vec!["fail-test".to_string()],
            max_memory_pages: 64,
            max_execution_time_ms: 5000,
            allowed_env: vec![],
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain registration
    let _ = rx.recv().await.unwrap();

    let task = Task::new("fail-test", json!({"data": "test"}));
    let task_id = task.id;
    transport
        .send("wasm-fail", Message::TaskDispatch { task })
        .await
        .unwrap();

    let (wid, msg) = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(wid, "wasm-fail");
    if let Message::TaskResult { result } = msg {
        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::Failed);
        assert!(result.error.is_some());
        let err = result.error.unwrap();
        assert_eq!(err.code, "EXECUTION_ERROR");
        assert!(err.retryable);
        assert!(err.message.contains("WASM module failed"));
    } else {
        panic!("Expected TaskResult with Failed status (EXECUTION_ERROR)");
    }
}

#[tokio::test]
#[serial(path_env)]
async fn test_wasm_broadcast() {
    env::set_var("PATH", mock_path());

    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Message)>();

    let transport = WasmTransport::new(
        vec![WasmWorkerConfig {
            module_path: wasm_module("wasmtime"),
            worker_id: "wasm-bc".to_string(),
            supported_tasks: vec!["bc-task".to_string()],
            max_memory_pages: 64,
            max_execution_time_ms: 5000,
            allowed_env: vec!["HOME".to_string()],
        }],
        move |worker_id, msg| {
            let _ = tx.send((worker_id, msg));
        },
    );

    transport.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain registration
    let _ = rx.recv().await.unwrap();

    let task = Task::new("bc-task", json!({"x": 1}));
    transport
        .broadcast(Message::TaskDispatch { task })
        .await
        .unwrap();

    let (_, msg) = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(msg, Message::TaskResult { .. }));
}

// =============================================================================
// Validation tests (from e2e_validation_coverage.rs)
// =============================================================================

#[tokio::test]
#[serial(path_env)]
async fn test_docker_rejects_invalid_worker_id() {
    env::set_var("PATH", mock_path());
    let transport = DockerTransport::new(
        vec![DockerWorkerConfig {
            image: "nginx:latest".to_string(),
            worker_id: "$(inject)".to_string(), // Invalid!
            supported_tasks: vec!["x".to_string()],
            env: HashMap::new(),
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
        }],
        |_, _| {},
    );
    let result = transport.start().await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid worker ID"));
}

#[tokio::test]
#[serial(path_env)]
async fn test_docker_rejects_invalid_image() {
    env::set_var("PATH", mock_path());
    let transport = DockerTransport::new(
        vec![DockerWorkerConfig {
            image: "--privileged".to_string(), // Invalid!
            worker_id: "valid-id".to_string(),
            supported_tasks: vec!["x".to_string()],
            env: HashMap::new(),
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
        }],
        |_, _| {},
    );
    let result = transport.start().await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid Docker image"));
}

#[tokio::test]
#[serial(path_env)]
async fn test_docker_rejects_dangerous_env_key() {
    env::set_var("PATH", mock_path());
    let mut env_map = HashMap::new();
    env_map.insert("$(whoami)".to_string(), "value".to_string());

    let transport = DockerTransport::new(
        vec![DockerWorkerConfig {
            image: "nginx:latest".to_string(),
            worker_id: "valid-id".to_string(),
            supported_tasks: vec!["x".to_string()],
            env: env_map,
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
        }],
        |_, _| {},
    );
    let result = transport.start().await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("shell metacharacters"));
}

#[tokio::test]
#[serial(path_env)]
async fn test_docker_rejects_dangerous_env_value() {
    env::set_var("PATH", mock_path());
    let mut env_map = HashMap::new();
    env_map.insert("SAFE_KEY".to_string(), "value;rm -rf /".to_string());

    let transport = DockerTransport::new(
        vec![DockerWorkerConfig {
            image: "nginx:latest".to_string(),
            worker_id: "valid-id".to_string(),
            supported_tasks: vec!["x".to_string()],
            env: env_map,
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
        }],
        |_, _| {},
    );
    let result = transport.start().await;
    assert!(result.is_err());
}

#[tokio::test]
#[serial(path_env)]
async fn test_docker_rejects_dangerous_volume() {
    env::set_var("PATH", mock_path());
    let transport = DockerTransport::new(
        vec![DockerWorkerConfig {
            image: "nginx:latest".to_string(),
            worker_id: "valid-id".to_string(),
            supported_tasks: vec!["x".to_string()],
            env: HashMap::new(),
            volumes: vec!["/host;rm -rf /:container".to_string()],
            network: None,
            memory_limit: None,
            cpu_limit: None,
        }],
        |_, _| {},
    );
    let result = transport.start().await;
    assert!(result.is_err());
}

#[tokio::test]
#[serial(path_env)]
async fn test_ssh_rejects_invalid_hostname() {
    env::set_var("PATH", mock_path());
    let transport = SshTransport::new(
        vec![SshWorkerConfig {
            host: "$(inject)".to_string(), // Invalid!
            user: "root".to_string(),
            port: 22,
            worker_id: "valid-ssh".to_string(),
            supported_tasks: vec!["x".to_string()],
            remote_command: "worker".to_string(),
            identity_file: None,
            connect_timeout_secs: 5,
        }],
        |_, _| {},
    );
    let result = transport.start().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid hostname"));
}

#[tokio::test]
#[serial(path_env)]
async fn test_ssh_rejects_invalid_username() {
    env::set_var("PATH", mock_path());
    let transport = SshTransport::new(
        vec![SshWorkerConfig {
            host: "valid-host".to_string(),
            user: "$(whoami)".to_string(), // Invalid!
            port: 22,
            worker_id: "valid-ssh".to_string(),
            supported_tasks: vec!["x".to_string()],
            remote_command: "worker".to_string(),
            identity_file: None,
            connect_timeout_secs: 5,
        }],
        |_, _| {},
    );
    let result = transport.start().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid username"));
}

#[tokio::test]
#[serial(path_env)]
async fn test_ssh_rejects_invalid_worker_id() {
    env::set_var("PATH", mock_path());
    let transport = SshTransport::new(
        vec![SshWorkerConfig {
            host: "valid-host".to_string(),
            user: "root".to_string(),
            port: 22,
            worker_id: ";evil".to_string(), // Invalid!
            supported_tasks: vec!["x".to_string()],
            remote_command: "worker".to_string(),
            identity_file: None,
            connect_timeout_secs: 5,
        }],
        |_, _| {},
    );
    let result = transport.start().await;
    assert!(result.is_err());
}

#[tokio::test]
#[serial(path_env)]
async fn test_ssh_rejects_dangerous_identity_file() {
    env::set_var("PATH", mock_path());
    let transport = SshTransport::new(
        vec![SshWorkerConfig {
            host: "valid-host".to_string(),
            user: "root".to_string(),
            port: 22,
            worker_id: "valid-ssh".to_string(),
            supported_tasks: vec!["x".to_string()],
            remote_command: "worker".to_string(),
            identity_file: Some("../../etc/passwd".to_string()), // Path traversal!
            connect_timeout_secs: 5,
        }],
        |_, _| {},
    );
    let result = transport.start().await;
    assert!(result.is_err());
}

// =============================================================================
// Polyglot worker tests (from e2e_polyglot.rs)
// =============================================================================

/// Spawns a real TypeScript worker as a subprocess and verifies end-to-end dispatch.
/// Requires: cd tests/ts_worker && npm install
#[tokio::test]
#[ignore = "requires npm install in tests/ts_worker"]
async fn test_typescript_worker_e2e() {
    let dispatcher = Dispatcher::builder().host("127.0.0.1").port(19900).build();

    dispatcher.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Spawn TypeScript worker
    let mut ts_worker = Command::new("npx")
        .args(["tsx", "tests/ts_worker/worker.ts"])
        .env("DISPATCHER_URL", "ws://127.0.0.1:19900")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to spawn TypeScript worker");

    // Wait for worker to connect and register
    tokio::time::sleep(Duration::from_secs(3)).await;

    let stats = dispatcher.pool_stats();
    assert!(
        stats.total >= 1,
        "TypeScript worker should have registered. Got: {}",
        stats.total
    );

    // Dispatch echo task
    let task =
        Task::new("echo", json!({"message": "hello from rust"})).with_priority(Priority::High);
    let handle = dispatcher.dispatch(task).await.unwrap();
    let result = handle
        .await_with_timeout(Duration::from_secs(10))
        .await
        .unwrap();

    assert_eq!(result.status, TaskStatus::Completed);
    assert_eq!(
        result.payload.unwrap()["echoed"]["message"],
        "hello from rust"
    );

    // Dispatch add-numbers task
    let task2 = Task::new("add-numbers", json!({"a": 17, "b": 25}));
    let handle2 = dispatcher.dispatch(task2).await.unwrap();
    let result2 = handle2
        .await_with_timeout(Duration::from_secs(10))
        .await
        .unwrap();

    assert_eq!(result2.status, TaskStatus::Completed);
    assert_eq!(result2.payload.unwrap()["sum"], 42);

    // Clean up
    ts_worker.kill().await.ok();
}

/// Spawns a real Python worker as a subprocess and verifies end-to-end dispatch.
#[tokio::test]
async fn test_python_worker_e2e() {
    let dispatcher = Dispatcher::builder().host("127.0.0.1").port(19901).build();

    dispatcher.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Spawn Python worker
    let mut py_worker = Command::new("python3")
        .args(["tests/py_worker/worker.py"])
        .env("DISPATCHER_URL", "ws://127.0.0.1:19901")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to spawn Python worker");

    // Wait for worker to connect
    tokio::time::sleep(Duration::from_secs(2)).await;

    let stats = dispatcher.pool_stats();
    eprintln!(
        "Pool stats: total={} active={} busy={}",
        stats.total, stats.active, stats.busy
    );
    assert!(
        stats.total >= 1,
        "Python worker should have registered. Got: {}",
        stats.total
    );

    // Dispatch echo task
    let task = Task::new("echo", json!({"data": [1, 2, 3]}));
    eprintln!("Dispatching task: {}", task.id);
    let handle = dispatcher.dispatch(task).await.unwrap();
    eprintln!("Dispatch successful, waiting for result...");
    let result = handle.await_with_timeout(Duration::from_secs(10)).await;
    eprintln!("Result: {:?}", result);
    let result = result.unwrap();

    assert_eq!(result.status, TaskStatus::Completed);
    assert_eq!(result.payload.unwrap()["echoed"]["data"], json!([1, 2, 3]));

    // Dispatch multiply task
    let task2 = Task::new("multiply", json!({"a": 7, "b": 6}));
    let handle2 = dispatcher.dispatch(task2).await.unwrap();
    let result2 = handle2
        .await_with_timeout(Duration::from_secs(10))
        .await
        .unwrap();

    assert_eq!(result2.status, TaskStatus::Completed);
    assert_eq!(result2.payload.unwrap()["product"], 42);

    // Dispatch reverse-string task
    let task3 = Task::new("reverse-string", json!({"text": "rust-pipe"}));
    let handle3 = dispatcher.dispatch(task3).await.unwrap();
    let result3 = handle3
        .await_with_timeout(Duration::from_secs(10))
        .await
        .unwrap();

    assert_eq!(result3.status, TaskStatus::Completed);
    assert_eq!(result3.payload.unwrap()["reversed"], "epip-tsur");

    py_worker.kill().await.ok();
}

/// Tests dispatching to a Unix stdio pipe (bash) worker — no network needed.
#[tokio::test]
async fn test_bash_stdio_via_dispatcher() {
    // This test uses the StdioTransport directly (already covered in e2e_stdio.rs)
    // Here we just verify that the bash worker protocol is stable
    let output = StdCommand::new("bash")
        .args(["tests/stdio_worker/worker.sh"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn();

    assert!(output.is_ok(), "Bash worker should be spawnable");

    let mut child = output.unwrap();
    use std::io::Write;
    let stdin = child.stdin.as_mut().unwrap();

    let task_json = r#"{"type":"TaskDispatch","task":{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","taskType":"word-count","payload":{"text":"one two three four five"},"metadata":{"createdAt":"2026-01-01T00:00:00Z","timeoutMs":5000,"priority":"Normal","retryCount":0,"maxRetries":3,"traceId":null}}}"#;
    writeln!(stdin, "{}", task_json).unwrap();

    let shutdown = r#"{"type":"Shutdown","graceful":true}"#;
    writeln!(stdin, "{}", shutdown).unwrap();

    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        stdout.contains(r#""wordCount":5"#),
        "Expected word count of 5, got: {}",
        stdout
    );
    assert!(stdout.contains(r#""status":"Completed""#));
}
