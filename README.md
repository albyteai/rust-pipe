# rust-pipe

Lightweight typed task dispatch from Rust to polyglot workers (TypeScript, Python, Go, Java, C#, Ruby, Elixir, Swift, PHP).

[![Crates.io](https://img.shields.io/crates/v/rust-pipe.svg)](https://crates.io/crates/rust-pipe)
[![Docs.rs](https://docs.rs/rust-pipe/badge.svg)](https://docs.rs/rust-pipe)
[![License](https://img.shields.io/crates/l/rust-pipe.svg)](LICENSE-MIT)

---

## What it does

rust-pipe sits between "raw gRPC" (too low-level) and "Temporal" (too heavy). It gives you typed task dispatch from a Rust orchestrator to workers written in any language — with zero boilerplate.

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': { 'primaryColor': '#1f2937', 'primaryTextColor': '#f9fafb', 'primaryBorderColor': '#6366f1', 'lineColor': '#818cf8', 'secondaryColor': '#374151', 'tertiaryColor': '#111827', 'edgeLabelBackground': '#1f2937', 'fontSize': '14px'}}}%%

graph LR
    subgraph RUST["🦀 Rust Dispatcher"]
        direction TB
        D[/"Task Dispatch Engine"\]
        P[("Worker Pool")]
        D --- P
    end

    subgraph TRANSPORTS["⚡ Transports"]
        direction TB
        WS["🌐 WebSocket"]
        STDIO["📟 stdio"]
        DOCK["🐳 Docker"]
        SSHT["🔑 SSH"]
        WASM["⚙️ WASM"]
    end

    subgraph WORKERS["🔧 Workers"]
        direction TB
        TS["TypeScript"]
        PY["Python"]
        GO["Go"]
        JAVA["Java"]
        CS["C#"]
        RUBY["Ruby"]
        CLI["Any CLI"]
    end

    RUST ==>|route| TRANSPORTS
    TRANSPORTS ==>|deliver| WORKERS

    style RUST fill:#1e1b4b,stroke:#6366f1,stroke-width:2px,color:#e0e7ff
    style TRANSPORTS fill:#172554,stroke:#3b82f6,stroke-width:2px,color:#dbeafe
    style WORKERS fill:#052e16,stroke:#22c55e,stroke-width:2px,color:#dcfce7
```

---

## How it works

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': { 'primaryColor': '#1f2937', 'primaryTextColor': '#f9fafb', 'primaryBorderColor': '#6366f1', 'lineColor': '#818cf8', 'secondaryColor': '#374151', 'tertiaryColor': '#111827', 'actorBkg': '#312e81', 'actorBorder': '#6366f1', 'actorTextColor': '#e0e7ff', 'activationBkgColor': '#1e3a5f', 'activationBorderColor': '#3b82f6', 'signalColor': '#818cf8', 'noteBkgColor': '#1c1917', 'noteBorderColor': '#a16207', 'noteTextColor': '#fef3c7'}}}%%

sequenceDiagram
    participant D as 🦀 Dispatcher
    participant W as 🔧 Worker

    rect rgba(99, 102, 241, 0.1)
        Note over D,W: 🔌 Connection Phase
        W->>+D: Connect via WebSocket
        W->>D: WorkerRegister {tasks, capacity}
        D->>-W: WorkerRegistered ✓
        D-->>D: Add to pool (least-loaded routing)
    end

    rect rgba(34, 197, 94, 0.1)
        Note over D,W: 🚀 Dispatch Phase
        D->>D: select_and_reserve(task_type)
        D->>+W: TaskDispatch {taskType, payload}
        W->>W: Execute handler
        W->>-D: TaskResult {status: Completed, payload}
        D-->>D: Resolve DispatchResult to caller
    end

    rect rgba(59, 130, 246, 0.1)
        Note over D,W: 💓 Health Monitoring
        loop Every 5 seconds
            W->>D: Heartbeat {activeTasks, capacity}
        end
    end

    rect rgba(239, 68, 68, 0.1)
        Note over D,W: ☠️ Failure Detection
        D->>D: Heartbeat timeout exceeded
        D-->>D: Mark worker Dead
        D-->>D: Fail pending tasks → WorkerDisconnected
    end
```

---

## Architecture

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': { 'primaryColor': '#1f2937', 'primaryTextColor': '#f9fafb', 'primaryBorderColor': '#6366f1', 'lineColor': '#818cf8', 'secondaryColor': '#374151', 'fontSize': '13px'}}}%%

graph TB
    subgraph CORE["🦀 rust-pipe crate"]
        direction TB
        DISP["<b>Dispatcher</b><br/>Task routing & lifecycle"]
        POOL["<b>Worker Pool</b><br/>Least-loaded selection"]
        VALID["<b>Validation</b><br/>Input sanitization"]
        SCHEMA["<b>Schema</b><br/>Task, Result, Priority"]
        DISP --> POOL
        DISP --> VALID
        DISP --> SCHEMA
    end

    subgraph TRANS["⚡ Transport Layer"]
        direction LR
        WS["🌐<br/>WebSocket<br/><i>networked</i>"]
        STDIO["📟<br/>stdio<br/><i>local CLI</i>"]
        DOCK["🐳<br/>Docker<br/><i>isolated</i>"]
        SSHT["🔑<br/>SSH<br/><i>remote</i>"]
        WASMT["⚙️<br/>WASM<br/><i>sandboxed</i>"]
    end

    subgraph SDKS["📦 Language SDKs"]
        direction LR
        S1["TypeScript<br/><code>@rust-pipe/worker</code>"]
        S2["Python<br/><code>pip install rust-pipe</code>"]
        S3["Go<br/><code>rust-pipe-go</code>"]
        S4["Java · C# · Ruby<br/>Elixir · Swift · PHP"]
        S5["<b>Any CLI</b><br/><i>No SDK needed</i>"]
    end

    CORE ==> TRANS
    TRANS ==> SDKS

    style CORE fill:#1e1b4b,stroke:#6366f1,stroke-width:2px,color:#e0e7ff
    style TRANS fill:#172554,stroke:#3b82f6,stroke-width:2px,color:#dbeafe
    style SDKS fill:#052e16,stroke:#22c55e,stroke-width:2px,color:#dcfce7
    style DISP fill:#312e81,stroke:#818cf8,color:#e0e7ff
    style POOL fill:#312e81,stroke:#818cf8,color:#e0e7ff
    style VALID fill:#312e81,stroke:#818cf8,color:#e0e7ff
    style SCHEMA fill:#312e81,stroke:#818cf8,color:#e0e7ff
    style WS fill:#1e3a5f,stroke:#60a5fa,color:#dbeafe
    style STDIO fill:#1e3a5f,stroke:#60a5fa,color:#dbeafe
    style DOCK fill:#1e3a5f,stroke:#60a5fa,color:#dbeafe
    style SSHT fill:#1e3a5f,stroke:#60a5fa,color:#dbeafe
    style WASMT fill:#1e3a5f,stroke:#60a5fa,color:#dbeafe
```

---

## Features

| Feature | Description |
|---------|-------------|
| 🎯 **Least-loaded routing** | Tasks go to the worker with most available capacity |
| 💀 **Dead worker detection** | Heartbeat timeout marks workers dead, fails their pending tasks |
| 🚦 **Backpressure** | Workers signal overload, dispatcher throttles |
| 🛑 **Graceful shutdown** | `stop()` drains tasks and cleanly disconnects |
| 🔒 **Input validation** | All transport configs validated against command injection |
| 🔄 **Idempotent start** | Safe to call `start()` multiple times |
| ⏱️ **Timeout support** | Per-task timeouts with `await_with_timeout()` |
| 📊 **Pool stats** | Real-time visibility into worker capacity and utilization |

---

## Task Lifecycle

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': { 'primaryColor': '#312e81', 'primaryTextColor': '#e0e7ff', 'primaryBorderColor': '#6366f1', 'lineColor': '#818cf8', 'secondaryColor': '#1e3a5f', 'tertiaryColor': '#1c1917', 'fontSize': '13px'}}}%%

stateDiagram-v2
    direction LR

    [*] --> Pending: Task::new()
    Pending --> Dispatched: dispatch()
    Dispatched --> Running: Worker receives

    Running --> Completed: ✅ Success
    Running --> Failed: ❌ Error
    Running --> TimedOut: ⏱️ Deadline exceeded

    Dispatched --> Failed: Transport error
    Running --> Cancelled: Kill signal

    Completed --> [*]
    Failed --> [*]
    TimedOut --> [*]
    Cancelled --> [*]
```

---

## Quick start

### Rust (dispatcher)

```rust
use rust_pipe::prelude::*;
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dispatcher = Dispatcher::builder()
        .host("0.0.0.0")
        .port(9876)
        .build();

    dispatcher.start().await?;

    // Workers connect via WebSocket and register their supported task types.
    // Once connected, dispatch tasks:
    let task = Task::new("scan-target", json!({
        "url": "https://example.com",
        "checks": ["xss", "sqli"]
    }))
    .with_timeout(60_000)
    .with_priority(Priority::High);

    let handle = dispatcher.dispatch(task).await?;
    let result = handle.await_with_timeout(Duration::from_secs(60)).await?;

    println!("Result: {:?}", result.payload);
    Ok(())
}
```

### TypeScript (worker)

```typescript
import { createWorker } from '@rust-pipe/worker';

const worker = createWorker({
  url: 'ws://localhost:9876',
  handlers: {
    'scan-target': async (task) => {
      // Do work...
      return { vulnerabilities: 3 };
    },
  },
});

await worker.start();
```

### Python (worker)

```python
from rust_pipe import create_worker

async def scan_target(task):
    # Do work...
    return {"vulnerabilities": 3}

worker = create_worker(
    url="ws://localhost:9876",
    handlers={"scan-target": scan_target},
)
await worker.start()
```

### Bash (worker via stdio — no SDK needed)

```bash
#!/bin/bash
# Read JSON task from stdin, write JSON result to stdout
while IFS= read -r line; do
  task_id=$(echo "$line" | jq -r '.task.id')
  echo "{\"type\":\"TaskResult\",\"result\":{\"taskId\":\"$task_id\",\"status\":\"Completed\",\"payload\":{\"done\":true},\"durationMs\":1,\"workerId\":\"bash\"}}"
done
```

---

## SDKs

| Language | Package | Install |
|----------|---------|---------|
| TypeScript | `@rust-pipe/worker` | `npm install @rust-pipe/worker` |
| Python | `rust-pipe` | `pip install rust-pipe` |
| Go | `rust-pipe-go` | `go get github.com/albyte-ai/rust-pipe-go` |
| Java | `io.rustpipe:rust-pipe-worker` | Maven Central |
| C# | `RustPipe.Worker` | `dotnet add package RustPipe.Worker` |
| Ruby | `rust_pipe` | `gem install rust_pipe` |
| Elixir | `rust_pipe` | `{:rust_pipe, "~> 0.1.0"}` in mix.exs |
| Swift | `RustPipe` | Swift Package Manager |
| PHP | `rust-pipe/worker` | `composer require rust-pipe/worker` |
| **Any CLI** | None needed | Read/write JSON on stdin/stdout |

---

## Wire Protocol

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': { 'primaryColor': '#1f2937', 'primaryTextColor': '#f9fafb', 'primaryBorderColor': '#6366f1', 'lineColor': '#818cf8', 'fontSize': '12px'}}}%%

graph LR
    subgraph DISPATCH["📤 Dispatcher → Worker"]
        TD["<b>TaskDispatch</b><br/>{task: {id, taskType, payload, metadata}}"]
        KILL["<b>Kill</b><br/>{taskId, reason}"]
        SHUT["<b>Shutdown</b><br/>{graceful: bool}"]
    end

    subgraph RESPONSE["📥 Worker → Dispatcher"]
        TR["<b>TaskResult</b><br/>{taskId, status, payload, durationMs}"]
        HB["<b>Heartbeat</b><br/>{activeTasks, capacity, uptimeSeconds}"]
        BP["<b>Backpressure</b><br/>{currentLoad, shouldThrottle}"]
        WR["<b>WorkerRegister</b><br/>{workerId, supportedTasks, maxConcurrency}"]
    end

    style DISPATCH fill:#1e1b4b,stroke:#6366f1,stroke-width:2px,color:#e0e7ff
    style RESPONSE fill:#052e16,stroke:#22c55e,stroke-width:2px,color:#dcfce7
    style TD fill:#312e81,stroke:#818cf8,color:#e0e7ff
    style KILL fill:#312e81,stroke:#818cf8,color:#e0e7ff
    style SHUT fill:#312e81,stroke:#818cf8,color:#e0e7ff
    style TR fill:#14532d,stroke:#4ade80,color:#dcfce7
    style HB fill:#14532d,stroke:#4ade80,color:#dcfce7
    style BP fill:#14532d,stroke:#4ade80,color:#dcfce7
    style WR fill:#14532d,stroke:#4ade80,color:#dcfce7
```

All fields use **camelCase**. JSON over WebSocket or stdin/stdout:

```json
{"type": "TaskDispatch", "task": {"id": "...", "taskType": "scan", "payload": {...}}}
{"type": "TaskResult", "result": {"taskId": "...", "status": "Completed", "payload": {...}}}
{"type": "Heartbeat", "payload": {"workerId": "w1", "activeTasks": 2, "capacity": 10}}
```

---

## Transports

| Transport | Use case | Requires SDK? | Isolation |
|-----------|----------|---------------|-----------|
| 🌐 **WebSocket** | Networked workers, auto-scaling pools | Yes | Process |
| 📟 **stdio** | Any CLI tool as a worker | No | Process |
| 🐳 **Docker** | Untrusted workloads, resource limits | No | Container |
| 🔑 **SSH** | Distribute across machines | No | Machine |
| ⚙️ **WASM** | Plugin systems, user-provided logic | No | Sandbox |

---

## Safety

| Protection | Implementation |
|-----------|----------------|
| Command injection | Input validation on worker IDs, Docker images, hostnames, env vars |
| Resource exhaustion | Capacity-based routing, backpressure signaling |
| Hanging tasks | Dead worker detection fails pending tasks automatically |
| Data leakage | Per-worker task isolation, no shared state between workers |
| Denial of service | Rate limiting via max_concurrency per worker |

---

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
