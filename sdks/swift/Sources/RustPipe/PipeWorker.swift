import Foundation

public typealias TaskHandler = @Sendable (PipeTask) async throws -> Any

public struct PipeTask: Sendable {
    public let id: String
    public let taskType: String
    public let payload: [String: Any]
}

public actor PipeWorker {
    private let url: URL
    private let workerId: String
    private let handlers: [String: TaskHandler]
    private let maxConcurrency: Int
    private let heartbeatInterval: TimeInterval
    private var webSocketTask: URLSessionWebSocketTask?
    private var activeTasks: Int = 0
    private let startTime = Date()

    public init(
        url: String,
        workerId: String? = nil,
        handlers: [String: TaskHandler],
        maxConcurrency: Int = 10,
        heartbeatInterval: TimeInterval = 5.0
    ) {
        self.url = URL(string: url)!
        self.workerId = workerId ?? "worker-swift-\(UUID().uuidString.prefix(8))"
        self.handlers = handlers
        self.maxConcurrency = maxConcurrency
        self.heartbeatInterval = heartbeatInterval
    }

    public func start() async throws {
        let session = URLSession(configuration: .default)
        webSocketTask = session.webSocketTask(with: url)
        webSocketTask?.resume()

        await register()
        startHeartbeat()
        await receiveLoop()
    }

    public func stop() {
        webSocketTask?.cancel(with: .normalClosure, reason: nil)
    }

    private func register() async {
        let msg: [String: Any] = [
            "type": "WorkerRegister",
            "registration": [
                "workerId": workerId,
                "supportedTasks": Array(handlers.keys),
                "maxConcurrency": maxConcurrency,
                "language": "Swift"
            ]
        ]
        await send(msg)
    }

    private func startHeartbeat() {
        Task {
            while webSocketTask?.state == .running {
                try await Task.sleep(nanoseconds: UInt64(heartbeatInterval * 1_000_000_000))
                let msg: [String: Any] = [
                    "type": "Heartbeat",
                    "payload": [
                        "workerId": workerId,
                        "activeTasks": activeTasks,
                        "capacity": maxConcurrency,
                        "uptimeSeconds": Int(Date().timeIntervalSince(startTime))
                    ]
                ]
                await send(msg)
            }
        }
    }

    private func receiveLoop() async {
        guard let ws = webSocketTask else { return }
        while ws.state == .running {
            do {
                let message = try await ws.receive()
                switch message {
                case .string(let text):
                    await handleMessage(text)
                default:
                    break
                }
            } catch {
                break
            }
        }
    }

    private func handleMessage(_ raw: String) async {
        guard let data = raw.data(using: .utf8),
              let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let type = json["type"] as? String else { return }

        switch type {
        case "TaskDispatch":
            if let taskDict = json["task"] as? [String: Any] {
                let task = PipeTask(
                    id: taskDict["id"] as? String ?? "",
                    taskType: taskDict["taskType"] as? String ?? "",
                    payload: taskDict["payload"] as? [String: Any] ?? [:]
                )
                Task { await executeTask(task) }
            }
        case "Shutdown":
            stop()
        default:
            break
        }
    }

    private func executeTask(_ task: PipeTask) async {
        activeTasks += 1
        let start = Date()

        defer { activeTasks -= 1 }

        guard let handler = handlers[task.taskType] else {
            await sendResult(taskId: task.id, status: "Failed", payload: nil,
                error: ["code": "UNKNOWN_TASK_TYPE", "message": "No handler for: \(task.taskType)", "retryable": false],
                durationMs: 0)
            return
        }

        do {
            let result = try await handler(task)
            let duration = Int(Date().timeIntervalSince(start) * 1000)
            await sendResult(taskId: task.id, status: "Completed", payload: result, error: nil, durationMs: duration)
        } catch {
            let duration = Int(Date().timeIntervalSince(start) * 1000)
            await sendResult(taskId: task.id, status: "Failed", payload: nil,
                error: ["code": "HANDLER_ERROR", "message": error.localizedDescription, "retryable": true],
                durationMs: duration)
        }
    }

    private func sendResult(taskId: String, status: String, payload: Any?, error: [String: Any]?, durationMs: Int) async {
        var result: [String: Any] = [
            "taskId": taskId,
            "status": status,
            "durationMs": durationMs,
            "workerId": workerId
        ]
        if let payload = payload { result["payload"] = payload }
        if let error = error { result["error"] = error }

        let msg: [String: Any] = ["type": "TaskResult", "result": result]
        await send(msg)
    }

    private func send(_ message: [String: Any]) async {
        guard let data = try? JSONSerialization.data(withJSONObject: message),
              let text = String(data: data, encoding: .utf8) else { return }
        try? await webSocketTask?.send(.string(text))
    }
}
