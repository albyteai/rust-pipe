"""
Minimal Python worker for e2e testing.
Connects to rust-pipe dispatcher via WebSocket.
"""
import asyncio
import json
import os
import time

import websockets

DISPATCHER_URL = os.environ.get("DISPATCHER_URL", "ws://127.0.0.1:19901")
WORKER_ID = f"py-test-{int(time.time())}"


async def main():
    async with websockets.connect(DISPATCHER_URL) as ws:
        # Register
        await ws.send(json.dumps({
            "type": "WorkerRegister",
            "registration": {
                "workerId": WORKER_ID,
                "supportedTasks": ["echo", "multiply", "reverse-string"],
                "maxConcurrency": 5,
                "language": "Python",
            },
        }))
        print(f"[py-worker] Registered as {WORKER_ID}", flush=True)

        async for raw in ws:
            msg = json.loads(raw)

            if msg["type"] == "TaskDispatch":
                task = msg["task"]
                print(f"[py-worker] Received task: {task['taskType']} ({task['id']})", flush=True)

                start = time.time()
                try:
                    if task["taskType"] == "echo":
                        result = {"echoed": task["payload"]}
                    elif task["taskType"] == "multiply":
                        result = {"product": task["payload"]["a"] * task["payload"]["b"]}
                    elif task["taskType"] == "reverse-string":
                        result = {"reversed": task["payload"]["text"][::-1]}
                    else:
                        raise ValueError(f"Unknown task type: {task['taskType']}")

                    await ws.send(json.dumps({
                        "type": "TaskResult",
                        "result": {
                            "taskId": task["id"],
                            "status": "Completed",
                            "payload": result,
                            "durationMs": int((time.time() - start) * 1000),
                            "workerId": WORKER_ID,
                        },
                    }))
                except Exception as e:
                    await ws.send(json.dumps({
                        "type": "TaskResult",
                        "result": {
                            "taskId": task["id"],
                            "status": "Failed",
                            "error": {"code": "HANDLER_ERROR", "message": str(e), "retryable": False},
                            "durationMs": int((time.time() - start) * 1000),
                            "workerId": WORKER_ID,
                        },
                    }))

            elif msg["type"] == "Shutdown":
                print("[py-worker] Shutdown received", flush=True)
                break


if __name__ == "__main__":
    asyncio.run(main())
