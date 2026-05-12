// Minimal Go worker for e2e testing.
// Connects to rust-pipe dispatcher via WebSocket.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

var dispatcherURL = getEnv("DISPATCHER_URL", "ws://127.0.0.1:19902")
var workerID = fmt.Sprintf("go-test-%d", time.Now().Unix())

func main() {
	conn, _, err := websocket.DefaultDialer.Dial(dispatcherURL, nil)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Register
	register := map[string]any{
		"type": "WorkerRegister",
		"registration": map[string]any{
			"workerId":       workerID,
			"supportedTasks": []string{"echo", "fibonacci", "uppercase"},
			"maxConcurrency": 5,
			"language":       "Go",
		},
	}
	data, _ := json.Marshal(register)
	conn.WriteMessage(websocket.TextMessage, data)
	fmt.Fprintf(os.Stderr, "[go-worker] Registered as %s\n", workerID)

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			log.Fatalf("Read error: %v", err)
		}

		var msg map[string]any
		json.Unmarshal(raw, &msg)

		msgType, _ := msg["type"].(string)

		switch msgType {
		case "TaskDispatch":
			taskData, _ := json.Marshal(msg["task"])
			var task struct {
				ID       string         `json:"id"`
				TaskType string         `json:"taskType"`
				Payload  map[string]any `json:"payload"`
			}
			json.Unmarshal(taskData, &task)

			fmt.Fprintf(os.Stderr, "[go-worker] Received task: %s (%s)\n", task.TaskType, task.ID)

			start := time.Now()
			var result any
			var taskErr error

			switch task.TaskType {
			case "echo":
				result = map[string]any{"echoed": task.Payload}
			case "fibonacci":
				n := int(task.Payload["n"].(float64))
				result = map[string]any{"result": fibonacci(n)}
			case "uppercase":
				text, _ := task.Payload["text"].(string)
				result = map[string]any{"upper": toUpper(text)}
			default:
				taskErr = fmt.Errorf("unknown task type: %s", task.TaskType)
			}

			duration := int(time.Since(start).Milliseconds())

			var response map[string]any
			if taskErr != nil {
				response = map[string]any{
					"type": "TaskResult",
					"result": map[string]any{
						"taskId":     task.ID,
						"status":     "failed",
						"error":      map[string]any{"code": "HANDLER_ERROR", "message": taskErr.Error(), "retryable": false},
						"durationMs": duration,
						"workerId":   workerID,
					},
				}
			} else {
				response = map[string]any{
					"type": "TaskResult",
					"result": map[string]any{
						"taskId":     task.ID,
						"status":     "completed",
						"payload":    result,
						"durationMs": duration,
						"workerId":   workerID,
					},
				}
			}

			respData, _ := json.Marshal(response)
			conn.WriteMessage(websocket.TextMessage, respData)

		case "Shutdown":
			fmt.Fprintf(os.Stderr, "[go-worker] Shutdown received\n")
			return
		}
	}
}

func fibonacci(n int) int {
	if n <= 1 {
		return n
	}
	a, b := 0, 1
	for i := 2; i <= n; i++ {
		a, b = b, a+b
	}
	return b
}

func toUpper(s string) string {
	result := make([]byte, len(s))
	for i := range s {
		if s[i] >= 'a' && s[i] <= 'z' {
			result[i] = s[i] - 32
		} else {
			result[i] = s[i]
		}
	}
	return string(result)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
