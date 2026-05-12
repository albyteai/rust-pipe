package rustpipe

import "encoding/json"

type Task struct {
	ID       string          `json:"id"`
	TaskType string          `json:"taskType"`
	Payload  json.RawMessage `json:"payload"`
	Metadata TaskMetadata    `json:"metadata"`
}

type TaskMetadata struct {
	CreatedAt  string `json:"createdAt"`
	TimeoutMs  int    `json:"timeoutMs"`
	Priority   string `json:"priority"`
	RetryCount int    `json:"retryCount"`
	MaxRetries int    `json:"maxRetries"`
	TraceID    string `json:"traceId,omitempty"`
}

type TaskResult struct {
	TaskID     string           `json:"taskId"`
	Status     string           `json:"status"`
	Payload    *json.RawMessage `json:"payload,omitempty"`
	Error      *TaskError       `json:"error,omitempty"`
	DurationMs int              `json:"durationMs"`
	WorkerID   string           `json:"workerId"`
}

type TaskError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type Message struct {
	Type         string              `json:"type"`
	Task         *Task               `json:"task,omitempty"`
	Result       *TaskResult         `json:"result,omitempty"`
	Heartbeat    *HeartbeatPayload   `json:"payload,omitempty"`
	Registration *WorkerRegistration `json:"registration,omitempty"`
	Signal       *BackpressureSignal `json:"signal,omitempty"`
	Graceful     bool                `json:"graceful,omitempty"`
	TaskID       string              `json:"taskId,omitempty"`
	Reason       string              `json:"reason,omitempty"`
}

type HeartbeatPayload struct {
	WorkerID      string `json:"workerId"`
	ActiveTasks   int    `json:"activeTasks"`
	Capacity      int    `json:"capacity"`
	UptimeSeconds int    `json:"uptimeSeconds"`
}

type WorkerRegistration struct {
	WorkerID       string   `json:"workerId"`
	SupportedTasks []string `json:"supportedTasks"`
	MaxConcurrency int      `json:"maxConcurrency"`
	Language       string   `json:"language"`
}

type BackpressureSignal struct {
	WorkerID       string  `json:"workerId"`
	CurrentLoad    float64 `json:"currentLoad"`
	ShouldThrottle bool    `json:"shouldThrottle"`
}
