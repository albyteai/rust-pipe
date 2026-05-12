package rustpipe

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type TaskHandler func(task Task) (any, error)

type WorkerConfig struct {
	URL                  string
	WorkerID             string
	Handlers             map[string]TaskHandler
	MaxConcurrency       int
	HeartbeatInterval    time.Duration
	ReconnectDelay       time.Duration
	MaxReconnectAttempts int
}

type Worker struct {
	config      WorkerConfig
	conn        *websocket.Conn
	activeTasks atomic.Int32
	running     atomic.Bool
	startTime   time.Time
	mu          sync.Mutex
	sem         chan struct{}
}

func NewWorker(config WorkerConfig) *Worker {
	if config.WorkerID == "" {
		config.WorkerID = fmt.Sprintf("worker-go-%s", uuid.New().String()[:8])
	}
	if config.MaxConcurrency == 0 {
		config.MaxConcurrency = 10
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 5 * time.Second
	}
	if config.ReconnectDelay == 0 {
		config.ReconnectDelay = time.Second
	}
	if config.MaxReconnectAttempts == 0 {
		config.MaxReconnectAttempts = 10
	}

	return &Worker{
		config: config,
		sem:    make(chan struct{}, config.MaxConcurrency),
	}
}

func (w *Worker) Start(ctx context.Context) error {
	w.running.Store(true)
	w.startTime = time.Now()
	return w.connect(ctx)
}

func (w *Worker) Stop() {
	w.running.Store(false)
	w.mu.Lock()
	if w.conn != nil {
		w.conn.Close()
	}
	w.mu.Unlock()
}

func (w *Worker) connect(ctx context.Context) error {
	attempts := 0
	for w.running.Load() {
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, w.config.URL, nil)
		if err != nil {
			attempts++
			if attempts >= w.config.MaxReconnectAttempts {
				return fmt.Errorf("max reconnect attempts reached: %w", err)
			}
			delay := w.config.ReconnectDelay * time.Duration(1<<(attempts-1))
			time.Sleep(delay)
			continue
		}

		w.mu.Lock()
		w.conn = conn
		w.mu.Unlock()
		attempts = 0

		w.register()
		go w.heartbeatLoop(ctx)
		w.receiveLoop()

		if !w.running.Load() {
			return nil
		}
	}
	return nil
}

func (w *Worker) register() {
	supportedTasks := make([]string, 0, len(w.config.Handlers))
	for k := range w.config.Handlers {
		supportedTasks = append(supportedTasks, k)
	}

	w.send(Message{
		Type: "WorkerRegister",
		Registration: &WorkerRegistration{
			WorkerID:       w.config.WorkerID,
			SupportedTasks: supportedTasks,
			MaxConcurrency: w.config.MaxConcurrency,
			Language:       "Go",
		},
	})
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !w.running.Load() {
				return
			}
			w.send(Message{
				Type: "Heartbeat",
				Heartbeat: &HeartbeatPayload{
					WorkerID:      w.config.WorkerID,
					ActiveTasks:   int(w.activeTasks.Load()),
					Capacity:      w.config.MaxConcurrency,
					UptimeSeconds: int(time.Since(w.startTime).Seconds()),
				},
			})
		}
	}
}

func (w *Worker) receiveLoop() {
	for w.running.Load() {
		_, raw, err := w.conn.ReadMessage()
		if err != nil {
			return
		}

		var msg Message
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "TaskDispatch":
			if msg.Task != nil {
				go w.executeTask(*msg.Task)
			}
		case "Shutdown":
			w.Stop()
			return
		}
	}
}

func (w *Worker) executeTask(task Task) {
	w.sem <- struct{}{}
	defer func() { <-w.sem }()

	w.activeTasks.Add(1)
	defer w.activeTasks.Add(-1)

	handler, ok := w.config.Handlers[task.TaskType]
	if !ok {
		w.sendResult(TaskResult{
			TaskID:   task.ID,
			Status:   "failed",
			WorkerID: w.config.WorkerID,
			Error: &TaskError{
				Code:      "UNKNOWN_TASK_TYPE",
				Message:   fmt.Sprintf("No handler for: %s", task.TaskType),
				Retryable: false,
			},
		})
		return
	}

	start := time.Now()
	result, err := handler(task)
	durationMs := int(time.Since(start).Milliseconds())

	if err != nil {
		w.sendResult(TaskResult{
			TaskID:     task.ID,
			Status:     "failed",
			DurationMs: durationMs,
			WorkerID:   w.config.WorkerID,
			Error: &TaskError{
				Code:      "HANDLER_ERROR",
				Message:   err.Error(),
				Retryable: true,
			},
		})
		return
	}

	payload, _ := json.Marshal(result)
	rawPayload := json.RawMessage(payload)

	w.sendResult(TaskResult{
		TaskID:     task.ID,
		Status:     "completed",
		Payload:    &rawPayload,
		DurationMs: durationMs,
		WorkerID:   w.config.WorkerID,
	})
}

func (w *Worker) sendResult(result TaskResult) {
	w.send(Message{
		Type:   "TaskResult",
		Result: &result,
	})
}

func (w *Worker) send(msg Message) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn != nil {
		data, _ := json.Marshal(msg)
		w.conn.WriteMessage(websocket.TextMessage, data)
	}
}
