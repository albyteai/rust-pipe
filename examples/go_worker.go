// Example: Go worker connecting to a rust-pipe dispatcher
//
// Run the Rust dispatcher first, then:
//   go run examples/go_worker.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	rustpipe "github.com/havocray/rust-pipe-go"
)

func main() {
	worker := rustpipe.NewWorker(rustpipe.WorkerConfig{
		URL: "ws://localhost:9876",
		Handlers: map[string]rustpipe.TaskHandler{
			"scan-network": scanNetwork,
			"check-ports":  checkPorts,
		},
		MaxConcurrency: 10,
	})

	fmt.Println("Go worker connecting to rust-pipe dispatcher...")

	ctx := context.Background()
	if err := worker.Start(ctx); err != nil {
		fmt.Printf("Worker error: %v\n", err)
	}
}

func scanNetwork(task rustpipe.Task) (any, error) {
	var payload struct {
		Target string   `json:"target"`
		Ports  []int    `json:"ports"`
	}
	json.Unmarshal(task.Payload, &payload)

	fmt.Printf("Scanning network: %s\n", payload.Target)
	time.Sleep(3 * time.Second)

	return map[string]any{
		"open_ports":    []int{22, 80, 443, 8080},
		"services":     []string{"ssh", "http", "https", "http-proxy"},
		"scan_duration": "3.2s",
	}, nil
}

func checkPorts(task rustpipe.Task) (any, error) {
	fmt.Println("Checking ports...")
	time.Sleep(1 * time.Second)

	return map[string]any{
		"status": "healthy",
		"checked": 1000,
	}, nil
}
