package main

import "time"

// TaskStatus represents where a task is in its lifecycle.
type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusProcessing TaskStatus = "processing"
	StatusDone       TaskStatus = "done"
	StatusFailed     TaskStatus = "failed"
)

// Task is the core unit of work moving through the system.
// JSON tags control how this looks when serialized to API responses.
type Task struct {
	ID        string     `json:"id"`
	Payload   string     `json:"payload"`
	Status    TaskStatus `json:"status"`
	Result    string     `json:"result,omitempty"`
	Error     string     `json:"error,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// CreateTaskRequest is what the client sends in the POST body.
type CreateTaskRequest struct {
	Payload string `json:"payload"`
}
