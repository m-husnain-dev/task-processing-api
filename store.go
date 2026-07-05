package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Store holds tasks in memory, safe for concurrent access from
// HTTP handler goroutines AND worker goroutines at the same time.
type Store struct {
	mu     sync.RWMutex     // RWMutex: many readers OR one writer, never both
	tasks  map[string]*Task
	nextID int64            // atomic counter, no mutex needed for a simple int
}

func NewStore() *Store {
	return &Store{
		tasks: make(map[string]*Task),
	}
}

// Create inserts a new pending task and returns it.
func (s *Store) Create(payload string) *Task {
	id := atomic.AddInt64(&s.nextID, 1)
	now := time.Now()

	task := &Task{
		ID:        fmt.Sprintf("t-%d", id),
		Payload:   payload,
		Status:    StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	s.mu.Lock() // writing to the map needs the exclusive lock
	defer s.mu.Unlock()
	s.tasks[task.ID] = task

	return task
}

// Get retrieves a task by ID. Returns nil, false if not found.
// Returns a COPY, not the pointer stored internally, so callers can't
// mutate our internal state by accident.
func (s *Store) Get(id string) (Task, bool) {
	s.mu.RLock() // reading only needs the shared read lock
	defer s.mu.RUnlock()

	t, ok := s.tasks[id]
	if !ok {
		return Task{}, false
	}
	return *t, true
}

// UpdateStatus is called by workers once processing finishes.
func (s *Store) UpdateStatus(id string, status TaskStatus, result string, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[id]
	if !ok {
		return
	}
	t.Status = status
	t.Result = result
	t.Error = errMsg
	t.UpdatedAt = time.Now()
}

// All returns a snapshot slice of every task (useful for a list endpoint).
func (s *Store) All() []Task {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		out = append(out, *t)
	}
	return out
}
