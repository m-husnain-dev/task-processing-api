package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// ----------------------------------------------------------------------------
// STORE TESTS — table-driven, the idiomatic Go pattern for multiple cases
// ----------------------------------------------------------------------------

func TestStore_CreateAndGet(t *testing.T) {
	cases := []struct {
		name    string
		payload string
	}{
		{"normal payload", "hello world"},
		{"empty payload", ""},
		{"long payload", "this is a much longer piece of text to process"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := NewStore()
			task := store.Create(c.payload)

			if task.Status != StatusPending {
				t.Errorf("expected status %q, got %q", StatusPending, task.Status)
			}

			got, ok := store.Get(task.ID)
			if !ok {
				t.Fatalf("expected task %s to exist", task.ID)
			}
			if got.Payload != c.payload {
				t.Errorf("expected payload %q, got %q", c.payload, got.Payload)
			}
		})
	}
}

func TestStore_GetMissing(t *testing.T) {
	store := NewStore()
	_, ok := store.Get("does-not-exist")
	if ok {
		t.Error("expected ok=false for missing task, got true")
	}
}

func TestStore_UpdateStatus(t *testing.T) {
	store := NewStore()
	task := store.Create("some work")

	store.UpdateStatus(task.ID, StatusDone, "RESULT", "")

	got, _ := store.Get(task.ID)
	if got.Status != StatusDone {
		t.Errorf("expected status done, got %q", got.Status)
	}
	if got.Result != "RESULT" {
		t.Errorf("expected result RESULT, got %q", got.Result)
	}
}

// This test specifically exercises concurrent access to catch data races.
// Run with: go test -race ./...
func TestStore_ConcurrentAccess(t *testing.T) {
	store := NewStore()
	var wg sync.WaitGroup

	// 50 goroutines creating tasks at the same time
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.Create("concurrent payload")
		}()
	}
	wg.Wait()

	if len(store.All()) != 50 {
		t.Errorf("expected 50 tasks, got %d", len(store.All()))
	}
}

// ----------------------------------------------------------------------------
// WORKER POOL TESTS
// ----------------------------------------------------------------------------

func TestWorkerPool_ProcessesJob(t *testing.T) {
	store := NewStore()
	pool := NewWorkerPool(store, 2, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	task := store.Create("go is fun")
	if !pool.Submit(Job{TaskID: task.ID, Payload: task.Payload}) {
		t.Fatal("expected job to be accepted")
	}

	// Poll for completion instead of a fixed sleep — more reliable under
	// varying CI/test-runner speeds.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := store.Get(task.ID)
		if got.Status == StatusDone {
			if got.Result == "" {
				t.Error("expected non-empty result on success")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("task did not complete in time")
}

func TestWorkerPool_FailsOnEmptyPayload(t *testing.T) {
	store := NewStore()
	pool := NewWorkerPool(store, 1, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	task := store.Create("")
	pool.Submit(Job{TaskID: task.ID, Payload: ""})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := store.Get(task.ID)
		if got.Status == StatusFailed {
			if got.Error == "" {
				t.Error("expected an error message on failure")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("task did not fail as expected in time")
}

func TestWorkerPool_QueueFullReturnsFalse(t *testing.T) {
	store := NewStore()
	// Zero workers means nothing ever drains the queue, so it fills up fast.
	pool := NewWorkerPool(store, 0, 1)

	first := pool.Submit(Job{TaskID: "a", Payload: "x"})
	second := pool.Submit(Job{TaskID: "b", Payload: "y"})

	if !first {
		t.Error("expected first submit to succeed (queue size 1)")
	}
	if second {
		t.Error("expected second submit to fail, queue should be full")
	}
}

// ----------------------------------------------------------------------------
// HTTP HANDLER TESTS — using httptest, no real network needed
// ----------------------------------------------------------------------------

func TestHandler_HealthCheck(t *testing.T) {
	store := NewStore()
	pool := NewWorkerPool(store, 1, 10)
	server := NewServer(store, pool)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestHandler_CreateTask(t *testing.T) {
	store := NewStore()
	// Deliberately NOT calling pool.Start() here — this test only checks
	// that the HTTP layer responds correctly and immediately with a
	// "pending" task. Starting the pool introduces a race: on a fast
	// machine the worker can grab the job and flip it to "processing"
	// before this test even reads the response body. Processing behavior
	// is already covered separately by TestWorkerPool_ProcessesJob.
	pool := NewWorkerPool(store, 1, 10)

	server := NewServer(store, pool)

	body, _ := json.Marshal(CreateTaskRequest{Payload: "test payload"})
	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d, body: %s", rec.Code, rec.Body.String())
	}

	var task Task
	if err := json.NewDecoder(rec.Body).Decode(&task); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if task.Status != StatusPending {
		t.Errorf("expected pending status, got %q", task.Status)
	}
}

func TestHandler_GetTaskNotFound(t *testing.T) {
	store := NewStore()
	pool := NewWorkerPool(store, 1, 10)
	server := NewServer(store, pool)

	req := httptest.NewRequest(http.MethodGet, "/tasks/does-not-exist", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestHandler_CreateTask_InvalidJSON(t *testing.T) {
	store := NewStore()
	pool := NewWorkerPool(store, 1, 10)
	server := NewServer(store, pool)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader([]byte("not json")))
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

// ----------------------------------------------------------------------------
// BENCHMARK — measures how fast task creation is under load
// Run with: go test -bench=. -benchmem
// ----------------------------------------------------------------------------

func BenchmarkStore_Create(b *testing.B) {
	store := NewStore()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Create("benchmark payload")
	}
}