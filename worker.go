package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// Job is what gets pushed onto the work queue — just enough info
// for a worker to do its job without touching the HTTP layer.
type Job struct {
	TaskID  string
	Payload string
}

// WorkerPool owns a fixed number of goroutines pulling from a shared
// channel. This caps concurrency instead of spawning one goroutine per
// request, which would let a traffic spike exhaust memory/CPU.
type WorkerPool struct {
	jobs    chan Job
	store   *Store
	workers int
	wg      sync.WaitGroup
}

func NewWorkerPool(store *Store, workers int, queueSize int) *WorkerPool {
	return &WorkerPool{
		jobs:    make(chan Job, queueSize),
		store:   store,
		workers: workers,
	}
}

// Start launches the worker goroutines. They keep running until
// ctx is cancelled (e.g. on server shutdown) or the jobs channel is closed.
func (p *WorkerPool) Start(ctx context.Context) {
	for i := 1; i <= p.workers; i++ {
		p.wg.Add(1)
		go p.runWorker(ctx, i)
	}
}

func (p *WorkerPool) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d: shutting down", id)
			return

		case job, ok := <-p.jobs:
			if !ok { // channel closed, no more jobs will ever arrive
				return
			}
			p.process(ctx, id, job)
		}
	}
}

// process does the actual "work". Here it's simulated, but this is
// where you'd call a real downstream service, run a computation, etc.
func (p *WorkerPool) process(ctx context.Context, workerID int, job Job) {
	p.store.UpdateStatus(job.TaskID, StatusProcessing, "", "")

	// Simulate variable-length work while still respecting cancellation.
	select {
	case <-time.After(time.Duration(200+rand.Intn(400)) * time.Millisecond):
		// work finished normally
	case <-ctx.Done():
		p.store.UpdateStatus(job.TaskID, StatusFailed, "", "cancelled: server shutting down")
		return
	}

	// Simulate an occasional failure so callers can see error handling too.
	if strings.TrimSpace(job.Payload) == "" {
		p.store.UpdateStatus(job.TaskID, StatusFailed, "", "empty payload")
		return
	}

	result := fmt.Sprintf("processed[worker=%d]: %s", workerID, strings.ToUpper(job.Payload))
	p.store.UpdateStatus(job.TaskID, StatusDone, result, "")
}

// Submit enqueues a job without blocking the HTTP handler for long.
// Returns false if the queue is full — caller should respond 503.
func (p *WorkerPool) Submit(job Job) bool {
	select {
	case p.jobs <- job:
		return true
	default:
		return false // queue full, backpressure signal
	}
}

// Shutdown closes the queue and waits for in-flight jobs to finish
// or for ctx to be cancelled, whichever comes first.
func (p *WorkerPool) Shutdown() {
	close(p.jobs)
	p.wg.Wait()
}
