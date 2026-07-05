package main

import (
	"encoding/json"
	"net/http"
	"strings"
)

// Server wires the store and worker pool into HTTP handlers.
type Server struct {
	store *Store
	pool  *WorkerPool
}

func NewServer(store *Store, pool *WorkerPool) *Server {
	return &Server{store: store, pool: pool}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/tasks", s.handleTasks)      // POST create, GET list
	mux.HandleFunc("/tasks/", s.handleTaskByID)  // GET /tasks/{id}
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.createTask(w, r)
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.store.All())
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	var req CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	task := s.store.Create(req.Payload)

	// Enqueue the work; if the queue is full, tell the client to back off.
	if !s.pool.Submit(Job{TaskID: task.ID, Payload: req.Payload}) {
		s.store.UpdateStatus(task.ID, StatusFailed, "", "queue full")
		writeError(w, http.StatusServiceUnavailable, "server busy, try again shortly")
		return
	}

	// 202 Accepted: we took the request, but processing happens async.
	writeJSON(w, http.StatusAccepted, task)
}

func (s *Server) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/tasks/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing task id")
		return
	}

	task, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}

	writeJSON(w, http.StatusOK, task)
}

// --- small helpers kept at the bottom, used across handlers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
