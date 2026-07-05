package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	store := NewStore()
	pool := NewWorkerPool(store, 4, 100) // 4 workers, queue holds 100 pending jobs

	// ctx is cancelled when the process gets SIGINT/SIGTERM (Ctrl+C, or
	// `docker stop`, or a k8s pod eviction) — this is what "graceful
	// shutdown" means in practice.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool.Start(ctx)

	server := NewServer(store, pool)
	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: server.Routes(),
	}

	go func() {
		log.Println("listening on :8080")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done() // blocks here until a shutdown signal arrives
	log.Println("shutdown signal received, draining...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}

	pool.Shutdown() // wait for in-flight jobs to finish before exiting
	log.Println("shutdown complete")
}
