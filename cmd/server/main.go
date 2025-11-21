package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "modernc.org/sqlite" // <-- IMPORTANT

	"go-job-queue/internal/httpapi"
	"go-job-queue/internal/queue"
	"go-job-queue/internal/storage"
)

func main() {
	// Configure DB path (sqlite file)
	dbPath := "jobs.db"
	store, err := storage.NewStore(dbPath)
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}

	// Dispatcher coordinates pools and jobs
	d := queue.NewDispatcher(store)
	d.Start()

	// HTTP handlers
	h := &httpapi.Handler{Store: store}
	r := httpapi.NewRouter(h)

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// graceful shutdown signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Println("server starting on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen error: %v", err)
		}
	}()

	<-stop
	log.Println("shutting down...")
	d.Stop()
	srv.Close()
	log.Println("bye")
}
