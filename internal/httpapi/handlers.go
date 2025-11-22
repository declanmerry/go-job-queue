package httpapi

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	"go-job-queue/internal/storage"
	"go-job-queue/internal/version"
)

// Handler holds dependencies for HTTP handlers.
type Handler struct {
	Store *storage.Store
}

// NewRouter builds the HTTP router with routes bound to our handlers.
func NewRouter(h *Handler) http.Handler {
	r := mux.NewRouter()

	r.Use(versionHeaderMiddleware)

	r.HandleFunc("/jobs", h.CreateJob).Methods("POST")
	r.HandleFunc("/jobs/{id:[0-9]+}", h.GetJob).Methods("GET")
	r.HandleFunc("/jobs/{id:[0-9]+}/cancel", h.CancelJob).Methods("POST")
	return r
}

func versionHeaderMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Add version header
		w.Header().Set("X-App-Version", version.Version)
		next.ServeHTTP(w, r)
	})
}

// CreateJob accepts a JSON payload to create a job.
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	var j storage.Job
	if err := json.Unmarshal(body, &j); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	// ensure payload remains raw
	j.Payload = json.RawMessage(j.Payload)
	if j.MaxAttempts == 0 {
		j.MaxAttempts = 3
	}
	// store
	enq, err := h.Store.Enqueue(&j)
	if err != nil {
		log.Printf("enqueue error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(enq)
}

// GetJob returns job metadata so clients can poll status/progress.
func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, _ := strconv.ParseInt(vars["id"], 10, 64)
	j, err := h.Store.Get(id)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(j)
}

// CancelJob marks a job cancelled. Note: workers should periodically check DB or store cancellation state; here we just persist status change.
func (h *Handler) CancelJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, _ := strconv.ParseInt(vars["id"], 10, 64)
	if err := h.Store.Cancel(id); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
