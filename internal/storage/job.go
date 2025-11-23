package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// JobStatus enumerates possible states for a job.
type JobStatus string

const (
	StatusQueued    JobStatus = "queued"
	StatusRunning   JobStatus = "running"
	StatusSucceeded JobStatus = "succeeded"
	StatusFailed    JobStatus = "failed"
	StatusCancelled JobStatus = "cancelled"
)

// Job represents the data we store for each job.
// We keep payload generic as raw JSON and include metadata such as attempts, priority and type.
type Job struct {
	ID             int64           `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         JobStatus       `json:"status"`
	Progress       int             `json:"progress"` // 0-100
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	Priority       int             `json:"priority"` // higher is processed first
	ErrorMessage   sql.NullString  `json:"error_message"`
	CreatedAt      time.Time       `json:"created_at"`
	StartedAt      sql.NullTime    `json:"started_at"`
	FinishedAt     sql.NullTime    `json:"finished_at"`
	LastHeartbeat  sql.NullTime    `json:"last_heartbeat"`
	IdempotencyKey sql.NullString  `json:"idempotency_key"`
}

// ValidateBasic checks minimal requirements before enqueueing.
func (j *Job) ValidateBasic() error {
	if j.Type == "" {
		return errors.New("type is required")
	}
	if j.MaxAttempts <= 0 {
		j.MaxAttempts = 3
	}
	if j.Priority == 0 {
		j.Priority = 1
	}
	return nil
}
