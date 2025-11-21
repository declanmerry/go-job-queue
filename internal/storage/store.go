package storage

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "modernc.org/sqlite" // Pure Go SQLite driver (no CGO needed)
)

// Store provides methods to persist and retrieve jobs.
type Store struct {
	db *sql.DB
}

// NewStore opens (or creates) the SQLite database and ensures schema exists.
func NewStore(dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn) // NOTE: driver name is "sqlite", not "sqlite3"
	if err != nil {
		return nil, err
	}

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

// migrate creates the jobs table if it doesn't exist.
func (s *Store) migrate() error {
	q := `
	CREATE TABLE IF NOT EXISTS jobs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		type TEXT NOT NULL,
		payload BLOB NOT NULL,
		status TEXT NOT NULL,
		progress INTEGER NOT NULL DEFAULT 0,
		attempts INTEGER NOT NULL DEFAULT 0,
		max_attempts INTEGER NOT NULL DEFAULT 3,
		priority INTEGER NOT NULL DEFAULT 1,
		error_message TEXT,
		created_at DATETIME NOT NULL,
		started_at DATETIME NULL,
		finished_at DATETIME NULL
	);
	CREATE INDEX IF NOT EXISTS idx_jobs_status_priority ON jobs(status, priority DESC, created_at);
	`
	_, err := s.db.Exec(q)
	return err
}

// Enqueue inserts a new job and returns it with ID and timestamps set.
func (s *Store) Enqueue(j *Job) (*Job, error) {
	if err := j.ValidateBasic(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	res, err := s.db.Exec(`INSERT INTO jobs(type, payload, status, progress, attempts, max_attempts, priority, created_at) VALUES(?,?,?,?,?,?,?,?)`,
		j.Type, string(j.Payload), string(StatusQueued), 0, 0, j.MaxAttempts, j.Priority, now)
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	j.ID = id
	j.Status = StatusQueued
	j.Progress = 0
	j.Attempts = 0
	j.CreatedAt = now
	return j, nil
}

// Get retrieves a job by id.
func (s *Store) Get(id int64) (*Job, error) {
	row := s.db.QueryRow(`SELECT id, type, payload, status, progress, attempts, max_attempts, priority, error_message, created_at, started_at, finished_at FROM jobs WHERE id = ?`, id)

	var j Job
	var payload string
	var errMsg sql.NullString

	if err := row.Scan(&j.ID, &j.Type, &payload, &j.Status, &j.Progress, &j.Attempts, &j.MaxAttempts, &j.Priority, &errMsg, &j.CreatedAt, &j.StartedAt, &j.FinishedAt); err != nil {
		return nil, err
	}
	j.Payload = []byte(payload)
	j.ErrorMessage = errMsg
	return &j, nil
}

// NextQueued pulls the highest priority queued job and marks it running.
func (s *Store) NextQueued() (*Job, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}

	row := tx.QueryRow(`SELECT id FROM jobs WHERE status = ? ORDER BY priority DESC, created_at ASC LIMIT 1`, string(StatusQueued))

	var id int64
	if err := row.Scan(&id); err != nil {
		tx.Rollback()
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	now := time.Now().UTC()
	_, err = tx.Exec(`UPDATE jobs SET status = ?, started_at = ? WHERE id = ?`, string(StatusRunning), now, id)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return s.Get(id)
}

// Update modifies mutable fields of a job.
func (s *Store) Update(j *Job) error {
	_, err := s.db.Exec(`UPDATE jobs SET status = ?, progress = ?, attempts = ?, max_attempts = ?, priority = ?, error_message = ?, started_at = ?, finished_at = ? WHERE id = ?`,
		string(j.Status), j.Progress, j.Attempts, j.MaxAttempts, j.Priority,
		nullableString(j.ErrorMessage), nullableTime(j.StartedAt), nullableTime(j.FinishedAt), j.ID)
	return err
}

func nullableString(ns sql.NullString) interface{} {
	if ns.Valid {
		return ns.String
	}
	return nil
}

func nullableTime(nt sql.NullTime) interface{} {
	if nt.Valid {
		return nt.Time
	}
	return nil
}

// Cancel sets a job to cancelled state.
func (s *Store) Cancel(id int64) error {
	res, err := s.db.Exec(`UPDATE jobs SET status = ? WHERE id = ? AND status IN (?, ?)`,
		string(StatusCancelled), id, string(StatusQueued), string(StatusRunning))
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("job %d could not be cancelled", id)
	}
	log.Printf("job %d cancelled", id)
	return nil
}

// Requeue increments attempts and puts the job back to queued.
func (s *Store) Requeue(j *Job) error {
	j.Attempts++
	j.Status = StatusQueued
	j.Progress = 0
	j.ErrorMessage = sql.NullString{}
	_, err := s.db.Exec(`UPDATE jobs SET status = ?, progress = ?, attempts = ?, error_message = NULL WHERE id = ?`,
		string(j.Status), j.Progress, j.Attempts, j.ID)
	return err
}

// MarkFailed sets the job as permanently failed.
func (s *Store) MarkFailed(j *Job, msg string) error {
	j.Status = StatusFailed
	j.ErrorMessage = sql.NullString{String: msg, Valid: true}
	now := time.Now().UTC()
	j.FinishedAt = sql.NullTime{Time: now, Valid: true}
	return s.Update(j)
}

// MarkSucceeded marks job complete.
func (s *Store) MarkSucceeded(j *Job) error {
	j.Status = StatusSucceeded
	j.Progress = 100
	now := time.Now().UTC()
	j.FinishedAt = sql.NullTime{Time: now, Valid: true}
	return s.Update(j)
}
