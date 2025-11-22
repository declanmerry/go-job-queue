package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite" // Pure Go SQLite driver
)

// ===============================================
// Helpers for retrying locked database operations
// ===============================================

var writeMu sync.Mutex

func withRetry(op func() error) error {
	maxRetries := 5
	backoff := 20 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		err := op()
		if err == nil {
			return nil
		}

		if strings.Contains(err.Error(), "database is locked") {
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		// Not a locking error → return immediately
		return err
	}

	return errors.New("database operation failed after retries")
}

// ===============================================
// Store Definition
// ===============================================

type Store struct {
	db *sql.DB
}

// NewStore opens (or creates) the SQLite database and ensures schema exists.
func NewStore(dsn string) (*Store, error) {
	// Add WAL + busy timeout into SQLite DSN
	dsn = dsn + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

// migrate creates the jobs table if needed.
func (s *Store) migrate() error {
	return withRetry(func() error {
		_, err := s.db.Exec(`
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
			CREATE INDEX IF NOT EXISTS idx_jobs_status_priority
			ON jobs(status, priority DESC, created_at);
		`)
		return err
	})
}

// ================ CRUD Operations ===================

func (s *Store) Enqueue(j *Job) (*Job, error) {
	writeMu.Lock()
	defer writeMu.Unlock()

	if err := j.ValidateBasic(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()

	err := withRetry(func() error {
		res, err := s.db.Exec(`
			INSERT INTO jobs(type, payload, status, progress, attempts, max_attempts, priority, created_at)
			VALUES(?,?,?,?,?,?,?,?)`,
			j.Type, string(j.Payload), string(StatusQueued), 0, 0, j.MaxAttempts, j.Priority, now,
		)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		j.ID = id
		return nil
	})
	if err != nil {
		return nil, err
	}

	j.Status = StatusQueued
	j.Progress = 0
	j.Attempts = 0
	j.CreatedAt = now
	return j, nil
}

func (s *Store) Get(id int64) (*Job, error) {
	var j Job
	var payload string
	var errMsg sql.NullString

	err := withRetry(func() error {
		row := s.db.QueryRow(`
			SELECT id, type, payload, status, progress, attempts,
			       max_attempts, priority, error_message,
			       created_at, started_at, finished_at
			FROM jobs WHERE id = ?`, id)

		return row.Scan(
			&j.ID, &j.Type, &payload, &j.Status, &j.Progress,
			&j.Attempts, &j.MaxAttempts, &j.Priority, &errMsg,
			&j.CreatedAt, &j.StartedAt, &j.FinishedAt,
		)
	})

	if err != nil {
		return nil, err
	}

	j.Payload = []byte(payload)
	j.ErrorMessage = errMsg
	return &j, nil
}

// NextQueued atomically fetches + marks job running
func (s *Store) NextQueued() (*Job, error) {
	writeMu.Lock()
	defer writeMu.Unlock()

	var id int64

	err := withRetry(func() error {
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}

		row := tx.QueryRow(`
			SELECT id FROM jobs
			WHERE status = ?
			ORDER BY priority DESC, created_at ASC
			LIMIT 1`,
			string(StatusQueued),
		)

		if err := row.Scan(&id); err != nil {
			tx.Rollback()
			if err == sql.ErrNoRows {
				return nil // no job available
			}
			return err
		}

		now := time.Now().UTC()

		_, err = tx.Exec(`
			UPDATE jobs SET status = ?, started_at = ?
			WHERE id = ?`, string(StatusRunning), now, id)
		if err != nil {
			tx.Rollback()
			return err
		}

		return tx.Commit()
	})

	if err != nil {
		return nil, err
	}

	if id == 0 {
		return nil, nil
	}

	return s.Get(id)
}

func (s *Store) Update(j *Job) error {
	writeMu.Lock()
	defer writeMu.Unlock()

	return withRetry(func() error {
		_, err := s.db.Exec(`
			UPDATE jobs SET status = ?, progress = ?, attempts = ?,
			       max_attempts = ?, priority = ?, error_message = ?,
			       started_at = ?, finished_at = ?
			WHERE id = ?`,
			string(j.Status), j.Progress, j.Attempts, j.MaxAttempts,
			j.Priority, nullableString(j.ErrorMessage),
			nullableTime(j.StartedAt), nullableTime(j.FinishedAt), j.ID,
		)
		return err
	})
}

func (s *Store) Cancel(id int64) error {
	writeMu.Lock()
	defer writeMu.Unlock()

	return withRetry(func() error {
		res, err := s.db.Exec(`
			UPDATE jobs SET status = ?
			WHERE id = ? AND status IN (?, ?)`,
			string(StatusCancelled), id,
			string(StatusQueued), string(StatusRunning),
		)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return fmt.Errorf("job %d could not be cancelled", id)
		}
		return nil
	})
}

func (s *Store) Requeue(j *Job) error {
	writeMu.Lock()
	defer writeMu.Unlock()

	j.Attempts++
	j.Status = StatusQueued
	j.Progress = 0
	j.ErrorMessage = sql.NullString{}

	return withRetry(func() error {
		_, err := s.db.Exec(`
			UPDATE jobs SET status = ?, progress = ?, attempts = ?, error_message = NULL
			WHERE id = ?`,
			string(StatusQueued), j.Progress, j.Attempts, j.ID,
		)
		return err
	})
}

func (s *Store) MarkFailed(j *Job, msg string) error {
	j.Status = StatusFailed
	j.ErrorMessage = sql.NullString{String: msg, Valid: true}
	now := time.Now().UTC()
	j.FinishedAt = sql.NullTime{Time: now, Valid: true}
	return s.Update(j)
}

func (s *Store) MarkSucceeded(j *Job) error {
	j.Status = StatusSucceeded
	j.Progress = 100
	now := time.Now().UTC()
	j.FinishedAt = sql.NullTime{Time: now, Valid: true}
	return s.Update(j)
}

func (s *Store) RequeueRunningJobs() error {
	_, err := s.db.Exec(`
        UPDATE jobs
        SET status = 'queued'
        WHERE status = 'running'
    `)
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

func (s *Store) Close() error {
	return s.db.Close()
}
