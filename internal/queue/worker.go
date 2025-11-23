package queue

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"strconv"
	"sync"
	"time"

	"go-job-queue/internal/storage"
)

// WorkerPool manages a pool of goroutines that run job handlers concurrently.
type WorkerPool struct {
	jobType string
	size    int
	store   *storage.Store
	handler func(context.Context, *storage.Job) error
	jobs    chan *storage.Job
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewWorkerPool constructs a pool for a given job type and handler.
func NewWorkerPool(jobType string, size int, store *storage.Store, handler func(context.Context, *storage.Job) error) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerPool{
		jobType: jobType,
		size:    size,
		store:   store,
		handler: handler,
		jobs:    make(chan *storage.Job, 100),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start launches worker goroutines.
func (p *WorkerPool) Start() {
	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// Stop signals workers to finish.
func (p *WorkerPool) Stop() {
	p.cancel()
	close(p.jobs)
	p.wg.Wait()
}

// Submit hands a job to the pool.
func (p *WorkerPool) Submit(j *storage.Job) error {
	select {
	case <-p.ctx.Done():
		return errors.New("worker pool is stopping")
	default:
	}

	select {
	case p.jobs <- j:
		return nil
	default:
		return errors.New("worker pool queue is full")
	}
}

// worker executes jobs for this pool.
func (p *WorkerPool) worker(idx int) {
	workerName := p.jobType + "-" + strconv.Itoa(idx)

	defer p.wg.Done()
	log.Printf("[worker %s] starting", workerName)

	for j := range p.jobs {

		// Check for shutdown
		select {
		case <-p.ctx.Done():
			log.Printf("[worker %s] stopping due to context", workerName)
			return
		default:
		}

		// Log job received
		log.Printf(
			"[worker %s] picked up job %d (type=%s)",
			workerName,
			j.ID,
			j.Type,
		)

		startTime := time.Now()

		//Initial heartbeat: mark this job as alive right now
		if err := p.store.UpdateHeartbeat(j.ID); err != nil {
			log.Printf("[heartbeat] failed to update heartbeat for job %d: %v", j.ID, err)
		}

		// Give each job a timeout
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)

		func() {
			defer cancel()

			heartbeatStop := make(chan struct{})
			go p.startHeartbeat(ctx, j, heartbeatStop)
			err := p.handler(ctx, j)
			close(heartbeatStop)

			//Execute handler functions
			if err == nil {
				// Log success
				duration := time.Since(startTime)
				log.Printf(
					"[worker %s] job %d succeeded in %v",
					workerName,
					j.ID,
					duration,
				)
				p.store.MarkSucceeded(j)
				return
			}

			// Log failure
			log.Printf(
				"[worker %s] job %d FAILED: %v (attempt %d/%d)",
				workerName,
				j.ID,
				err,
				j.Attempts+1,
				j.MaxAttempts,
			)

			// If max attempts reached → fail permanently & add to Dead Letter Queue
			if j.Attempts+1 >= j.MaxAttempts {
				p.store.MarkFailed(j, err.Error())
				p.store.AddToDeadLetter(j, err.Error())
				log.Printf("[DLQ] job %d moved to dead-letter queue", j.ID)
				return
			}

			// Retry logic
			backoff := time.Duration(100*(1<<uint(j.Attempts))) * time.Millisecond
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}

			log.Printf(
				"[worker %s] retrying job %d after backoff %v",
				workerName,
				j.ID,
				backoff,
			)

			j.Attempts++
			p.store.Update(j)

			time.Sleep(backoff)
			p.store.Requeue(j)
		}()
	}

	log.Printf("[worker %s] exiting", workerName)
}

// Heartbeats run every 5 seconds while a job is in progress
func (p *WorkerPool) startHeartbeat(ctx context.Context, j *storage.Job, stop chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Update heartbeat timestamp and progress indicator
			ts := time.Now().UTC()
			// Store in job struct
			j.LastHeartbeat = sql.NullTime{
				Time:  ts,
				Valid: true,
			}
			// Persist to DB
			p.store.UpdateHeartbeat(j.ID)
			log.Printf("[heartbeat] job %d alive at %v", j.ID, j.LastHeartbeat)
		}
	}
}
