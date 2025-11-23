package queue

import (
	"context"
	"log"
	"math"
	"time"

	"go-job-queue/internal/httpapi"
	"go-job-queue/internal/storage"
)

// Dispatcher coordinates queued job handling.
type Dispatcher struct {
	store        *storage.Store
	pools        map[string]*WorkerPool
	pollInterval time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewDispatcher creates a dispatcher with default worker pools.
func NewDispatcher(store *storage.Store) *Dispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	d := &Dispatcher{
		store:        store,
		pools:        make(map[string]*WorkerPool),
		pollInterval: 500 * time.Millisecond,
		ctx:          ctx,
		cancel:       cancel,
	}

	// Default worker pools
	d.pools["http_webhook"] = NewWorkerPool("http_webhook", 5, store, d.handleJob)
	d.pools["image_resize"] = NewWorkerPool("image_resize", 3, store, d.handleJob)
	d.pools["compute_task"] = NewWorkerPool("compute_task", 2, store, d.handleJob)

	return d
}

// Start launches all worker pools and begins dispatch loop.
func (d *Dispatcher) Start() {
	log.Println("[dispatcher] starting worker pools")

	for name, p := range d.pools {
		log.Printf("[dispatcher] starting pool: %s (size=%d)", name, p.size)
		p.Start()

	}

	go d.loop()
}

// Stop shuts down all pools.
func (d *Dispatcher) Stop() {
	log.Println("[dispatcher] stopping dispatcher and pools")
	d.cancel()

	for name, p := range d.pools {
		log.Printf("[dispatcher] stopping pool: %s", name)
		p.Stop()
	}
}

// Core dispatch loop.
func (d *Dispatcher) loop() {
	log.Println("[dispatcher] loop started")
	backoff := 0

	for {
		select {
		case <-d.ctx.Done():
			log.Println("[dispatcher] stopping")
			return
		default:
		}

		// Try to fetch next job
		job, err := d.store.NextQueued()
		if err != nil {
			log.Printf("[dispatcher] error fetching job: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if job == nil {
			// backoff when no jobs are ready
			backoff++
			sleep := time.Duration(math.Min(5, math.Pow(2, float64(backoff)))) * d.pollInterval
			if sleep > 5*time.Second {
				sleep = 5 * time.Second
			}

			log.Printf("[dispatcher] no jobs available, backing off for %v", sleep)
			time.Sleep(sleep)
			continue
		}

		// reset backoff on successful fetch
		backoff = 0

		log.Printf(
			"[dispatcher] fetched job %d (type=%s), dispatching...",
			job.ID,
			job.Type,
		)

		pool, ok := d.pools[job.Type]
		if !ok {
			log.Printf("[dispatcher] unknown job type: %s", job.Type)
			d.store.MarkFailed(job, "unknown job type")
			continue
		}

		err = pool.Submit(job)
		if err != nil {
			log.Printf(
				"[dispatcher] pool for type %s full — requeueing job %d (%v)",
				job.Type,
				job.ID,
				err,
			)

			time.Sleep(100 * time.Millisecond)

			if err := d.store.Requeue(job); err != nil {
				log.Printf("[dispatcher] failed to requeue job %d: %v", job.ID, err)
			}
		}
	}
}

// Job handler routing based on job type.
func (d *Dispatcher) handleJob(ctx context.Context, j *storage.Job) error {
	switch j.Type {
	case "http_webhook":
		return httpapi.DoWebhook(ctx, j, d.store)
	case "image_resize":
		return httpapi.DoImageResize(ctx, j, d.store)
	case "compute_task":
		return httpapi.DoCompute(ctx, j, d.store)
	default:
		log.Printf("[dispatcher] unsupported job type: %s", j.Type)
		return d.store.MarkFailed(j, "unsupported job type")
	}
}
