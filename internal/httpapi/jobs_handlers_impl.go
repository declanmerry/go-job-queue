package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"go-job-queue/internal/storage"
)

// Below are simple implementations for job handler functions.
// In real projects these would contain the actual logic such as making outbound HTTP requests, processing images, etc.

// doWebhook performs an HTTP POST to the URL specified in the job payload. It demonstrates an IO-bound job.
func DoWebhook(ctx context.Context, j *storage.Job, s *storage.Store) error {
	// parse payload
	var p struct {
		URL     string            `json:"url"`
		Method  string            `json:"method"`
		Headers map[string]string `json:"headers"`
		Body    interface{}       `json:"body"`
	}
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return err
	}
	if p.URL == "" {
		return errors.New("missing url")
	}
	method := p.Method
	if method == "" {
		method = "POST"
	}
	// ---- NORMALISE BODY ----
	//changed because different body types were failing to unmarshall
	var b []byte
	switch typed := p.Body.(type) {

	case string:
		// If user gives: "body": "hello"
		// Convert into JSON object: { "message": "hello" }
		b, _ = json.Marshal(map[string]interface{}{
			"message": typed,
		})

	case map[string]interface{}:
		// Already correct: "body": { "a": "b" }
		b, _ = json.Marshal(typed)

	case nil:
		// No body provided
		b = []byte("{}")

	default:
		// Unsupported type (e.g. number, bool)
		return errors.New("unsupported body type")
	}

	// Build HTTP request with context
	req, err := http.NewRequestWithContext(ctx, method, p.URL, io.NopCloser(bytesReader(b)))
	if err != nil {
		return err
	}
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}
	// We use a short client timeout to avoid long hangs
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// treat non-2xx as error to allow retries
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New("non-2xx response from target")
	}
	// update progress to 100 and persist
	j.Progress = 100
	s.MarkSucceeded(j)
	return nil
}

// doImageResize simulates an IO + CPU job that would fetch an image and produce derived sizes.
func DoImageResize(ctx context.Context, j *storage.Job, s *storage.Store) error {
	// simulate work by sleeping and updating progress
	for i := 1; i <= 5; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		time.Sleep(400 * time.Millisecond)
		j.Progress = i * 20
		s.Update(j)
	}
	return nil
}

// doCompute simulates a long CPU task with progress + heartbeat
func DoCompute(ctx context.Context, j *storage.Job, s *storage.Store) error {

	var p struct {
		N int `json:"n"`
	}
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return err
	}
	if p.N <= 0 {
		p.N = 20 // default workload
	}

	totalSteps := p.N
	res := 0

	for step := 1; step <= totalSteps; step++ {

		// Light CPU work
		for x := 0; x < 2_000_000; x++ { // ~2M ops (fast)
			res += x % 13
		}

		// Make progress SLOW enough to see heartbeats
		time.Sleep(200 * time.Millisecond)

		// Check cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// PROGRESS
		j.Progress = (step * 100) / totalSteps
		if err := s.Update(j); err != nil {
			return err
		}

		// HEARTBEAT
		if err := s.UpdateHeartbeat(j.ID); err != nil {
			return err
		}
	}

	// Store result
	output := map[string]int{"checksum": res}
	b, _ := json.Marshal(output)
	j.Payload = b

	return nil
}

// small helper to convert bytes to Reader
func bytesReader(b []byte) *bytesReaderImpl { return &bytesReaderImpl{b: b} }

type bytesReaderImpl struct {
	b []byte
	i int
}

func (r *bytesReaderImpl) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
