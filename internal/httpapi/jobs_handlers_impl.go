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
		URL     string                 `json:"url"`
		Method  string                 `json:"method"`
		Headers map[string]string      `json:"headers"`
		Body    map[string]interface{} `json:"body"`
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
	// Serialize body
	b, _ := json.Marshal(p.Body)
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

// doCompute simulates a CPU-bound task. We'll run a deterministic small calculation to mimic CPU use.
func DoCompute(ctx context.Context, j *storage.Job, s *storage.Store) error {
	// simulate CPU by performing work in a loop and checking context
	res := 0
	for i := 0; i < 500000; i++ {
		if i%100000 == 0 {
			j.Progress += 20
			s.Update(j)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		res += i % 7
	}
	// store a tiny result into payload (not ideal for large results)
	r := map[string]int{"checksum": res}
	b, _ := json.Marshal(r)
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
