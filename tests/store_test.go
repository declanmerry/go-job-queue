package tests

import (
	"os"
	"testing"

	"go-job-queue/internal/storage"
)

func TestEnqueueGet(t *testing.T) {
	os.Remove("s_test.db")
	s, err := storage.NewStore("s_test.db")
	if err != nil {
		t.Fatalf("store init: %v", err)
	}
	j := &storage.Job{Type: "compute_task", Payload: []byte(`{"n":10}`)}
	if _, err := s.Enqueue(j); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if j.ID == 0 {
		t.Fatalf("expected id")
	}
	if got, err := s.Get(j.ID); err != nil || got.Type != j.Type {
		t.Fatalf("get mismatch: %v %v", got, err)
	}
}
