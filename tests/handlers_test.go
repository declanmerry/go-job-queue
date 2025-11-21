package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"go-job-queue/internal/httpapi"
	"go-job-queue/internal/storage"
)

func TestCreateAndGetJob(t *testing.T) {
	// use a temporary sqlite file
	os.Remove("test.db")
	s, err := storage.NewStore("test.db")
	if err != nil {
		t.Fatalf("store init: %v", err)
	}
	h := &httpapi.Handler{Store: s}
	r := httpapi.NewRouter(h)

	// create job
	payload := map[string]interface{}{"url": "https://httpbin.org/status/200"}
	pbytes, _ := json.Marshal(payload)
	job := map[string]interface{}{"type": "http_webhook", "payload": json.RawMessage(pbytes)}
	jb, _ := json.Marshal(job)
	req := httptest.NewRequest("POST", "/jobs", bytes.NewReader(jb))
	rw := httptest.NewRecorder()
	r.ServeHTTP(rw, req)
	if rw.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rw.Code)
	}
	var out storage.Job
	json.NewDecoder(rw.Body).Decode(&out)
	if out.ID == 0 {
		t.Fatalf("expected job id")
	}

	// get job
	req2 := httptest.NewRequest("GET", "/jobs/"+strconv.FormatInt(out.ID, 10), nil)
	rw2 := httptest.NewRecorder()
	r.ServeHTTP(rw2, req2)
	if rw2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw2.Code)
	}
}
