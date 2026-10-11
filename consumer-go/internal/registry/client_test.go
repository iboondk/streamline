package registry

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

const testSchema = `{"type":"record","name":"Ping","namespace":"test","fields":[{"name":"n","type":"int"}]}`

func TestSchemaCachesByID(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Path != "/schemas/ids/7" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": testSchema})
	}))
	defer srv.Close()

	c := New(srv.URL, srv.Client())
	for i := 0; i < 2; i++ {
		s, err := c.Schema(context.Background(), 7)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if s == nil {
			t.Fatalf("call %d: nil schema", i)
		}
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("server hits = %d, want 1 (second call must be cached)", got)
	}
}

func TestSchemaNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error_code":40403,"message":"Schema not found"}`))
	}))
	defer srv.Close()

	c := New(srv.URL+"/", srv.Client())
	_, err := c.Schema(context.Background(), 99)
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestSchemaServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := New(srv.URL, srv.Client())
	_, err := c.Schema(context.Background(), 1)
	if err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want non-nil error that is not ErrNotFound", err)
	}
}
