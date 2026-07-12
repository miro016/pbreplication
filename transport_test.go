package pbreplication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/router"
)

func newAuthedRequestEvent(t *testing.T, r *Replicator, method, target string, body []byte) *core.RequestEvent {
	t.Helper()
	var rd io.Reader
	if body != nil {
		rd = strings.NewReader(string(body))
	}
	req := httptest.NewRequest(method, target, rd)
	r.signRequest(req, body)
	e := &core.RequestEvent{}
	e.Response = httptest.NewRecorder()
	e.Request = req
	return e
}

func TestAuthSkipsBodylessRead(t *testing.T) {
	_, r := newTestNode(t, "nodeA0000000001")

	e := newAuthedRequestEvent(t, r, http.MethodGet, "http://peer/api/replication/ping", nil)
	// a GET carries http.NoBody; the middleware must not consume or
	// replace it and must still authenticate (empty-body hash)
	if err := r.requireClusterAuth(e); err != nil {
		t.Fatalf("body-less GET rejected: %v", err)
	}
	if got, _ := e.Get(ctxCallerNodeID).(string); got != r.nodeID {
		t.Fatalf("caller node id = %q, want %q", got, r.nodeID)
	}
}

func TestAuthBufferedBodyStillReadable(t *testing.T) {
	_, r := newTestNode(t, "nodeA0000000001")

	body := []byte(`{"x":1}`)
	e := newAuthedRequestEvent(t, r, http.MethodPost, "http://peer/api/replication/ops", body)
	if err := r.requireClusterAuth(e); err != nil {
		t.Fatalf("valid POST rejected: %v", err)
	}
	got, err := io.ReadAll(e.Request.Body)
	if err != nil || string(got) != string(body) {
		t.Fatalf("handler can't re-read body: %q err=%v", got, err)
	}
}

func TestAuthRejectsOversizedBody(t *testing.T) {
	_, r := newTestNode(t, "nodeA0000000001")
	r.cfg.MaxBodyBytes = 16

	body := []byte(strings.Repeat("a", 64))
	e := newAuthedRequestEvent(t, r, http.MethodPost, "http://peer/api/replication/ops", body)
	err := r.requireClusterAuth(e)
	if err == nil {
		t.Fatal("oversized body accepted")
	}
	var apiErr *router.ApiError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 ApiError, got %v", err)
	}

	// also when Content-Length lies low but the actual body is larger
	e2 := newAuthedRequestEvent(t, r, http.MethodPost, "http://peer/api/replication/ops", body)
	e2.Request.ContentLength = -1
	if err := r.requireClusterAuth(e2); err == nil {
		t.Fatal("oversized chunked body accepted")
	}
}

func TestWithRetryBackoffAndPermanent(t *testing.T) {
	_, r := newTestNode(t, "nodeA0000000001")

	// succeeds on the 3rd attempt
	n := 0
	err := r.withRetry(context.Background(), 5, time.Millisecond, func() error {
		n++
		if n < 3 {
			return fmt.Errorf("transient %d", n)
		}
		return nil
	})
	if err != nil || n != 3 {
		t.Fatalf("retry: err=%v attempts=%d", err, n)
	}

	// permanent errors abort immediately
	n = 0
	err = r.withRetry(context.Background(), 5, time.Millisecond, func() error {
		n++
		return fmt.Errorf("nope: %w", errPermanent)
	})
	if err == nil || n != 1 {
		t.Fatalf("permanent: err=%v attempts=%d", err, n)
	}

	// cancelled context stops retrying
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n = 0
	_ = r.withRetry(ctx, 5, 10*time.Millisecond, func() error {
		n++
		return errors.New("transient")
	})
	if n != 1 {
		t.Fatalf("cancelled ctx: attempts=%d, want 1", n)
	}
}

func TestOpenPeerStreamOffsetFallback(t *testing.T) {
	// a server that ignores Range must have the prefix skipped client-side;
	// a server that honors it returns 206 and the body is used as-is
	payload := []byte("0123456789abcdef")
	honorRange := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if honorRange && req.Header.Get("Range") != "" {
			w.WriteHeader(http.StatusPartialContent)
			w.Write(payload[10:])
			return
		}
		w.Write(payload)
	}))
	defer srv.Close()

	_, r := newTestNode(t, "nodeA0000000001")

	for _, honor := range []bool{false, true} {
		honorRange = honor
		rc, err := r.openPeerStreamCtx(context.Background(), srv.URL, "/f", 10)
		if err != nil {
			t.Fatalf("honor=%v: %v", honor, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if string(got) != "abcdef" {
			t.Fatalf("honor=%v: got %q, want remainder %q", honor, got, "abcdef")
		}
	}
}

func TestCallPeerStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.NotFound(w, req)
	}))
	defer srv.Close()

	_, r := newTestNode(t, "nodeA0000000001")
	err := r.callPeer(srv.URL, http.MethodPost, "/api/replication/snapshot/db", nil, nil)
	if httpStatus(err) != http.StatusNotFound {
		t.Fatalf("expected status 404 in error, got %v (status %d)", err, httpStatus(err))
	}
}
