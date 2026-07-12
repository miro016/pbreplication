package pbreplication

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pocketbase/pocketbase/core"
)

const (
	authScheme  = "PBR"
	maxClockDev = 5 * time.Minute
)

// signPayload computes the request MAC.
func signPayload(secret, nodeID, ts, method, path string, body []byte) string {
	bodyHash := sha256.Sum256(body)
	msg := strings.Join([]string{nodeID, ts, method, path, hex.EncodeToString(bodyHash[:])}, ".")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(msg))
	return hex.EncodeToString(mac.Sum(nil))
}

// signRequest sets the replication Authorization header.
func (r *Replicator) signRequest(req *http.Request, body []byte) {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	mac := signPayload(r.cfg.ClusterSecret, r.nodeID, ts, req.Method, req.URL.Path, body)
	req.Header.Set("Authorization", fmt.Sprintf("%s %s.%s.%s", authScheme, r.nodeID, ts, mac))
}

// verifyAuth validates the replication Authorization header and returns
// the calling node's id.
func (r *Replicator) verifyAuth(req *http.Request, body []byte) (string, error) {
	header := req.Header.Get("Authorization")
	if !strings.HasPrefix(header, authScheme+" ") {
		return "", fmt.Errorf("missing %s authorization", authScheme)
	}
	parts := strings.SplitN(strings.TrimPrefix(header, authScheme+" "), ".", 3)
	if len(parts) != 3 {
		return "", fmt.Errorf("malformed authorization header")
	}
	nodeID, tsStr, gotMAC := parts[0], parts[1], parts[2]

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return "", fmt.Errorf("malformed timestamp")
	}
	dev := time.Since(time.Unix(ts, 0))
	if dev > maxClockDev || dev < -maxClockDev {
		return "", fmt.Errorf("stale request timestamp")
	}

	want := signPayload(r.cfg.ClusterSecret, nodeID, tsStr, req.Method, req.URL.Path, body)
	if !hmac.Equal([]byte(want), []byte(gotMAC)) {
		return "", fmt.Errorf("invalid signature")
	}
	return nodeID, nil
}

// requireClusterAuth is a middleware for the node-to-node endpoints. It
// verifies the HMAC (buffering the body so handlers can still read it)
// and refreshes the caller's membership liveness. Body-less requests
// (all the GETs) skip the buffering entirely; bodies larger than
// cfg.MaxBodyBytes are rejected instead of buffered.
func (r *Replicator) requireClusterAuth(e *core.RequestEvent) error {
	var body []byte
	if e.Request.Body != nil && e.Request.Body != http.NoBody && e.Request.ContentLength != 0 {
		if e.Request.ContentLength > r.cfg.MaxBodyBytes {
			return e.Error(http.StatusRequestEntityTooLarge, "request body too large", nil)
		}
		var err error
		body, err = io.ReadAll(io.LimitReader(e.Request.Body, r.cfg.MaxBodyBytes+1))
		if err != nil {
			return e.BadRequestError("failed to read request body", nil)
		}
		if int64(len(body)) > r.cfg.MaxBodyBytes {
			return e.Error(http.StatusRequestEntityTooLarge, "request body too large", nil)
		}
		e.Request.Body = io.NopCloser(bytes.NewReader(body))
	}

	nodeID, err := r.verifyAuth(e.Request, body)
	if err != nil {
		return e.UnauthorizedError("cluster authentication failed", nil)
	}
	if nodeID != r.nodeID {
		_ = touchMember(r.app.NonconcurrentDB(), nodeID)
	}
	e.Set(ctxCallerNodeID, nodeID)
	return e.Next()
}

const ctxCallerNodeID = "pbreplicationCallerNodeID"

// statusError carries the HTTP status of a failed peer call so callers
// can distinguish e.g. "old peer without this endpoint" (404) from
// transient network failures.
type statusError struct {
	status int
	msg    string
}

func (e *statusError) Error() string { return e.msg }

// httpStatus extracts the HTTP status from a peer-call error, or 0.
func httpStatus(err error) int {
	var se *statusError
	if errors.As(err, &se) {
		return se.status
	}
	return 0
}

// callPeer performs a signed JSON request against another node with the
// default request timeout.
func (r *Replicator) callPeer(baseURL, method, path string, reqBody any, respDst any) error {
	ctx, cancel := context.WithTimeout(r.runCtx, r.cfg.RequestTimeout)
	defer cancel()
	return r.callPeerCtx(ctx, baseURL, method, path, reqBody, respDst)
}

// callPeerCtx performs a signed JSON request against another node,
// bounded by the given context.
func (r *Replicator) callPeerCtx(ctx context.Context, baseURL, method, path string, reqBody any, respDst any) error {
	var body []byte
	if reqBody != nil {
		var err error
		body, err = json.Marshal(reqBody)
		if err != nil {
			return err
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(baseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	r.signRequest(req, body)

	resp, err := r.jsonClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		limited, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &statusError{
			status: resp.StatusCode,
			msg:    fmt.Sprintf("%s %s%s: status %d: %s", method, baseURL, path, resp.StatusCode, string(limited)),
		}
	}
	if respDst != nil {
		return json.NewDecoder(resp.Body).Decode(respDst)
	}
	return nil
}

// openPeerStream performs a signed GET returning the raw body stream.
func (r *Replicator) openPeerStream(baseURL, path string) (io.ReadCloser, error) {
	return r.openPeerStreamCtx(r.runCtx, baseURL, path, 0)
}

// openPeerStreamCtx performs a signed GET returning the raw body
// stream. A positive offset requests the remainder of the resource via
// a Range header (servers that ignore it simply return the full body
// with status 200 instead of 206). The stream is NOT subject to
// RequestTimeout; cancel ctx to abort it.
func (r *Replicator) openPeerStreamCtx(ctx context.Context, baseURL, path string, offset int64) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	r.signRequest(req, nil)

	resp, err := r.streamClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		return nil, &statusError{
			status: resp.StatusCode,
			msg:    fmt.Sprintf("GET %s%s: status %d", baseURL, path, resp.StatusCode),
		}
	}
	if offset > 0 && resp.StatusCode != http.StatusPartialContent {
		// server ignored the Range header; caller expects the remainder
		if _, err := io.CopyN(io.Discard, resp.Body, offset); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("skipping %d already-fetched bytes: %w", offset, err)
		}
	}
	return resp.Body, nil
}

// withRetry runs fn with exponential backoff and jitter until it
// succeeds, attempts are exhausted, or the context/shutdown aborts.
// Non-transient failures can opt out by returning an error wrapped in
// errPermanent.
func (r *Replicator) withRetry(ctx context.Context, attempts int, base time.Duration, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if errors.Is(err, errPermanent) || i == attempts-1 {
			return err
		}
		delay := base << i
		delay += time.Duration(rand.Int64N(int64(delay)/2 + 1))
		select {
		case <-ctx.Done():
			return err
		case <-r.stopCh:
			return err
		case <-time.After(delay):
		}
	}
	return err
}

// errPermanent marks an error withRetry should not retry.
var errPermanent = errors.New("permanent")
