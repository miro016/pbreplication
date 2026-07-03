package pbreplication

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
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
// and refreshes the caller's membership liveness.
func (r *Replicator) requireClusterAuth(e *core.RequestEvent) error {
	var body []byte
	if e.Request.Body != nil {
		var err error
		body, err = io.ReadAll(io.LimitReader(e.Request.Body, 1<<28)) // 256MB cap
		if err != nil {
			return e.BadRequestError("failed to read request body", nil)
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

// callPeer performs a signed JSON request against another node.
func (r *Replicator) callPeer(baseURL, method, path string, reqBody any, respDst any) error {
	var body []byte
	if reqBody != nil {
		var err error
		body, err = json.Marshal(reqBody)
		if err != nil {
			return err
		}
	}

	req, err := http.NewRequest(method, strings.TrimRight(baseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	r.signRequest(req, body)

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		limited, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s%s: status %d: %s", method, baseURL, path, resp.StatusCode, string(limited))
	}
	if respDst != nil {
		return json.NewDecoder(resp.Body).Decode(respDst)
	}
	return nil
}

// openPeerStream performs a signed GET returning the raw body stream.
func (r *Replicator) openPeerStream(baseURL, path string) (io.ReadCloser, error) {
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(baseURL, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	r.signRequest(req, nil)

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("GET %s%s: status %d", baseURL, path, resp.StatusCode)
	}
	return resp.Body, nil
}
