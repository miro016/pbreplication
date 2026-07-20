package pbreplication

import (
	"errors"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------
// notePeerErr / clearPeerErr

func TestClearPeerErrRetractsGlobalLastError(t *testing.T) {
	_, r := newTestNode(t, "nodeM0000000001")

	r.notePeerErr("nodeA0000000001", errors.New("dial tcp: connection refused"))

	if v, _ := r.memberErrs.Load("nodeA0000000001"); v != "dial tcp: connection refused" {
		t.Fatalf("member error not recorded: %v", v)
	}
	if le := r.LastError(); !strings.Contains(le, "sync with peer nodeA0000000001 failing") {
		t.Fatalf("global last error not recorded: %q", le)
	}

	// Peer recovers -> both the per-member error and the global banner
	// referring to it must disappear.
	r.clearPeerErr("nodeA0000000001")

	if v, _ := r.memberErrs.Load("nodeA0000000001"); v != "" {
		t.Fatalf("member error not cleared: %v", v)
	}
	if le := r.LastError(); le != "" {
		t.Fatalf("global last error should be cleared after rejoin, got %q", le)
	}
}

func TestClearPeerErrKeepsUnrelatedLastError(t *testing.T) {
	_, r := newTestNode(t, "nodeM0000000001")

	// An unrelated error recorded after the peer failure must survive
	// the peer's recovery.
	r.notePeerErr("nodeA0000000001", errors.New("connection refused"))
	r.logError("compaction failed", errors.New("disk full"))

	r.clearPeerErr("nodeA0000000001")

	if le := r.LastError(); !strings.Contains(le, "compaction failed") {
		t.Fatalf("unrelated last error was lost: %q", le)
	}
}

func TestClearPeerErrKeepsOtherPeersLastError(t *testing.T) {
	_, r := newTestNode(t, "nodeM0000000001")

	r.notePeerErr("nodeA0000000001", errors.New("connection refused"))
	r.notePeerErr("nodeB0000000001", errors.New("connection refused"))

	// nodeB's failure is the most recent global error; nodeA recovering
	// must not clear it.
	r.clearPeerErr("nodeA0000000001")

	if le := r.LastError(); !strings.Contains(le, "nodeB0000000001") {
		t.Fatalf("other peer's last error was lost: %q", le)
	}
}
