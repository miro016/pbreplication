package pbreplication

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestEventRingBufferWrapsAndOrders(t *testing.T) {
	l := newEventLog(4)
	for i := 0; i < 6; i++ {
		l.add(Event{Type: EventNodeJoined, Message: fmt.Sprintf("e%d", i)})
	}

	got := l.list(0)
	if len(got) != 4 {
		t.Fatalf("size = %d, want 4 (capacity)", len(got))
	}
	// newest first: e5, e4, e3, e2
	for i, want := range []string{"e5", "e4", "e3", "e2"} {
		if got[i].Message != want {
			t.Fatalf("list[%d] = %q, want %q", i, got[i].Message, want)
		}
	}

	if got := l.list(2); len(got) != 2 || got[0].Message != "e5" || got[1].Message != "e4" {
		t.Fatalf("limited list wrong: %+v", got)
	}
}

func TestEmitEventFieldsAndAccessor(t *testing.T) {
	_, r := newTestNode(t, "nodeA0000000001")

	r.emitEvent(EventOpFailed, "boom", "peer", "nodeB", "collection", "posts", "error", "conflict")
	evs := r.Events(10)
	if len(evs) == 0 {
		t.Fatal("no events recorded")
	}
	ev := evs[0]
	if ev.Type != EventOpFailed || ev.Peer != "nodeB" || ev.Collection != "posts" {
		t.Fatalf("event fields wrong: %+v", ev)
	}
	if ev.Fields["error"] != "conflict" {
		t.Fatalf("extra field lost: %+v", ev.Fields)
	}
	if ev.Time.IsZero() {
		t.Fatal("event time not stamped")
	}
}

func TestOnEventSubscription(t *testing.T) {
	_, r := newTestNode(t, "nodeA0000000001")

	var mu sync.Mutex
	var seen []Event
	done := make(chan struct{}, 8)
	unsub := r.OnEvent(func(ev Event) {
		mu.Lock()
		seen = append(seen, ev)
		mu.Unlock()
		done <- struct{}{}
	})

	r.emitEvent(EventPeerHealthy, "up", "peer", "nodeB")
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber not invoked")
	}

	unsub()
	r.emitEvent(EventPeerUnhealthy, "down", "peer", "nodeB")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0].Type != EventPeerHealthy {
		t.Fatalf("subscription delivered wrong events: %+v", seen)
	}
}

func TestHealthTransitionEmitsEvent(t *testing.T) {
	_, r := newTestNode(t, "nodeA0000000001")

	fresh := time.Now().UTC().Format(time.RFC3339)
	stale := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)

	peer := &member{NodeID: "nodeB0000000001", URL: "http://b.test", LastSeen: fresh}
	// first observation records the baseline without an event
	r.detectHealthTransitions([]*member{peer})
	if n := len(r.Events(0)); n != 0 {
		t.Fatalf("baseline observation emitted %d events", n)
	}

	// flip to unhealthy
	peer.LastSeen = stale
	r.detectHealthTransitions([]*member{peer})
	evs := r.Events(1)
	if len(evs) != 1 || evs[0].Type != EventPeerUnhealthy || evs[0].Peer != peer.NodeID {
		t.Fatalf("expected unhealthy event, got %+v", evs)
	}

	// stays unhealthy: no duplicate
	r.detectHealthTransitions([]*member{peer})
	if n := len(r.Events(0)); n != 1 {
		t.Fatalf("duplicate transition events: %d", n)
	}

	// recovers
	peer.LastSeen = fresh
	r.detectHealthTransitions([]*member{peer})
	evs = r.Events(1)
	if evs[0].Type != EventPeerHealthy {
		t.Fatalf("expected healthy event, got %+v", evs[0])
	}
}

func TestPeerLagFromVectors(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")

	makeTestCollection(t, app, "posts")
	// makeTestCollection captures a col_upsert op -> local_seq >= 1
	var localSeq int64
	v, _ := getState(app.DB(), stateLocalSeq)
	fmt.Sscanf(v, "%d", &localSeq)
	if localSeq == 0 {
		t.Fatal("expected a captured op to bump local_seq")
	}

	if lag := r.peerLag("nodeB0000000001"); lag != -1 {
		t.Fatalf("unknown peer lag = %d, want -1", lag)
	}

	r.notePeerVector("nodeB0000000001", map[string]int64{r.nodeID: 0})
	if lag := r.peerLag("nodeB0000000001"); lag != localSeq {
		t.Fatalf("lag = %d, want %d", lag, localSeq)
	}

	r.notePeerVector("nodeB0000000001", map[string]int64{r.nodeID: localSeq})
	if lag := r.peerLag("nodeB0000000001"); lag != 0 {
		t.Fatalf("caught-up lag = %d, want 0", lag)
	}
}

func TestThrottleOK(t *testing.T) {
	_, r := newTestNode(t, "nodeA0000000001")
	if !r.throttleOK("k", time.Minute) {
		t.Fatal("first call must pass")
	}
	if r.throttleOK("k", time.Minute) {
		t.Fatal("second call within gap must be throttled")
	}
	if !r.throttleOK("other", time.Minute) {
		t.Fatal("different key must pass")
	}
}
