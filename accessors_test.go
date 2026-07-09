package pbreplication

import (
	"testing"
	"time"
)

func TestAccessorsMembersPeersAndLeader(t *testing.T) {
	// Self id sorts AFTER nodeA but BEFORE nodeZ, so leadership depends on
	// which peers are healthy.
	app, r := newTestNode(t, "nodeM0000000001")
	db := app.NonconcurrentDB()

	// A healthy peer that sorts lower than self -> it should be leader.
	if err := upsertMember(db, &member{
		NodeID:    "nodeA0000000001",
		URL:       "http://nodeA.test:8090",
		Reachable: true,
		LastSeen:  nowStr(),
	}); err != nil {
		t.Fatal(err)
	}
	// A stale peer that sorts even lower -> must be ignored (not healthy).
	if err := upsertMember(db, &member{
		NodeID:    "node00000000001",
		URL:       "http://node0.test:8090",
		Reachable: true,
		LastSeen:  time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatal(err)
	}

	// Members includes self + both peers.
	members := r.Members()
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}
	var self *MemberInfo
	byID := map[string]MemberInfo{}
	for i := range members {
		byID[members[i].NodeID] = members[i]
		if members[i].Self {
			self = &members[i]
		}
	}
	if self == nil || self.NodeID != r.nodeID {
		t.Fatalf("self entry missing or wrong: %+v", self)
	}
	if !byID["nodeA0000000001"].Healthy {
		t.Fatal("fresh peer should be healthy")
	}
	if byID["node00000000001"].Healthy {
		t.Fatal("stale peer should be unhealthy")
	}

	// PeerURLs excludes self and the stale peer.
	peers := r.PeerURLs()
	if len(peers) != 1 {
		t.Fatalf("expected 1 healthy peer URL, got %d: %v", len(peers), peers)
	}
	if peers["nodeA0000000001"] != "http://nodeA.test:8090" {
		t.Fatalf("unexpected peer URL map: %v", peers)
	}

	// Leader is the lowest-id HEALTHY member: nodeA (the stale node0 is skipped).
	if got := r.LeaderID(); got != "nodeA0000000001" {
		t.Fatalf("expected leader nodeA0000000001, got %q", got)
	}
	if r.IsLeader() {
		t.Fatal("self should not be leader while a lower healthy peer exists")
	}
}

func TestAccessorsStandaloneIsLeader(t *testing.T) {
	_, r := newTestNode(t, "solo0000000001")
	if !r.IsLeader() {
		t.Fatal("a lone node must be its own leader")
	}
	if got := r.LeaderID(); got != r.nodeID {
		t.Fatalf("expected leader %q, got %q", r.nodeID, got)
	}
	if len(r.PeerURLs()) != 0 {
		t.Fatal("standalone node should have no peers")
	}
	if r.NodeURL() != "http://solo0000000001.test:8090" {
		t.Fatalf("unexpected NodeURL %q", r.NodeURL())
	}
	// Synced only flips true once the background bootstrap goroutine runs
	// (not started in these unit tests), so it is false right after
	// initStorage.
	if r.Synced() {
		t.Fatal("Synced should be false before the background bootstrap runs")
	}
}
