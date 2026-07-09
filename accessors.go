package pbreplication

// This file exposes a small read-only API over the running replication
// engine so a host application can integrate with the cluster: discover
// peers, gate singleton work on a deterministic leader, and report node
// health. Everything here is derived from the members table plus live
// health, so it stays correct as membership changes.

// MemberInfo is a read-only snapshot of one cluster member.
type MemberInfo struct {
	// NodeID is the member's persistent node id.
	NodeID string
	// URL is the base URL this node uses to reach the member (a learned
	// override when the advertised URL isn't reachable from here,
	// otherwise the advertised URL). Empty for URL-less pull-only nodes.
	URL string
	// Reachable reports whether the member advertises a usable URL.
	Reachable bool
	// Healthy reports whether the member was seen recently enough to be
	// considered live (self is always healthy).
	Healthy bool
	// Self marks this node's own entry.
	Self bool
	// LastSeen is the RFC3339 timestamp of the last contact.
	LastSeen string
}

// NodeURL returns this node's advertised base URL (Config.NodeURL).
func (r *Replicator) NodeURL() string { return r.cfg.NodeURL }

// Ready reports whether the node has finished its initial bootstrap and
// is actively capturing and applying replicated writes.
func (r *Replicator) Ready() bool { return r.ready.Load() }

// memberURL returns the URL this node actually uses to reach the given
// member: a learned override when the advertised URL isn't reachable
// from here, otherwise the advertised URL.
func (r *Replicator) memberURL(m *member) string {
	if ov, ok := r.urlOverrides.Load(m.NodeID); ok {
		if s, _ := ov.(string); s != "" {
			return s
		}
	}
	return m.URL
}

// Members returns every known cluster member (self included), each with
// its current health computed the same way the dashboard reports it.
// Returns nil if the members table can't be read.
func (r *Replicator) Members() []MemberInfo {
	ms, err := listMembers(r.app.DB(), false)
	if err != nil {
		return nil
	}
	out := make([]MemberInfo, 0, len(ms))
	for _, m := range ms {
		out = append(out, MemberInfo{
			NodeID:    m.NodeID,
			URL:       r.memberURL(m),
			Reachable: m.Reachable,
			Healthy:   r.isHealthy(m),
			Self:      m.NodeID == r.nodeID,
			LastSeen:  m.LastSeen,
		})
	}
	return out
}

// PeerURLs returns a nodeID→URL map of every healthy peer (self
// excluded) that advertises a usable URL. Empty in standalone mode.
func (r *Replicator) PeerURLs() map[string]string {
	out := map[string]string{}
	ms, err := listMembers(r.app.DB(), false)
	if err != nil {
		return out
	}
	for _, m := range ms {
		if m.NodeID == r.nodeID || !r.isHealthy(m) {
			continue
		}
		if url := r.memberURL(m); url != "" {
			out[m.NodeID] = url
		}
	}
	return out
}

// LeaderID returns the deterministic cluster leader: the
// lexicographically lowest node id among currently healthy members
// (self included). Returns this node's id when it is the only known
// member. Because the rule is a pure function of the live membership,
// every node independently agrees on the same leader.
func (r *Replicator) LeaderID() string {
	leader := r.nodeID
	ms, err := listMembers(r.app.DB(), false)
	if err != nil {
		return leader
	}
	for _, m := range ms {
		if !r.isHealthy(m) {
			continue
		}
		if leader == "" || m.NodeID < leader {
			leader = m.NodeID
		}
	}
	return leader
}

// IsLeader reports whether this node is the current cluster leader.
// A standalone node (no healthy peers) is always the leader.
func (r *Replicator) IsLeader() bool {
	return r.LeaderID() == r.nodeID
}
