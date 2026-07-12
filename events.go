package pbreplication

import (
	"sync"
	"time"
)

// EventType classifies a replication event.
type EventType string

const (
	EventNodeJoined      EventType = "node_joined"
	EventNodeRemoved     EventType = "node_removed"
	EventPeerHealthy     EventType = "peer_healthy"
	EventPeerUnhealthy   EventType = "peer_unhealthy"
	EventSyncStarted     EventType = "sync_started"
	EventSyncFinished    EventType = "sync_finished"
	EventSnapshotStarted  EventType = "snapshot_started"
	EventSnapshotFinished EventType = "snapshot_finished"
	EventCopyStarted     EventType = "copy_started"
	EventCopyFinished    EventType = "copy_finished"
	EventOpFailed        EventType = "op_failed"
	EventMigrationRun    EventType = "migration_run"
	EventFirewallBlock   EventType = "firewall_block"
	EventIntegrityReport EventType = "integrity_report"
)

// Event is one entry of the replication event timeline. Events are held
// in a fixed-size in-memory ring buffer (Config.EventBufferSize) and
// exposed via (*Replicator).Events, OnEvent subscriptions, the
// /api/replication/events endpoint and the dashboard.
type Event struct {
	Time       time.Time      `json:"time"`
	Type       EventType      `json:"type"`
	Peer       string         `json:"peer,omitempty"`
	Collection string         `json:"collection,omitempty"`
	Message    string         `json:"message"`
	Fields     map[string]any `json:"fields,omitempty"`
}

// eventLog is a fixed-capacity ring buffer of events with subscriber
// fan-out.
type eventLog struct {
	mu      sync.Mutex
	buf     []Event
	head    int // next write position
	size    int // number of valid entries
	subs    map[int]func(Event)
	nextSub int
}

func newEventLog(capacity int) *eventLog {
	if capacity <= 0 {
		capacity = 512
	}
	return &eventLog{
		buf:  make([]Event, capacity),
		subs: map[int]func(Event){},
	}
}

func (l *eventLog) add(ev Event) {
	l.mu.Lock()
	l.buf[l.head] = ev
	l.head = (l.head + 1) % len(l.buf)
	if l.size < len(l.buf) {
		l.size++
	}
	subs := make([]func(Event), 0, len(l.subs))
	for _, fn := range l.subs {
		subs = append(subs, fn)
	}
	l.mu.Unlock()

	// fan out outside the lock; a subscriber must never block or
	// deadlock the emitter
	for _, fn := range subs {
		go func(fn func(Event)) {
			defer func() { _ = recover() }()
			fn(ev)
		}(fn)
	}
}

// list returns up to limit events, newest first. limit <= 0 means all.
func (l *eventLog) list(limit int) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := l.size
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]Event, 0, n)
	for i := 0; i < n; i++ {
		idx := (l.head - 1 - i + 2*len(l.buf)) % len(l.buf)
		out = append(out, l.buf[idx])
	}
	return out
}

func (l *eventLog) subscribe(fn func(Event)) (unsubscribe func()) {
	l.mu.Lock()
	id := l.nextSub
	l.nextSub++
	l.subs[id] = fn
	l.mu.Unlock()
	return func() {
		l.mu.Lock()
		delete(l.subs, id)
		l.mu.Unlock()
	}
}

// emitEvent appends an event to the ring buffer. kv are alternating
// key/value pairs stored in Event.Fields; the reserved keys "peer" and
// "collection" populate the dedicated struct fields instead.
func (r *Replicator) emitEvent(t EventType, msg string, kv ...any) {
	ev := Event{Time: time.Now(), Type: t, Message: msg}
	for i := 0; i+1 < len(kv); i += 2 {
		key, _ := kv[i].(string)
		switch key {
		case "peer":
			ev.Peer, _ = kv[i+1].(string)
		case "collection":
			ev.Collection, _ = kv[i+1].(string)
		default:
			if ev.Fields == nil {
				ev.Fields = map[string]any{}
			}
			ev.Fields[key] = kv[i+1]
		}
	}
	r.events.add(ev)
}

// Events returns up to limit events from the in-memory replication
// event timeline, newest first. limit <= 0 returns everything buffered.
func (r *Replicator) Events(limit int) []Event {
	return r.events.list(limit)
}

// OnEvent registers a callback invoked (on its own goroutine) for every
// new replication event. The returned function unsubscribes it.
func (r *Replicator) OnEvent(fn func(Event)) (unsubscribe func()) {
	return r.events.subscribe(fn)
}
