package pbreplication

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// hlc is a hybrid logical clock. Timestamps are encoded as
// "%016x-%04x" (unix milliseconds + logical counter) so that plain
// lexicographic string comparison matches causal ordering.
type hlc struct {
	mu       sync.Mutex
	physical uint64 // unix ms
	logical  uint16
}

func newHLC() *hlc {
	return &hlc{}
}

func encodeHLC(physical uint64, logical uint16) string {
	return fmt.Sprintf("%016x-%04x", physical, logical)
}

func decodeHLC(s string) (physical uint64, logical uint16, err error) {
	parts := strings.SplitN(s, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid hlc %q", s)
	}
	p, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid hlc physical part %q: %w", s, err)
	}
	l, err := strconv.ParseUint(parts[1], 16, 16)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid hlc logical part %q: %w", s, err)
	}
	return p, uint16(l), nil
}

// Now returns a new timestamp strictly greater than any previously
// issued or observed one.
func (c *hlc) Now() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	wall := uint64(time.Now().UnixMilli())
	if wall > c.physical {
		c.physical = wall
		c.logical = 0
	} else {
		c.logical++
		if c.logical == 0 { // counter overflow
			c.physical++
		}
	}
	return encodeHLC(c.physical, c.logical)
}

// Observe merges a remote timestamp so subsequent Now() calls are
// guaranteed to sort after it, even with wall-clock skew.
func (c *hlc) Observe(remote string) {
	p, l, err := decodeHLC(remote)
	if err != nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if p > c.physical {
		c.physical = p
		c.logical = l
	} else if p == c.physical && l > c.logical {
		c.logical = l
	}
}

// Resume restores the clock from a persisted timestamp on startup.
func (c *hlc) Resume(persisted string) {
	c.Observe(persisted)
}

// Current returns the last issued/observed timestamp without advancing.
func (c *hlc) Current() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return encodeHLC(c.physical, c.logical)
}

// lwwLess reports whether write A loses against write B under
// last-write-wins (HLC first, node id as a deterministic tiebreaker).
func lwwLess(hlcA, nodeA, hlcB, nodeB string) bool {
	if hlcA != hlcB {
		return hlcA < hlcB
	}
	return nodeA < nodeB
}
