package sink

import (
	"context"
	"sync"

	"github.com/iboondk/p4-streamline/consumer-go/internal/batch"
)

// MemorySink is an in-memory Sink for tests. It records each accepted batch
// and silently drops a batch whose dedupe token was already seen, mirroring
// ClickHouse's insert deduplication.
type MemorySink struct {
	mu      sync.Mutex
	seen    map[string]struct{}
	batches []batch.Batch
}

// NewMemorySink returns an empty MemorySink.
func NewMemorySink() *MemorySink {
	return &MemorySink{seen: make(map[string]struct{})}
}

// Insert records b unless its token was already stored.
func (m *MemorySink) Insert(_ context.Context, b batch.Batch) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen == nil {
		m.seen = make(map[string]struct{})
	}
	tok := b.DedupToken()
	if _, dup := m.seen[tok]; dup {
		return nil
	}
	m.seen[tok] = struct{}{}
	m.batches = append(m.batches, b)
	return nil
}

// Batches returns a copy of the accepted batches in insert order.
func (m *MemorySink) Batches() []batch.Batch {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]batch.Batch, len(m.batches))
	copy(out, m.batches)
	return out
}

// RowCount returns the total number of rows across accepted batches.
func (m *MemorySink) RowCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, b := range m.batches {
		n += len(b.Rows)
	}
	return n
}
