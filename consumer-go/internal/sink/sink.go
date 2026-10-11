// Package sink writes batches to storage. Each batch carries a dedupe token so
// that a retried batch is dropped by the storage layer.
package sink

import (
	"context"

	"github.com/iboondk/streamline/consumer-go/internal/batch"
)

// Sink stores one batch. Implementations must treat a batch whose DedupToken
// was already stored as success.
type Sink interface {
	Insert(ctx context.Context, b batch.Batch) error
}
