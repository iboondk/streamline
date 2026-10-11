// Package app turns a poll's worth of Kafka records into ClickHouse inserts
// and dead-letter records.
package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iboondk/streamline/consumer-go/internal/batch"
	"github.com/iboondk/streamline/consumer-go/internal/model"
	"github.com/iboondk/streamline/consumer-go/internal/sink"
)

// DefaultBatchMax is the per-insert record cap used by Process.
const DefaultBatchMax = 1000

// Decoder turns a raw Kafka value into an order event. Returning an error
// marks the record as poison unless the error is a RetryableError.
type Decoder interface {
	Decode(ctx context.Context, b []byte) (model.OrderEvent, error)
}

// DLQ receives records that cannot be processed.
type DLQ interface {
	Send(ctx context.Context, rec *kgo.Record, cause error) error
}

// RetryableError wraps a decode failure that is not the record's fault, such
// as the schema registry being unreachable. Process returns it without
// sending the record to the DLQ, so the caller retries the whole poll.
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string { return "retryable: " + e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// Process is ProcessWithMax with DefaultBatchMax.
func Process(ctx context.Context, recs []*kgo.Record, dec Decoder, s sink.Sink, dlq DLQ) error {
	return ProcessWithMax(ctx, recs, dec, s, dlq, DefaultBatchMax)
}

// ProcessWithMax decodes recs, sends undecodable ones to dlq, and inserts the
// rest in per-partition batches of at most max records.
//
// It returns an error if any DLQ send or insert fails, and the caller must
// then not commit. Re-processing the same records is safe: inserts carry
// dedupe tokens and the DLQ may receive a duplicate.
//
// Known limit: the dedupe token is derived from the first and last offsets of
// each chunk in this poll. A retry after a rewind can fetch a different record
// set, so the chunk boundaries and tokens change and ClickHouse's
// insert_deduplication_token will not catch that re-insert. The
// ReplacingMergeTree (ORDER BY event_id) removes such duplicates eventually,
// but materialized views fed from the raw table (see
// sql/002_materialized_views.sql) can double-count until the merge runs.
// Aligning chunks to offset multiples of max would make retried ranges
// produce identical tokens; that is not implemented yet.
func ProcessWithMax(ctx context.Context, recs []*kgo.Record, dec Decoder, s sink.Sink, dlq DLQ, max int) error {
	rows := make(map[*kgo.Record]model.OrderRow, len(recs))
	valid := make([]*kgo.Record, 0, len(recs))

	for _, rec := range recs {
		ev, err := dec.Decode(ctx, rec.Value)
		if err != nil {
			var retry *RetryableError
			if errors.As(err, &retry) {
				return fmt.Errorf("decode %s/%d@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
			}
			if err := dlq.Send(ctx, rec, err); err != nil {
				return fmt.Errorf("send %s/%d@%d to dlq: %w", rec.Topic, rec.Partition, rec.Offset, err)
			}
			continue
		}
		rows[rec] = model.ToRow(ev, rec.Partition, rec.Offset)
		valid = append(valid, rec)
	}

	for _, chunk := range batch.Split(valid, max) {
		b := batch.Batch{
			Topic:       chunk[0].Topic,
			Partition:   chunk[0].Partition,
			FirstOffset: chunk[0].Offset,
			LastOffset:  chunk[len(chunk)-1].Offset,
			Rows:        make([]model.OrderRow, 0, len(chunk)),
		}
		for _, r := range chunk {
			b.Rows = append(b.Rows, rows[r])
		}
		if err := s.Insert(ctx, b); err != nil {
			return fmt.Errorf("insert %s: %w", b.DedupToken(), err)
		}
	}
	return nil
}
