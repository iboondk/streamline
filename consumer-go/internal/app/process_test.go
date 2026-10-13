package app

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iboondk/p4-streamline/consumer-go/internal/batch"
	"github.com/iboondk/p4-streamline/consumer-go/internal/model"
	"github.com/iboondk/p4-streamline/consumer-go/internal/sink"
)

// fakeDecoder treats values starting with "bad" as poison and decodes the
// rest into an event whose EventID is the value and whose line items total
// one unit.
type fakeDecoder struct{}

func (fakeDecoder) Decode(_ context.Context, b []byte) (model.OrderEvent, error) {
	if len(b) >= 3 && string(b[:3]) == "bad" {
		return model.OrderEvent{}, errors.New("undecodable")
	}
	return model.OrderEvent{
		EventID:   string(b),
		EventType: "ORDER_CREATED",
		LineItems: []model.LineItem{{SKU: "s", Quantity: 1}},
	}, nil
}

type fakeDLQ struct {
	sent []*kgo.Record
	err  error
}

func (f *fakeDLQ) Send(_ context.Context, rec *kgo.Record, _ error) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, rec)
	return nil
}

type failingSink struct{}

func (failingSink) Insert(context.Context, batch.Batch) error {
	return errors.New("clickhouse unavailable")
}

func recs(topic string, part int32, vals ...string) []*kgo.Record {
	out := make([]*kgo.Record, 0, len(vals))
	for i, v := range vals {
		out = append(out, &kgo.Record{
			Topic: topic, Partition: part, Offset: int64(100 + i), Value: []byte(v),
		})
	}
	return out
}

func TestPoisonGoesToDLQAndOthersInserted(t *testing.T) {
	in := append(recs("shop.orders.v1", 0, "ord-1", "bad-2"), recs("shop.orders.v1", 1, "ord-3")...)
	dlq := &fakeDLQ{}
	ms := sink.NewMemorySink()

	if err := Process(context.Background(), in, fakeDecoder{}, ms, dlq); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(dlq.sent) != 1 || string(dlq.sent[0].Value) != "bad-2" {
		t.Fatalf("dlq got %d records, want the single poison record", len(dlq.sent))
	}
	if ms.RowCount() != 2 {
		t.Fatalf("rows inserted = %d, want 2", ms.RowCount())
	}
	for _, b := range ms.Batches() {
		for _, r := range b.Rows {
			if r.EventID == "bad-2" {
				t.Fatal("poison record leaked into a batch")
			}
		}
	}
}

func TestInsertFailureReturnsError(t *testing.T) {
	in := recs("shop.orders.v1", 0, "ord-1", "ord-2")
	err := Process(context.Background(), in, fakeDecoder{}, failingSink{}, &fakeDLQ{})
	if err == nil {
		t.Fatal("expected error when insert fails")
	}
}

func TestDLQFailureReturnsError(t *testing.T) {
	in := recs("shop.orders.v1", 0, "bad-1")
	err := Process(context.Background(), in, fakeDecoder{}, sink.NewMemorySink(),
		&fakeDLQ{err: errors.New("broker down")})
	if err == nil {
		t.Fatal("expected error when DLQ send fails")
	}
}

func TestRetryableDecodeErrorSkipsDLQ(t *testing.T) {
	dec := retryDecoder{}
	dlq := &fakeDLQ{}
	err := Process(context.Background(), recs("shop.orders.v1", 0, "x"), dec, sink.NewMemorySink(), dlq)
	if err == nil {
		t.Fatal("expected error for retryable decode failure")
	}
	if len(dlq.sent) != 0 {
		t.Fatalf("retryable failure must not reach the DLQ, got %d", len(dlq.sent))
	}
}

type retryDecoder struct{}

func (retryDecoder) Decode(context.Context, []byte) (model.OrderEvent, error) {
	return model.OrderEvent{}, &RetryableError{Err: errors.New("registry timeout")}
}

func TestReprocessingSameRecordsStoresEachTokenOnce(t *testing.T) {
	in := recs("shop.orders.v1", 0, "ord-1", "bad-2", "ord-3")
	in = append(in, recs("shop.orders.v1", 2, "ord-4")...)
	ms := sink.NewMemorySink()
	dlq := &fakeDLQ{}

	for i := 0; i < 2; i++ {
		if err := ProcessWithMax(context.Background(), in, fakeDecoder{}, ms, dlq, 2); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}

	seen := map[string]bool{}
	for _, b := range ms.Batches() {
		tok := b.DedupToken()
		if seen[tok] {
			t.Fatalf("token %s stored twice", tok)
		}
		seen[tok] = true
	}
	// Partition 0 has 2 valid records (max 2 -> one batch); partition 2 has one.
	if len(ms.Batches()) != 2 {
		t.Fatalf("batches = %d, want 2", len(ms.Batches()))
	}
	if ms.RowCount() != 3 {
		t.Fatalf("rows = %d, want 3", ms.RowCount())
	}
	if want := "shop.orders.v1-0-100-102"; !seen[want] {
		t.Fatalf("missing token %s, have %v", want, seen)
	}
}
