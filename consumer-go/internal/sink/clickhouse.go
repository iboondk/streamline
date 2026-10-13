package sink

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/iboondk/p4-streamline/consumer-go/internal/batch"
)

const insertSQL = `INSERT INTO orders_raw (
	event_id, event_type, order_id, customer_id, shop_id, currency,
	total_cents, item_count, occurred_at, kafka_partition, kafka_offset)`

// ClickHouseSink inserts batches into streamline.orders_raw over the native protocol.
type ClickHouseSink struct {
	conn clickhouse.Conn
}

// NewClickHouse opens a native-protocol connection pool. It does not dial the
// server; the first Insert does.
func NewClickHouse(addr, db, user, pass string) (*ClickHouseSink, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: db, Username: user, Password: pass},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	return &ClickHouseSink{conn: conn}, nil
}

// Insert writes all rows of b in one block. The block carries the batch's
// dedupe token and asks ClickHouse to deduplicate the dependent materialized
// views too.
func (s *ClickHouseSink) Insert(ctx context.Context, b batch.Batch) error {
	if len(b.Rows) == 0 {
		return nil
	}
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"insert_deduplication_token":                         b.DedupToken(),
		"deduplicate_blocks_in_dependent_materialized_views": 1,
	}))

	pb, err := s.conn.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("prepare batch %s: %w", b.DedupToken(), err)
	}
	for _, r := range b.Rows {
		if err := pb.Append(
			r.EventID, r.EventType, r.OrderID, r.CustomerID, r.ShopID, r.Currency,
			r.TotalCents, r.ItemCount, r.OccurredAt, r.Partition, r.Offset,
		); err != nil {
			_ = pb.Abort()
			return fmt.Errorf("append row %s: %w", r.EventID, err)
		}
	}
	if err := pb.Send(); err != nil {
		return fmt.Errorf("send batch %s: %w", b.DedupToken(), err)
	}
	return nil
}

// Close releases the connection pool.
func (s *ClickHouseSink) Close() error {
	return s.conn.Close()
}
