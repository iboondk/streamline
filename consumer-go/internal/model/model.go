// Package model holds the decoded order event and the ClickHouse row it maps to.
package model

import "time"

// OrderEvent mirrors schemas/order_event.avsc (com.streamline.shop.OrderEvent).
type OrderEvent struct {
	EventID    string     `avro:"event_id"`
	EventType  string     `avro:"event_type"`
	OrderID    string     `avro:"order_id"`
	CustomerID string     `avro:"customer_id"`
	ShopID     string     `avro:"shop_id"`
	Currency   string     `avro:"currency"`
	TotalCents int64      `avro:"total_cents"`
	LineItems  []LineItem `avro:"line_items"`
	OccurredAt time.Time  `avro:"occurred_at"`
}

// LineItem mirrors the LineItem record nested in OrderEvent.
type LineItem struct {
	SKU            string `avro:"sku"`
	Quantity       int32  `avro:"quantity"`
	UnitPriceCents int64  `avro:"unit_price_cents"`
}

// OrderRow is one row of streamline.orders_raw plus its Kafka position.
type OrderRow struct {
	EventID    string
	EventType  string
	OrderID    string
	CustomerID string
	ShopID     string
	Currency   string
	TotalCents int64
	ItemCount  uint32
	OccurredAt time.Time
	Partition  uint16
	Offset     uint64
}

// ToRow converts a decoded event into a ClickHouse row. ItemCount is the sum
// of line item quantities.
func ToRow(e OrderEvent, partition int32, offset int64) OrderRow {
	var items uint32
	for _, li := range e.LineItems {
		items += uint32(li.Quantity)
	}
	return OrderRow{
		EventID:    e.EventID,
		EventType:  e.EventType,
		OrderID:    e.OrderID,
		CustomerID: e.CustomerID,
		ShopID:     e.ShopID,
		Currency:   e.Currency,
		TotalCents: e.TotalCents,
		ItemCount:  items,
		OccurredAt: e.OccurredAt,
		Partition:  uint16(partition),
		Offset:     uint64(offset),
	}
}
