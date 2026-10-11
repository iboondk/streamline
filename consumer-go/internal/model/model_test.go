package model

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
)

func TestDecodeAndToRow(t *testing.T) {
	// Schema file lives in the repo root, three levels above this package.
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "schemas", "order_event.avsc"))
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	schema, err := avro.Parse(string(raw))
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}

	when := time.UnixMilli(1760000000123).UTC()
	in := OrderEvent{
		EventID:    "5f1c2d3e-0000-4000-8000-000000000001",
		EventType:  "ORDER_PAID",
		OrderID:    "order-42",
		CustomerID: "cust-7",
		ShopID:     "shop-1",
		Currency:   "EUR",
		TotalCents: 4599,
		LineItems: []LineItem{
			{SKU: "sku-a", Quantity: 2, UnitPriceCents: 1500},
			{SKU: "sku-b", Quantity: 3, UnitPriceCents: 533},
		},
		OccurredAt: when,
	}

	data, err := avro.Marshal(schema, in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out OrderEvent
	if err := avro.Unmarshal(schema, data, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.EventID != in.EventID || out.EventType != "ORDER_PAID" || out.TotalCents != 4599 {
		t.Fatalf("decoded fields mismatch: %+v", out)
	}
	if !out.OccurredAt.Equal(when) {
		t.Fatalf("occurred_at = %v, want %v", out.OccurredAt, when)
	}
	if len(out.LineItems) != 2 || out.LineItems[1].SKU != "sku-b" {
		t.Fatalf("line items mismatch: %+v", out.LineItems)
	}

	row := ToRow(out, 4, 9001)
	want := OrderRow{
		EventID:    in.EventID,
		EventType:  "ORDER_PAID",
		OrderID:    "order-42",
		CustomerID: "cust-7",
		ShopID:     "shop-1",
		Currency:   "EUR",
		TotalCents: 4599,
		ItemCount:  5,
		OccurredAt: when,
		Partition:  4,
		Offset:     9001,
	}
	if row.EventID != want.EventID || row.EventType != want.EventType ||
		row.OrderID != want.OrderID || row.CustomerID != want.CustomerID ||
		row.ShopID != want.ShopID || row.Currency != want.Currency ||
		row.TotalCents != want.TotalCents || row.ItemCount != want.ItemCount ||
		!row.OccurredAt.Equal(want.OccurredAt) || row.Partition != want.Partition ||
		row.Offset != want.Offset {
		t.Fatalf("ToRow = %+v, want %+v", row, want)
	}
}
