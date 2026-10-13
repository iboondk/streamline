package batch

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iboondk/streamline/consumer-go/internal/model"
)

func TestDedupTokenFormat(t *testing.T) {
	b := Batch{Topic: "shop.orders.v1", Partition: 3, FirstOffset: 100, LastOffset: 149,
		Rows: []model.OrderRow{{}}}
	if got, want := b.DedupToken(), "shop.orders.v1-3-100-149"; got != want {
		t.Fatalf("DedupToken() = %q, want %q", got, want)
	}
}

func rec(topic string, part int32, off int64) *kgo.Record {
	return &kgo.Record{Topic: topic, Partition: part, Offset: off}
}

func TestSplitKeepsPartitionsSeparateAndContiguous(t *testing.T) {
	// Interleaved input across two partitions, out of offset order within p1.
	in := []*kgo.Record{
		rec("t", 0, 10), rec("t", 1, 50), rec("t", 0, 11), rec("t", 1, 48),
		rec("t", 0, 12), rec("t", 1, 49), rec("t", 0, 13),
	}

	chunks := Split(in, 2)

	// p0 has 4 records -> 2 chunks; p1 has 3 records -> 2 chunks.
	if len(chunks) != 4 {
		t.Fatalf("got %d chunks, want 4", len(chunks))
	}
	var total int
	for _, c := range chunks {
		if len(c) == 0 || len(c) > 2 {
			t.Fatalf("chunk size %d outside 1..2", len(c))
		}
		part := c[0].Partition
		for i, r := range c {
			total++
			if r.Partition != part {
				t.Fatalf("chunk mixes partitions %d and %d", part, r.Partition)
			}
			if i > 0 && r.Offset != c[i-1].Offset+1 {
				t.Fatalf("chunk offsets not contiguous in input: %d then %d", c[i-1].Offset, r.Offset)
			}
		}
	}
	if total != len(in) {
		t.Fatalf("records after split = %d, want %d", total, len(in))
	}
	// Chunks of one partition appear in offset order.
	first := map[int32]int64{}
	for _, c := range chunks {
		p := c[0].Partition
		if last, ok := first[p]; ok && c[0].Offset <= last {
			t.Fatalf("partition %d chunks out of order", p)
		}
		first[p] = c[len(c)-1].Offset
	}
}

func TestSplitNoChunkingWhenMaxNonPositive(t *testing.T) {
	in := []*kgo.Record{rec("t", 2, 1), rec("t", 2, 2), rec("t", 2, 3)}
	chunks := Split(in, 0)
	if len(chunks) != 1 || len(chunks[0]) != 3 {
		t.Fatalf("got %d chunks, want one chunk of 3", len(chunks))
	}
}

func TestSplitEmpty(t *testing.T) {
	if got := Split(nil, 10); len(got) != 0 {
		t.Fatalf("Split(nil) = %d chunks, want 0", len(got))
	}
}
