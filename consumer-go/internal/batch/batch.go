// Package batch groups consumed records into per-partition insert units.
package batch

import (
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iboondk/streamline/consumer-go/internal/model"
)

// Batch is one partition's contiguous offset range, ready for a single insert.
type Batch struct {
	Topic       string
	Partition   int32
	FirstOffset int64
	LastOffset  int64
	Rows        []model.OrderRow
}

// DedupToken identifies the batch for ClickHouse insert_deduplication_token.
// The same offset range always yields the same token, so a retried identical
// batch is dropped by the server.
func (b Batch) DedupToken() string {
	return fmt.Sprintf("%s-%d-%d-%d", b.Topic, b.Partition, b.FirstOffset, b.LastOffset)
}

type partKey struct {
	topic     string
	partition int32
}

// Split groups records by (topic, partition), orders each group by offset and
// cuts it into chunks of at most max records. Every chunk holds records from a
// single partition. Partitions are emitted in a stable order (topic, then
// partition). A max below 1 means no chunking.
func Split(records []*kgo.Record, max int) [][]*kgo.Record {
	groups := make(map[partKey][]*kgo.Record)
	for _, r := range records {
		k := partKey{r.Topic, r.Partition}
		groups[k] = append(groups[k], r)
	}

	keys := make([]partKey, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].topic != keys[j].topic {
			return keys[i].topic < keys[j].topic
		}
		return keys[i].partition < keys[j].partition
	})

	var out [][]*kgo.Record
	for _, k := range keys {
		recs := groups[k]
		sort.SliceStable(recs, func(i, j int) bool { return recs[i].Offset < recs[j].Offset })
		size := max
		if size < 1 {
			size = len(recs)
		}
		for start := 0; start < len(recs); start += size {
			end := start + size
			if end > len(recs) {
				end = len(recs)
			}
			out = append(out, recs[start:end])
		}
	}
	return out
}
