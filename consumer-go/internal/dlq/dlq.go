// Package dlq publishes poison records to a dead-letter topic.
package dlq

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Topic derives the dead-letter topic from a source topic by replacing the
// ".v1" version suffix with ".dlq", e.g. shop.orders.v1 -> shop.orders.dlq.
// A topic without ".v1" gets ".dlq" appended.
func Topic(src string) string {
	if strings.Contains(src, ".v1") {
		return strings.Replace(src, ".v1", ".dlq", 1)
	}
	return src + ".dlq"
}

// Producer sends failed records to a dead-letter topic.
type Producer struct {
	cl    *kgo.Client
	topic string
}

// New returns a Producer that writes to topic using cl.
func New(cl *kgo.Client, topic string) *Producer {
	return &Producer{cl: cl, topic: topic}
}

// Send produces rec to the dead-letter topic with the original key and value
// and the headers x-error, x-source-partition and x-source-offset.
func (p *Producer) Send(ctx context.Context, rec *kgo.Record, cause error) error {
	errText := "unknown error"
	if cause != nil {
		errText = cause.Error()
	}
	out := &kgo.Record{
		Topic: p.topic,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: []kgo.RecordHeader{
			{Key: "x-error", Value: []byte(errText)},
			{Key: "x-source-partition", Value: []byte(strconv.FormatInt(int64(rec.Partition), 10))},
			{Key: "x-source-offset", Value: []byte(strconv.FormatInt(rec.Offset, 10))},
		},
	}
	if err := p.cl.ProduceSync(ctx, out).FirstErr(); err != nil {
		return fmt.Errorf("dlq produce to %s: %w", p.topic, err)
	}
	return nil
}
