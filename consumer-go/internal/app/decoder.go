package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"

	"github.com/iboondk/p4-streamline/consumer-go/internal/model"
	"github.com/iboondk/p4-streamline/consumer-go/internal/registry"
	"github.com/iboondk/p4-streamline/consumer-go/internal/wire"
)

// AvroDecoder decodes Confluent-framed Avro order events, resolving writer
// schemas through the registry.
type AvroDecoder struct {
	reg *registry.Client
}

// NewAvroDecoder returns a decoder backed by reg.
func NewAvroDecoder(reg *registry.Client) *AvroDecoder {
	return &AvroDecoder{reg: reg}
}

// Decode returns the event in b. Bad framing, an unknown schema id and
// payloads that do not match the schema are poison. Registry failures other
// than 404 are RetryableError.
func (d *AvroDecoder) Decode(ctx context.Context, b []byte) (model.OrderEvent, error) {
	var ev model.OrderEvent
	id, payload, err := wire.Parse(b)
	if err != nil {
		return ev, err
	}
	schema, err := d.reg.Schema(ctx, id)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return ev, err
		}
		return ev, &RetryableError{Err: err}
	}
	if err := avro.Unmarshal(schema, payload, &ev); err != nil {
		return ev, fmt.Errorf("avro decode (schema %d): %w", id, err)
	}
	return ev, nil
}
