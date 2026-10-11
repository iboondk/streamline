# ADR-0003: One Go consumer and one Node consumer for different topics

Status: Accepted

## Context

The pipeline has two topics with the same delivery rules. The brief calls for a Go service and a Node service, so the project shows how the same semantics (commit after write, block tokens, DLQ) are built in two ecosystems. Splitting the work also keeps each consumer's responsibilities small.

## Decision

- `consumer-go` (franz-go, hamba/avro, clickhouse-go v2) consumes `shop.orders.v1`, group `streamline-orders`.
- `consumer-node` (kafkajs, @kafkajs/confluent-schema-registry, @clickhouse/client) consumes `shop.inventory.v1`, group `streamline-inventory`.
- Both write to ClickHouse with the same dedupe-token scheme. Their Dockerfiles are separate: a distroless static image for Go and `node:20-alpine` with `npm ci --omit=dev` for Node.
- Go is pure Go with no cgo and no librdkafka.

## Consequences

- Behaviour is duplicated across two code bases. The unit tests describe the same rules in each: Go has 18 test functions and Node's consumer has 20.
- The two consumers differ in one known way: the Go consumer retries registry outages, but the Node consumer sends them to the DLQ. This gap is recorded in `docs/FAILURE_HANDLING.md` as a known gap.
- Operators run two services to watch. Lag and row growth are sampled for the orders group by `tools/measure`.
