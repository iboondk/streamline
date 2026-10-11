# ADR-0001: Avro payloads through Confluent Schema Registry

Status: Accepted

## Context

Order and inventory events cross Kafka between a producer and two consumers written in different languages (Go and Node). Consumers need a contract that is checked, versioned and readable without sharing code. Plain JSON would leave field types unchecked and make schema changes silent.

## Decision

- Events are encoded with Avro. The schemas are `schemas/order_event.avsc` and `schemas/inventory_event.avsc`.
- Payloads use the Confluent wire format: one magic byte (0), a 4-byte big-endian schema id, then the Avro body.
- Subjects follow TopicNameStrategy (`shop.orders.v1-value`, `shop.inventory.v1-value`). `scripts/register-schemas.sh` sets compatibility to `BACKWARD` for each subject.
- Consumers decode with the writer schema that the registry returns by id.

## Consequences

- Adding an optional field with a default is a compatible change. An incompatible registration fails with HTTP 409 at the registry, before any consumer sees it.
- The wire header is a parsing contract. A record without it is treated as poison and sent to the DLQ (see ADR-0004).
- The registry is a runtime dependency. The Go consumer retries registry outages. The Node consumer currently treats every decode error as poison. This gap is listed in `docs/FAILURE_HANDLING.md`.
- Consumers need an Avro library in each language (`hamba/avro` in Go, and `@kafkajs/confluent-schema-registry`, which uses `avsc`, in Node). Their behaviour is covered only by unit tests in this repository. No live round trip has been run.
