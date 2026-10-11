# Streamline pipeline. Run every target from the project root.
# Caches sit on the Hazel drive to keep the main disk free. Override from the
# environment if the path differs on your machine.
GOMODCACHE ?= /Volumes/Hazel/caches/go
npm_config_cache ?= /Volumes/Hazel/caches/npm
PIP_CACHE_DIR ?= /Volumes/Hazel/caches/pip
export GOMODCACHE npm_config_cache PIP_CACHE_DIR

# Extra arguments for the producer, e.g. make produce ARGS="--rate 200 --duration 10"
ARGS ?=
# Arguments for the lag and row-growth sampler (needs a live stack).
MEASURE_ARGS ?= --group streamline-orders --topic shop.orders.v1 --table orders_raw --duration 60

.PHONY: help install up down bootstrap produce measure replay test check

help:
	@echo "targets: install up bootstrap produce measure replay test check down"

# Installs the locked dependencies for the Go and Node packages (needed before test).
install:
	cd consumer-go && go mod download
	cd tools/producer && npm ci
	cd tools/measure && npm ci
	cd consumer-node && npm ci

# Creates the local env file if missing, then starts the stack in the background.
up:
	[ -f .env ] || cp .env.example .env
	docker compose up -d

down:
	docker compose down -v

bootstrap:
	bash scripts/create-topics.sh && bash scripts/register-schemas.sh

produce:
	cd tools/producer && node src/cli.js $(ARGS)

measure:
	cd tools/measure && node src/cli.js $(MEASURE_ARGS)

replay:
	bash scripts/replay-demo.sh

# Unit tests only. They need no running infrastructure.
test:
	cd consumer-go && go test ./...
	cd tools/producer && npm test
	cd tools/measure && npm test
	cd consumer-node && npm test

check:
	python3 scripts/check-static.py
