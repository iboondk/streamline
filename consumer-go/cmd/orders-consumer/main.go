// Command orders-consumer reads Avro order events from Kafka, writes them to
// ClickHouse with idempotent batch inserts, and commits offsets only after a
// successful write.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iboondk/p4-streamline/consumer-go/internal/app"
	"github.com/iboondk/p4-streamline/consumer-go/internal/dlq"
	"github.com/iboondk/p4-streamline/consumer-go/internal/registry"
	"github.com/iboondk/p4-streamline/consumer-go/internal/sink"
)

const consumerGroup = "streamline-orders"

type config struct {
	brokers     []string
	registryURL string
	topic       string
	chAddr      string
	chUser      string
	chPassword  string
	chDB        string
	batchMax    int
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadConfig() (config, error) {
	var missing []string
	require := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			missing = append(missing, key)
		}
		return v
	}

	cfg := config{
		brokers:     strings.Split(require("KAFKA_BROKERS"), ","),
		registryURL: require("SCHEMA_REGISTRY_URL"),
		topic:       require("ORDERS_TOPIC"),
		chAddr:      require("CLICKHOUSE_ADDR"),
		chUser:      env("CLICKHOUSE_USER", "default"),
		chPassword:  os.Getenv("CLICKHOUSE_PASSWORD"),
		chDB:        env("CLICKHOUSE_DB", "streamline"),
		batchMax:    1000,
	}
	if len(missing) > 0 {
		return cfg, fmt.Errorf("missing required env: %s", strings.Join(missing, ", "))
	}
	if v := os.Getenv("BATCH_MAX"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return cfg, fmt.Errorf("BATCH_MAX must be a positive integer, got %q", v)
		}
		cfg.batchMax = n
	}
	return cfg, nil
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("consumer stopped with error", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	chSink, err := sink.NewClickHouse(cfg.chAddr, cfg.chDB, cfg.chUser, cfg.chPassword)
	if err != nil {
		return err
	}
	defer chSink.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.brokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(cfg.topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(500*time.Millisecond),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	// CloseAllowingRebalance, not Close: loop may return while a poll is still
	// outstanding (BlockRebalanceOnPoll), and Close would hang waiting to leave
	// the group in that state.
	defer cl.CloseAllowingRebalance()

	dlqProducer := dlq.New(cl, dlq.Topic(cfg.topic))
	decoder := app.NewAvroDecoder(registry.New(cfg.registryURL, &http.Client{Timeout: 10 * time.Second}))

	log.Info("orders consumer started", "topic", cfg.topic, "dlq", dlq.Topic(cfg.topic),
		"group", consumerGroup, "batch_max", cfg.batchMax)

	loop(ctx, log, cl, decoder, chSink, dlqProducer, cfg.batchMax)
	log.Info("orders consumer stopped")
	return nil
}

// loop polls, processes and commits until ctx is cancelled. A failed poll is
// not committed; its offsets are rewound so the same records are fetched again
// after a backoff.
func loop(ctx context.Context, log *slog.Logger, cl *kgo.Client, dec app.Decoder,
	s sink.Sink, dl app.DLQ, batchMax int) {

	const (
		minBackoff = 200 * time.Millisecond
		maxBackoff = 10 * time.Second
	)
	backoff := minBackoff

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		for _, fe := range fetches.Errors() {
			log.Warn("fetch error", "topic", fe.Topic, "partition", fe.Partition, "err", fe.Err)
		}

		recs := fetches.Records()
		if len(recs) > 0 {
			if err := app.ProcessWithMax(ctx, recs, dec, s, dl, batchMax); err != nil {
				if ctx.Err() != nil {
					cl.AllowRebalance()
					return
				}
				log.Error("process failed, not committing", "err", err, "records", len(recs), "backoff", backoff)
				rewind(cl, recs)
				cl.AllowRebalance()
				if !sleepCtx(ctx, backoff) {
					return
				}
				backoff = min(backoff*2, maxBackoff)
				continue
			}
			// The batch is already in ClickHouse. Commit with a context detached
			// from shutdown so a SIGTERM right after a successful insert does not
			// force a redelivery; the timeout bounds how long shutdown can wait.
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			err := cl.CommitRecords(cctx, recs...)
			cancel()
			if err != nil {
				log.Error("commit failed; records will be redelivered", "err", err)
			}
		}
		backoff = minBackoff
		cl.AllowRebalance()
	}
}

// rewind moves each partition's fetch position back to the first record of
// this poll so that nothing is skipped after a failure.
func rewind(cl *kgo.Client, recs []*kgo.Record) {
	first := make(map[string]map[int32]int64)
	for _, r := range recs {
		parts, ok := first[r.Topic]
		if !ok {
			parts = make(map[int32]int64)
			first[r.Topic] = parts
		}
		if off, seen := parts[r.Partition]; !seen || r.Offset < off {
			parts[r.Partition] = r.Offset
		}
	}
	offsets := make(map[string]map[int32]kgo.EpochOffset, len(first))
	for topic, parts := range first {
		m := make(map[int32]kgo.EpochOffset, len(parts))
		for p, off := range parts {
			m[p] = kgo.EpochOffset{Epoch: -1, Offset: off}
		}
		offsets[topic] = m
	}
	cl.SetOffsets(offsets)
}

// sleepCtx waits for d or until ctx is done. It reports whether the full wait elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
