// Package registry fetches Avro schemas by id from a Confluent-compatible
// Schema Registry and caches them in memory.
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/hamba/avro/v2"
)

// ErrNotFound means the registry does not know the schema id (HTTP 404).
var ErrNotFound = errors.New("registry: schema id not found")

// Client is a schema-by-id lookup with an unbounded in-memory cache.
// Schema ids are immutable in the registry, so cached entries never expire.
type Client struct {
	baseURL    string
	httpClient *http.Client

	mu    sync.Mutex
	cache map[uint32]avro.Schema
}

// New returns a Client for the registry at baseURL. A nil httpClient uses
// http.DefaultClient.
func New(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: httpClient,
		cache:      make(map[uint32]avro.Schema),
	}
}

// Schema returns the parsed Avro schema for id, fetching it on first use.
func (c *Client) Schema(ctx context.Context, id uint32) (avro.Schema, error) {
	c.mu.Lock()
	s, ok := c.cache[id]
	c.mu.Unlock()
	if ok {
		return s, nil
	}

	s, err := c.fetch(ctx, id)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.cache[id] = s
	c.mu.Unlock()
	return s, nil
}

func (c *Client) fetch(ctx context.Context, id uint32) (avro.Schema, error) {
	url := fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("registry: build request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("registry: get schema %d: %w", id, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: id %d", ErrNotFound, id)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("registry: get schema %d: unexpected status %s", id, resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("registry: read schema %d: %w", id, err)
	}
	var doc struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("registry: decode schema %d response: %w", id, err)
	}
	s, err := avro.Parse(doc.Schema)
	if err != nil {
		return nil, fmt.Errorf("registry: parse schema %d: %w", id, err)
	}
	return s, nil
}
