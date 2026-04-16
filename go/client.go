package showtrends

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const defaultHTTPTimeout = 10 * time.Second
const DefaultBatchInterval = 200 * time.Millisecond

type Client struct {
	serverURL     string
	namespace     string
	postURL       string
	batchInterval time.Duration
	httpClient    *http.Client

	mu      sync.Mutex
	pending []postStat
	closed  bool

	stopCh chan struct{}
	doneCh chan struct{}
}

type postStat struct {
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	Counter   *float64 `json:"counter,omitempty"`
	Value     *float64 `json:"value,omitempty"`
}

func NewClient(serverURL string, namespace string, batchInterval time.Duration) *Client {
	serverURL = strings.TrimRight(strings.TrimSpace(serverURL), "/")
	if batchInterval <= 0 {
		batchInterval = DefaultBatchInterval
	}

	c := &Client{
		serverURL:     serverURL,
		namespace:     namespace,
		postURL:       serverURL + "/v1/stats",
		batchInterval: batchInterval,
		httpClient: &http.Client{
			Timeout: defaultHTTPTimeout,
		},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	go c.run()
	return c
}

func (c *Client) Count(name string, counter float64) {
	if counter < 0 {
		return
	}
	c.enqueue(postStat{
		Namespace: c.namespace,
		Name:      name,
		Counter:   float64Ptr(counter),
	})
}

func (c *Client) CountOne(name string) {
	c.Count(name, 1)
}

func (c *Client) Value(name string, value float64) {
	c.enqueue(postStat{
		Namespace: c.namespace,
		Name:      name,
		Value:     float64Ptr(value),
	})
}

func (c *Client) Flush(ctx context.Context) error {
	return c.flush(ctx)
}

func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.stopCh)
	c.mu.Unlock()

	<-c.doneCh
	return c.flush(ctx)
}

func (c *Client) run() {
	ticker := time.NewTicker(c.batchInterval)
	defer ticker.Stop()
	defer close(c.doneCh)

	for {
		select {
		case <-ticker.C:
			if err := c.flush(context.Background()); err != nil {
				continue
			}
		case <-c.stopCh:
			return
		}
	}
}

func (c *Client) enqueue(stat postStat) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.pending = append(c.pending, stat)
}

func (c *Client) flush(ctx context.Context) error {
	stats := c.takePending()
	if len(stats) == 0 {
		return nil
	}

	return c.postBatch(ctx, stats)
}

func (c *Client) takePending() []postStat {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.pending) == 0 {
		return nil
	}

	stats := make([]postStat, len(c.pending))
	copy(stats, c.pending)
	c.pending = nil
	return stats
}

func (c *Client) postBatch(ctx context.Context, stats []postStat) error {
	body, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("marshal stats: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.postURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("post stats: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("post stats: unexpected status %s", resp.Status)
	}

	return nil
}

func float64Ptr(v float64) *float64 {
	return &v
}
