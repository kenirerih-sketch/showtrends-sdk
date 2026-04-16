package showtrends

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientBatchesCounterAndValuePosts(t *testing.T) {
	recorder := newRequestRecorder(t)
	client := newTestClient(t, 25*time.Millisecond, recorder.record)

	client.Count("hits", 4)
	client.Value("latency_ms", 12.5)

	recorder.waitForRequests(t, 1)
	requests := recorder.snapshot()
	require.Len(t, requests, 1)
	require.Len(t, requests[0], 2)
	require.Equal(t, "demo", requests[0][0].Namespace)
	require.Equal(t, "demo", requests[0][1].Namespace)
	requireStatCounterValue(t, requests[0][0], 4)
	requireStatValueValue(t, requests[0][1], 12.5)
}

func TestClientCloseFlushesPendingStats(t *testing.T) {
	recorder := newRequestRecorder(t)
	client := newTestClient(t, time.Hour, recorder.record)

	client.CountOne("hits")
	require.NoError(t, client.Close(context.Background()))

	requests := recorder.snapshot()
	require.Len(t, requests, 1)
	require.Len(t, requests[0], 1)
	require.Equal(t, "demo", requests[0][0].Namespace)
	requireStatCounterValue(t, requests[0][0], 1)
}

func TestClientAllowsBlankNamespaceAndName(t *testing.T) {
	recorder := newRequestRecorder(t)
	client := NewClient("http://showtrends.test", "", time.Hour)
	t.Cleanup(func() {
		require.NoError(t, client.Close(context.Background()))
	})

	client.httpClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			recorder.record(t, r)
			return testNoContentResponse(), nil
		}),
	}

	client.Count("", 1)
	client.Value("", 2)
	require.NoError(t, client.Flush(context.Background()))

	requests := recorder.snapshot()
	require.Len(t, requests, 1)
	require.Len(t, requests[0], 2)
	require.Equal(t, "", requests[0][0].Namespace)
	require.Equal(t, "", requests[0][0].Name)
	require.Equal(t, "", requests[0][1].Namespace)
	require.Equal(t, "", requests[0][1].Name)
}

func TestClientDropsNegativeCounters(t *testing.T) {
	recorder := newRequestRecorder(t)
	client := newTestClient(t, time.Hour, recorder.record)

	client.Count("hits", -1)
	require.NoError(t, client.Flush(context.Background()))

	require.Empty(t, recorder.snapshot())
}

func TestClientDropsCountsAfterClose(t *testing.T) {
	recorder := newRequestRecorder(t)
	client := newTestClient(t, time.Hour, recorder.record)

	require.NoError(t, client.Close(context.Background()))
	client.Count("hits", 1)
	client.Value("latency_ms", 2)
	require.NoError(t, client.Flush(context.Background()))

	require.Empty(t, recorder.snapshot())
}

func TestNewClientUsesDefaultBatchIntervalForNonPositiveValues(t *testing.T) {
	client := NewClient("http://showtrends.test", "demo", -1*time.Second)
	require.Equal(t, DefaultBatchInterval, client.batchInterval)
	require.NoError(t, client.Close(context.Background()))
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type requestRecorder struct {
	mu       sync.Mutex
	requests [][]postStat
	doneCh   chan struct{}
}

func newRequestRecorder(t *testing.T) *requestRecorder {
	t.Helper()
	return &requestRecorder{
		doneCh: make(chan struct{}, 8),
	}
}

func (r *requestRecorder) record(t *testing.T, req *http.Request) []postStat {
	t.Helper()
	require.Equal(t, http.MethodPost, req.Method)
	require.Equal(t, "/v1/stats", req.URL.Path)

	var payload []postStat
	require.NoError(t, json.NewDecoder(req.Body).Decode(&payload))

	r.mu.Lock()
	r.requests = append(r.requests, payload)
	r.mu.Unlock()

	select {
	case r.doneCh <- struct{}{}:
	default:
	}

	return payload
}

func (r *requestRecorder) waitForRequests(t *testing.T, count int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if len(r.snapshot()) >= count {
			return
		}
		select {
		case <-r.doneCh:
		case <-deadline:
			t.Fatalf("timed out waiting for %d requests", count)
		}
	}
}

func (r *requestRecorder) snapshot() [][]postStat {
	r.mu.Lock()
	defer r.mu.Unlock()

	requests := make([][]postStat, len(r.requests))
	copy(requests, r.requests)
	return requests
}

func newTestClient(t *testing.T, batchInterval time.Duration, handler func(*testing.T, *http.Request) []postStat) *Client {
	t.Helper()

	client := NewClient("http://showtrends.test", "demo", batchInterval)
	t.Cleanup(func() {
		require.NoError(t, client.Close(context.Background()))
	})

	client.httpClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			handler(t, r)
			return testNoContentResponse(), nil
		}),
	}

	return client
}

func testNoContentResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusNoContent,
		Status:     "204 No Content",
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     make(http.Header),
	}
}

func requireStatCounterValue(t *testing.T, stat postStat, want float64) {
	t.Helper()
	require.NotNil(t, stat.Counter)
	require.Nil(t, stat.Value)
	require.Equal(t, want, *stat.Counter)
}

func requireStatValueValue(t *testing.T, stat postStat, want float64) {
	t.Helper()
	require.NotNil(t, stat.Value)
	require.Nil(t, stat.Counter)
	require.Equal(t, want, *stat.Value)
}
