package prometheus

import (
	"context"
	"io/ioutil"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kawamuray/prometheus-kafka-consumer-group-exporter/mocks"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestPartitionInfoCollector(t *testing.T) {
	registry := prometheus.NewRegistry()

	timeout := 1 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	collector := NewPartitionInfoCollector(ctx, mocks.NewBasicConsumerGroupsCommandClient(), timeout, 4)
	cancel()
	registry.MustRegister(collector)

	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})

	// Make a request.

	req := httptest.NewRequest("GET", "http://localhost/metrics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Extract response.

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	// Verify the output.

	if resp.StatusCode != 200 {
		t.Fatal("Unexpected HTTP code. Expected: 200 Was:", resp.StatusCode)
	}

	if s, nlines := string(body), len(strings.Split(string(body), "\n")); nlines != 10 {
		t.Error("Unexpected body with", nlines, "lines:", s)
	}
}
