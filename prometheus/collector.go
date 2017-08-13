package prometheus

import (
	"context"
	"sync"
	"time"

	exporter "github.com/kawamuray/prometheus-kafka-consumer-group-exporter"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	log "github.com/sirupsen/logrus"
)

var (
	partitionOffsetMetricsDesc = prometheus.NewDesc(
		"kafka_broker_consumer_group_current_offset",
		"Current consumed offset of a topic/partition",
		[]string{"group_id", "consumer_address", "client_id", "topic", "partition"},
		nil)
	partitionLagMetricsDesc = prometheus.NewDesc(
		"kafka_broker_consumer_group_offset_lag",
		"Offset lag of a topic/partition",
		[]string{"group_id", "consumer_address", "client_id", "topic", "partition"},
		nil)
)

// PartitionInfoCollector is a Kafka prometheus.Collector. It uses a
// exporter.ConsumerGroupInfoClient for the actual querying of Kafka.
//
// To speed up collection each consumer group is collected
// concurrently.
type PartitionInfoCollector struct {
	groupListErrors     prometheus.Counter
	groupDescribeErrors *prometheus.CounterVec

	client exporter.ConsumerGroupInfoClient

	ctx                  context.Context
	execTimeout          time.Duration
	maxConcurrentQueries int
}

// NewPartitionInfoCollector returns a prometheus.Collector that queries Kafka
// using client. concurrency sets an upper limit on the number concurrent Kafka
// concumer group queries running.
func NewPartitionInfoCollector(ctx context.Context, client exporter.ConsumerGroupInfoClient, execTimeout time.Duration, maxConcurrentQueries int) *PartitionInfoCollector {
	if maxConcurrentQueries <= 0 {
		log.Fatal("maxConcurrentQueries must be positive.")
	}
	return &PartitionInfoCollector{
		prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kafka_broker_consumer_group_list_errors",
			Help: "Number of Kafka scraping errors.",
		}),
		prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_broker_consumer_group_describe_errors",
			Help: "Number of Kafka scraping errors.",
		}, []string{"group"}),
		client,
		ctx,
		execTimeout,
		maxConcurrentQueries,
	}
}

// Describe transmits all metric descriptions to c.
func (p *PartitionInfoCollector) Describe(c chan<- *prometheus.Desc) {
	c <- partitionOffsetMetricsDesc
	c <- partitionLagMetricsDesc
	p.groupListErrors.Describe(c)
	p.groupDescribeErrors.Describe(c)
}

func (p *PartitionInfoCollector) collectGroupsFromChan(<-chan string) {

}

// Collect triggers an on-demand scraping from Kafka and transmits metrics into
// c.
func (p *PartitionInfoCollector) Collect(c chan<- prometheus.Metric) {
	// Important that these are collected _after_ the Kafka collection below to
	// correctly accomodate for the errors that happened during the scrape.
	defer p.groupDescribeErrors.Collect(c)
	defer p.groupListErrors.Collect(c)

	ctx, cancel := context.WithTimeout(p.ctx, p.execTimeout)
	groupnames, err := p.client.Groups(ctx)
	cancel()
	if err != nil {
		log.Error("Could not list groups:", err)
		p.groupListErrors.Inc()
		return
	}

	var wg sync.WaitGroup
	wg.Add(p.maxConcurrentQueries)
	groupsToProcess := make(chan string)
	collectGroupWorker := func() {
		defer wg.Done()
		for groupname := range groupsToProcess {
			ctx, cancel := context.WithTimeout(p.ctx, p.execTimeout)
			partitions, err := p.client.DescribeGroup(ctx, groupname)
			cancel()
			if err != nil {
				log.Errorf("Could not describe group '%s': %s", groupname, err)
				p.groupDescribeErrors.WithLabelValues(groupname).Inc()
				continue
			}

			for _, part := range partitions {
				labels := []string{groupname, part.ConsumerAddress, part.ClientID, part.Topic, part.PartitionID}
				sendGaugeOrLog(c, partitionOffsetMetricsDesc, part.CurrentOffset, labels...)
				sendGaugeOrLog(c, partitionLagMetricsDesc, part.Lag, labels...)
			}
		}
	}
	for i := 0; i < p.maxConcurrentQueries; i++ {
		go collectGroupWorker()
	}
	for _, groupname := range groupnames {
		groupsToProcess <- groupname
	}
	close(groupsToProcess)
	wg.Wait()
}

func sendGaugeOrLog(c chan<- prometheus.Metric, desc *prometheus.Desc, value int64, labelValues ...string) {
	metric, err := prometheus.NewConstMetric(desc, prometheus.GaugeValue, float64(value), labelValues...)
	if err != nil {
		log.Warn("Could not construct a metric:", err)
		return
	}

	// Generally bundling timestamps with Prometheus metrics shouldn't be
	// necessary. However, in Kafka's case it can vary quite a lot between
	// consumer groups how long it can take to scrape the statisic (duration is
	// a function of number of partitions, throughput commit period, and
	// probably more). In case it takes ~1 min for one group to be scraped,
	// while it take 2 seconds for another, we want to give a more realistic
	// timestamp to the Prometheus scraper.
	c <- newTimestampedMetricNow(metric)
}

// timestampedMetric wraps a Metric and makes sure to add timestamp to it. All
// other hackery to do this was too much work.
type timestampedMetric struct {
	delegate    prometheus.Metric
	timestampMs int64
}

// Number of nanoseconds per millisecond.
var nanosPerMillis = int64(time.Millisecond / time.Nanosecond)

func newTimestampedMetricNow(delegate prometheus.Metric) *timestampedMetric {
	return &timestampedMetric{
		delegate,
		time.Now().UnixNano() / nanosPerMillis,
	}
}

func (t *timestampedMetric) Desc() *prometheus.Desc {
	return t.delegate.Desc()
}

func (t *timestampedMetric) Write(dest *dto.Metric) error {
	err := t.delegate.Write(dest)
	if err != nil {
		return err
	}
	dest.TimestampMs = &t.timestampMs
	return nil
}
