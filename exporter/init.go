package exporter

import (
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/kawamuray/prometheus-exporter-harness/harness"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	MetricNamePrefix = "kafka_broker_consumer_group"

	MetricCurrentOffset = MetricNamePrefix + "_current_offset"
	MetricOffsetLag     = MetricNamePrefix + "_offset_lag"
)

func Init(c *cli.Context, reg *harness.MetricRegistry) (harness.Collector, error) {
	args := c.Args()

	if len(args) < 1 {
		cli.ShowAppHelp(c)
		return nil, fmt.Errorf("not enough arguments")
	}

	var (
		bootstrapServers         = args[0]
		consumerGroupCommandPath = c.String("consumer-group-command-path")
	)

	labelNames := []string{
		"group_id",
		"consumer_address",
		"client_id",
		"topic",
		"partition",
	}

	reg.Register(
		MetricCurrentOffset,
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: MetricCurrentOffset,
			Help: "Current consumed offset of a topic/partition",
		}, labelNames),
	)

	reg.Register(
		MetricOffsetLag,
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: MetricOffsetLag,
			Help: "Offset lag of a topic/partition",
		}, labelNames),
	)

	return NewCollector(bootstrapServers, consumerGroupCommandPath), nil
}
