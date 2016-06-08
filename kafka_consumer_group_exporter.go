package main

import (
	"github.com/codegangsta/cli"
	"github.com/kawamuray/prometheus-exporter-harness/harness"
	"github.com/kawamuray/prometheus-kafka-consumer-group-exporter/exporter"
)

const ConsumerGroupCommandName = "kafka-consumer-groups.sh"

func main() {
	opts := harness.NewExporterOpts("kafka_consumer_group_exporter", exporter.Version)
	opts.Usage = "[OPTIONS] BOOTSTRAP_SERVERS"
	opts.Init = exporter.Init
	opts.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "consumer-group-command-path",
			Usage: "Path to the kafka-consumer-groups.sh",
			Value: ConsumerGroupCommandName,
		},
	}
	harness.Main(opts)
}
