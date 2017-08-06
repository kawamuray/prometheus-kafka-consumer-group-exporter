package main

import (
	"context"
	"net/http"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/kawamuray/prometheus-kafka-consumer-group-exporter/kafka"
	kafkaprom "github.com/kawamuray/prometheus-kafka-consumer-group-exporter/prometheus"
	"github.com/kawamuray/prometheus-kafka-consumer-group-exporter/sync"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli"
)

const consumerGroupCommandName = "kafka-consumer-groups.sh"
const version = "0.0.2"

func main() {
	app := cli.NewApp()
	app.Name = "kafka_consumer_group_exporter"
	app.Version = version
	app.Usage = "[OPTIONS] BOOTSTRAP_SERVER#1,BOOTSTRAP_SERVER#2,..."
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "consumer-group-command-path",
			Usage: "Path to `kafka-consumer-groups.sh`.",
			Value: consumerGroupCommandName,
		},
		cli.StringFlag{
			Name:  "listen",
			Usage: "Interface and port to listen on.",
			Value: ":7979",
		},
		cli.DurationFlag{
			Name:  "kafka-command-timeout",
			Usage: "The maximum time the Kafka command is allowed to take before we kill it. We've seen it block forever in production at times (most likely during rebalances).",
			Value: 5 * time.Minute,
		},
		cli.IntFlag{
			Name:  "max-concurrent-group-queries",
			Usage: "The maximum number of consumer groups that are queried concurrently.",
			// Given that Kafka defaults maximum heap size to 256M for the
			// `kafka-consumer-groups.sh` script, the upper heap allocation
			// could be Value*256 MB.
			Value: 4,
		},
	}

	app.Action = func(c *cli.Context) {
		if c.NArg() == 0 {
			log.Fatal("Bootstrap server(s) missing.")
		}
		bootstrapServers := c.Args().Get(0)

		kafkaClient := kafka.ConsumerGroupsCommandClient{
			BootstrapServers:         bootstrapServers,
			ConsumerGroupCommandPath: c.String("consumer-group-command-path"),
		}
		fanInClient := sync.FanInConsumerGroupInfoClient{
			Delegate: &kafkaClient,
		}
		collector := kafkaprom.NewPartitionInfoCollector(
			context.Background(),
			&fanInClient,
			c.Duration("kafka-command-timeout"),
			c.Int("max-concurrent-group-queries"),
		)
		prometheus.DefaultRegisterer.MustRegister(collector)

		log.Fatal(http.ListenAndServe(c.String("listen"), promhttp.Handler()))
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
