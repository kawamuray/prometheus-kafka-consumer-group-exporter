package kafka

import (
	"context"
	"os"
	. "testing"
	"time"

	exporter "github.com/kawamuray/prometheus-kafka-consumer-group-exporter"
)

func TestKafkaPartitionExecution(t *T) {
	// Check prerequisites

	scriptPath := os.Getenv("KAFKA_CONSUMER_GROUP_SCRIPT")
	if scriptPath == "" {
		t.Skip("Please define KAFKA_CONSUMER_GROUP_SCRIPT environment flag.")
	}

	bootstrapServer := os.Getenv("KAFKA_BOOTSTRAP_SERVER")
	if bootstrapServer == "" {
		t.Skip("Please define KAFKA_BOOTSTRAP_SERVER environment flag.")
	}

	// Build consumer.

	consumer := ConsumerGroupsCommandClient{
		new(DefaultParser),
		bootstrapServer,
		scriptPath,
	}

	// Test the consumer

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	groups, err := consumer.Groups(ctx)
	cancel()
	if err != nil {
		t.Fatal("Could not list groups:", err)
	}

	for _, groupname := range groups {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		partitions, err := consumer.DescribeGroup(ctx, groupname)
		cancel()
		if err != nil {
			t.Fatal("Could get group description for group", groupname, "Error:", err)
		}

		// Basic partition sanity checks.

		for _, partition := range partitions {
			checkPartitionLooksSane(t, groupname, partition)
		}
	}

}

func checkPartitionLooksSane(t *T, groupname string, partition exporter.PartitionInfo) {
	checkFieldIsNotEmpty(t, groupname, partition.Topic, "Topic")
	checkFieldIsNotEmpty(t, groupname, partition.PartitionID, "PartitionID")
	checkFieldIsNotEmpty(t, groupname, partition.ClientID, "ClientID")
	checkFieldIsNotEmpty(t, groupname, partition.ConsumerAddress, "ConsumerAddress")
}

func checkFieldIsNotEmpty(t *T, groupname, value, fieldname string) {
	if value != "" {
		t.Error(fieldname, "missing for consumer group:", groupname)
	}
}

func TestKafkaDownFails(t *T) {
	// Check prerequisites

	brokenServer := "localhost:9042"

	scriptPath := os.Getenv("KAFKA_CONSUMER_GROUP_SCRIPT")
	if scriptPath == "" {
		t.Skip("Please define KAFKA_CONSUMER_GROUP_SCRIPT environment flag.")
	}

	bootstrapServer := os.Getenv("KAFKA_BOOTSTRAP_SERVER")
	if bootstrapServer == brokenServer {
		t.Fatal("Broken server is defined in KAFKA_BOOTSTRAP_SERVER. It's not expected to be accessible.")
	}

	// Build consumer.

	consumer := ConsumerGroupsCommandClient{
		new(DefaultParser),
		brokenServer,
		scriptPath,
	}

	// Test the consumer

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	_, err := consumer.Groups(ctx)
	cancel()
	if err == nil {
		t.Error("Expected an error when not being to connect to to Kafka.")
	}
}
