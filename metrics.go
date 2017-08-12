package exporter

import "context"

// PartitionInfo holds information about a partition in Kafka.
type PartitionInfo struct {
	Topic           string
	PartitionID     string
	CurrentOffset   int64
	Lag             int64
	ClientID        string
	ConsumerAddress string
}

// ConsumerGroupInfoClient queries consumer groups and consumer group partitions
// for their stats.
type ConsumerGroupInfoClient interface {
	Groups(ctx context.Context) ([]string, error)
	DescribeGroup(ctx context.Context, group string) ([]PartitionInfo, error)
}
