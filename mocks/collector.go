package mocks

import (
	"context"

	exporter "github.com/kawamuray/prometheus-kafka-consumer-group-exporter"
)

var mockGroupName = "default"

// ConsumerGroupsCommandClient is a fake Kafka queries which can be used for testing
// purposes when Kafka and/or the `kafka-consumer-groups.sh` is missing.
type ConsumerGroupsCommandClient struct {
	GroupsFn         func() ([]string, error)
	GroupInvocations int

	DescribeGroupFn          func(group string) ([]*exporter.PartitionInfo, error)
	DescribeGroupInvocations int
}

// Groups returns a list of a single group.
func (col *ConsumerGroupsCommandClient) Groups(_ context.Context) ([]string, error) {
	col.GroupInvocations++
	return col.GroupsFn()
}

// DescribeGroup returns a single fake partition for the group that Groups
// returns. For other consumer groups it returns an error.
func (col *ConsumerGroupsCommandClient) DescribeGroup(_ context.Context, group string) ([]*exporter.PartitionInfo, error) {
	col.DescribeGroupInvocations++
	return col.DescribeGroupFn(group)
}

// NewBasicConsumerGroupsCommandClient creates a new
// ConsumerGroupsCommandClient with prepopulated functions. For mocking, you
// can override the function implementations.
func NewBasicConsumerGroupsCommandClient() *ConsumerGroupsCommandClient {
	return &ConsumerGroupsCommandClient{
		GroupsFn: func() ([]string, error) {
			return []string{mockGroupName}, nil
		},
		DescribeGroupFn: func(group string) ([]*exporter.PartitionInfo, error) {
			return []*exporter.PartitionInfo{
				{
					Topic:           "testtopic",
					PartitionID:     "0",
					CurrentOffset:   9999,
					Lag:             99,
					ClientID:        "consumer-99",
					ConsumerAddress: "127.0.0.1",
				},
			}, nil
		},
	}
}
