package sync

import (
	"context"
	"sync"
	"testing"

	exporter "github.com/kawamuray/prometheus-kafka-consumer-group-exporter"
	"github.com/kawamuray/prometheus-kafka-consumer-group-exporter/mocks"
)

func TestFanOutGroupsListing(t *testing.T) {
	slowGroupLister := &mocks.ConsumerGroupsCommandClient{
		GroupsFn: func() ([]string, error) {
			return nil, nil
		},
	}

	fanOuter := &FanInConsumerGroupInfoClient{
		Delegate: slowGroupLister,
	}
	defer fanOuter.Stop()

	var wg sync.WaitGroup
	wg.Add(2)
	f := func() {
		fanOuter.Groups(context.Background())
		wg.Done()
	}

	go f()
	go f()
	wg.Wait()

	if slowGroupLister.GroupInvocations < 1 && slowGroupLister.GroupInvocations > 2 {
		t.Error("Expected only a single invocation to Groups() function. It was called", slowGroupLister.GroupInvocations, "times.")
	}

}

func TestFanOutDescribeGroup(t *testing.T) {
	slowGroupLister := &mocks.ConsumerGroupsCommandClient{
		DescribeGroupFn: func(group string) ([]exporter.PartitionInfo, error) {
			return nil, nil
		},
	}

	fanOuter := &FanInConsumerGroupInfoClient{
		Delegate: slowGroupLister,
	}
	defer fanOuter.Stop()

	var wg sync.WaitGroup
	wg.Add(2)
	f := func() {
		fanOuter.DescribeGroup(context.Background(), "default")
		wg.Done()
	}

	go f()
	go f()
	wg.Wait()

	if slowGroupLister.GroupInvocations < 1 && slowGroupLister.GroupInvocations > 2 {
		t.Error("Expected only a single invocation to Groups() function. It was called", slowGroupLister.GroupInvocations, "times.")
	}

}
