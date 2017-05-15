package sync

import (
	"context"
	"sync"

	exporter "github.com/kawamuray/prometheus-kafka-consumer-group-exporter"
)

// FanInConsumerGroupInfoClient is a exporter.ConsumerGroupInfoClient decorator that
// makes multiple same calls to exporter.ConsumerGroupInfoClient functions only
// yield a single call to Delegate. This is useful to avoid running too many
// calls to `kafka-consumer-group.sh` if things are running slow.
type FanInConsumerGroupInfoClient struct {
	Delegate exporter.ConsumerGroupInfoClient

	groupsChan     chan groupsRequest
	initGroupsChan sync.Once

	describeChan     chan describeRequest
	initDescribeChan sync.Once
}

// Groups calls f.Delegate.Groups(). If multiple overlapping calls to this
// function are made, a single call will be made to the f.Delegate.
func (f *FanInConsumerGroupInfoClient) Groups(ctx context.Context) ([]string, error) {
	f.initGroupsChan.Do(func() {
		f.groupsChan = make(chan groupsRequest)
		go f.groupsLoop()
	})

	req := groupsRequest{
		make(chan groupResult),
		ctx,
	}
	f.groupsChan <- req
	res := <-req.result
	return res.groups, res.err
}

type groupResult struct {
	groups []string
	err    error
}
type groupsRequest struct {
	result chan groupResult
	ctx    context.Context
}

func (f *FanInConsumerGroupInfoClient) groupsLoop() {
	for {
		req, ok := <-f.groupsChan
		if !ok {
			// Stopped.
			return
		}

		// The list of all subscribers asking for a list of groups.
		subscribers := []chan groupResult{
			req.result,
		}

		// Start scraping.

		compRes := make(chan groupResult)
		go func() {
			groups, err := f.Delegate.Groups(req.ctx)
			compRes <- groupResult{
				groups,
				err,
			}
		}()

		// Collect additional subscribers, or return result.
	Loop:
		for {
			select {
			case additionalReq := <-f.groupsChan:
				// If there are more requests coming in, we add the to the list
				// of subscribers.
				subscribers = append(subscribers, additionalReq.result)
			case res := <-compRes:
				// Return the computation result to all subscribers.
				for _, subscriber := range subscribers {
					subscriber <- res
				}
				break Loop
			}
		}
	}
}

// DescribeGroup calls f.Delegate.DescribeGroup(). If multiple overlapping
// calls to this function with the same parameters are made, a single call will
// be made to the f.Delegate.
func (f *FanInConsumerGroupInfoClient) DescribeGroup(ctx context.Context, group string) ([]*exporter.PartitionInfo, error) {
	f.initDescribeChan.Do(func() {
		f.describeChan = make(chan describeRequest)
		go f.genericDescribeLoop()
	})

	req := describeRequest{
		group,
		make(chan describeResult),
		ctx,
	}
	f.describeChan <- req
	res := <-req.result
	return res.partitions, res.err
}

type describeRequest struct {
	group  string
	result chan describeResult
	ctx    context.Context
}
type describeResult struct {
	partitions []*exporter.PartitionInfo
	err        error
}

func (f *FanInConsumerGroupInfoClient) genericDescribeLoop() {
	// TODO: There's a memory and goroutine leak here. `m` is never cleaned up.
	// Generally groups are quite static so this shouldn't be too much of an
	// issue.
	m := make(map[string]chan describeRequest)

	for req := range f.describeChan {
		if _, ok := m[req.group]; !ok {
			m[req.group] = make(chan describeRequest)
			go f.describeLoop(m[req.group])
		}

		m[req.group] <- req
	}

	for _, c := range m {
		close(c)
	}
}

func (f *FanInConsumerGroupInfoClient) describeLoop(c chan describeRequest) {
	for {
		req, ok := <-c
		if !ok {
			// Stopped.
			return
		}

		// The list of all subscribers asking for a list of groups.
		subscribers := []chan describeResult{
			req.result,
		}

		// Start scraping.

		compRes := make(chan describeResult)
		go func() {
			partitions, err := f.Delegate.DescribeGroup(req.ctx, req.group)
			compRes <- describeResult{
				partitions,
				err,
			}
		}()

		// Collect additional subscribers, or return result.
	Loop:
		for {
			select {
			case additionalReq, ok := <-c:
				if !ok {
					res := <-compRes

					// Return the computation result to all subscribers.
					for _, subscriber := range subscribers {
						subscriber <- res
					}
				}

				// If there are more requests coming in, we add the to the list
				// of subscribers.
				subscribers = append(subscribers, additionalReq.result)
			case res := <-compRes:
				// Return the computation result to all subscribers.
				for _, subscriber := range subscribers {
					subscriber <- res
				}
				break Loop
			}
		}
	}
}

// Stop all goroutines started by other functions.
func (f *FanInConsumerGroupInfoClient) Stop() {
	if f.groupsChan != nil {
		close(f.groupsChan)
	}
	if f.describeChan != nil {
		close(f.describeChan)
	}

	// Reset the init so we can reuse this again.
	f.initGroupsChan = sync.Once{}
	f.initDescribeChan = sync.Once{}
}
