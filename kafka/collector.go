package kafka

import (
	"bytes"
	"context"
	"os/exec"

	exporter "github.com/kawamuray/prometheus-kafka-consumer-group-exporter"
)

// ConsumerGroupsCommandClient queries Kafka for consumer groups and their current log
// and offset states.
type ConsumerGroupsCommandClient struct {
	BootstrapServers         string
	ConsumerGroupCommandPath string
}

func (col *ConsumerGroupsCommandClient) execConsumerGroupCommand(ctx context.Context, args ...string) (string, error) {
	allArgs := append([]string{"--new-consumer", "--bootstrap-server", col.BootstrapServers}, args...)
	cmd := exec.Command(col.ConsumerGroupCommandPath, allArgs...)

	var b bytes.Buffer
	cmd.Stdout = &b
	cmd.Stderr = &b
	if err := cmd.Start(); err != nil {
		return "", err
	}

	quitChan := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cmd.Process.Kill()
		case <-quitChan:
			return
		}
	}()

	// `err` will be non-nil if the timeout function killed it.
	err := cmd.Wait()

	close(quitChan)
	return string(b.Bytes()), err
}

// Groups returns a list of the Kafka consumer groups.
func (col *ConsumerGroupsCommandClient) Groups(ctx context.Context) ([]string, error) {
	output, err := col.execConsumerGroupCommand(ctx, "--list")
	if err != nil {
		return nil, err
	}
	return parseGroups(output)
}

// DescribeGroup returns current state of all partitions subscribed to by a
// consumer group.
func (col *ConsumerGroupsCommandClient) DescribeGroup(ctx context.Context, group string) ([]exporter.PartitionInfo, error) {
	output, err := col.execConsumerGroupCommand(ctx, "--describe", "--group", group)
	if err != nil {
		return nil, err
	}
	return parsePartitionOutput(output)
}
