package kafka

import (
	"bytes"
	"context"
	"os/exec"

	exporter "github.com/kawamuray/prometheus-kafka-consumer-group-exporter"
)

// DescribeGroupParser parses the output from `kafka-consumer-group.sh --describe`.
type DescribeGroupParser interface {
	// Parses the output from `kafka-consumer-group.sh --describe`
	Parse(output CommandOutput) ([]exporter.PartitionInfo, error)
}

// ConsumerGroupsCommandClient queries Kafka for consumer groups and their current log
// and offset states.
type ConsumerGroupsCommandClient struct {
	Parser                   DescribeGroupParser
	BootstrapServers         string
	ConsumerGroupCommandPath string
}

// CommandOutput is the output from a DescribeGroupParser.
type CommandOutput struct {
	Stdout string
	Stderr string
}

func (col *ConsumerGroupsCommandClient) execConsumerGroupCommand(ctx context.Context, args ...string) (output CommandOutput, err error) {
	allArgs := append([]string{"--new-consumer", "--bootstrap-server", col.BootstrapServers}, args...)
	cmd := exec.Command(col.ConsumerGroupCommandPath, allArgs...)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		return
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
	err = cmd.Wait()

	close(quitChan)

	output.Stdout = string(stdout.Bytes())
	output.Stderr = string(stderr.Bytes())
	return
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
	return col.Parser.Parse(output)
}
