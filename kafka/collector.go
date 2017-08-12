package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	exporter "github.com/kawamuray/prometheus-kafka-consumer-group-exporter"
)

// Fields separator is different between kafka versions
// 0.9.X  => ", "
// 0.10.X => \s+
var consumerGroupCommandDescribeOutputSeparatorRegexp = regexp.MustCompile(`,?\s+`)

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

func parseGroups(output string) ([]string, error) {
	if strings.Contains(output, "java.lang.RuntimeException") {
		return nil, fmt.Errorf("Got runtime error when executing script. Output: %s", output)
	}

	lines := strings.Split(output, "\n")
	groups := make([]string, 0, len(lines))
	for _, line := range lines {
		if line != "" {
			groups = append(groups, line)
		}
	}

	return groups, nil
}

func parseClientIDAndConsumerAddress(clientIDAndConsumerAddress string) (string, string) {
	const Separator = "_/"
	splitPoint := strings.LastIndex(clientIDAndConsumerAddress, Separator)
	if splitPoint == -1 {
		return clientIDAndConsumerAddress, ""
	}
	return clientIDAndConsumerAddress[:splitPoint], clientIDAndConsumerAddress[splitPoint+len(Separator):]
}

func parseLong(value string) (int64, error) {
	longVal, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return -1, err
	}
	return longVal, nil
}

func parsePartitionInfo(line string) (*exporter.PartitionInfo, error) {
	fields := consumerGroupCommandDescribeOutputSeparatorRegexp.Split(line, -1)
	if len(fields) != 7 {
		return nil, fmt.Errorf("malformed line: %s", line)
	}

	var err error

	var currentOffset int64
	currentOffset, err = parseLong(fields[3])
	if err != nil {
		log.Warn("unable to parse int for current offset. line: %s", line)
	}

	var lag int64
	lag, err = parseLong(fields[5])
	if err != nil {
		log.Warn("unable to parse int for lag. line: %s", line)
	}

	clientID, consumerAddress := parseClientIDAndConsumerAddress(fields[6])
	partitionInfo := &exporter.PartitionInfo{
		Topic:           fields[1],
		PartitionID:     fields[2],
		CurrentOffset:   currentOffset,
		Lag:             lag,
		ClientID:        clientID,
		ConsumerAddress: consumerAddress,
	}

	return partitionInfo, nil
}

func parsePartitionOutput(output string) ([]exporter.PartitionInfo, error) {
	if strings.Contains(output, "java.lang.RuntimeException") {
		return nil, fmt.Errorf("Got runtime error when executing script. Output: %s", output)
	}

	lines := strings.Split(output, "\n")[1:] /* discard header line */
	partitionInfos := make([]exporter.PartitionInfo, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		partitionInfo, err := parsePartitionInfo(line)
		if err != nil {
			errMsg := fmt.Sprintf("failed to parse a line of group description: '%s'. Error: %s", line, err)
			log.Warn(errMsg)
			return nil, errors.New(errMsg)
		}
		partitionInfos = append(partitionInfos, *partitionInfo)
	}

	if len(partitionInfos) == 0 {
		return nil, errors.New("could not find any partitions")
	}

	return partitionInfos, nil
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
