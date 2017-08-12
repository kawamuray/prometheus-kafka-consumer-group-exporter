package kafka

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	exporter "github.com/kawamuray/prometheus-kafka-consumer-group-exporter"
	"github.com/prometheus/common/log"
)

// Fields separator is different between kafka versions
// 0.9.X  => ", "
// 0.10.X => \s+
var consumerGroupCommandDescribeOutputSeparatorRegexp = regexp.MustCompile(`,?\s+`)

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

const expectedPartitionInfoFields = 7

func parsePartitionInfo(line string) (*exporter.PartitionInfo, error) {
	fields := consumerGroupCommandDescribeOutputSeparatorRegexp.Split(line, -1)
	if len(fields) != expectedPartitionInfoFields {
		return nil, fmt.Errorf("incorrect number of fields. Expected: %d Was: %d Line: %s", expectedPartitionInfoFields, len(fields), line)
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

type DefaultParser struct{}

func (p *DefaultParser) Parse(output string) ([]exporter.PartitionInfo, error) {
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
