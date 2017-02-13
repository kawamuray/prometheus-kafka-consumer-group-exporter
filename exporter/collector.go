package exporter

import (
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	log "github.com/Sirupsen/logrus"
	"github.com/kawamuray/prometheus-exporter-harness/harness"
	"github.com/prometheus/client_golang/prometheus"
)

// Fields separator is different between kafka versions
// 0.9.X  => ", "
// 0.10.X => \s+
var ConsumerGroupCommandDescribeOutputSeparatorRegexp = regexp.MustCompile(`,?\s+`)

type Collector struct {
	BootstrapServers         string
	ConsumerGroupCommandPath string
	fetchBarrier             uintptr
	updateLock               sync.Mutex
	lastValue                map[string][]*PartitionInfo
}

func NewCollector(bootstrapServers, consumerGroupCommandPath string) *Collector {
	return &Collector{
		BootstrapServers:         bootstrapServers,
		ConsumerGroupCommandPath: consumerGroupCommandPath,
	}
}

func (col *Collector) execConsumerGroupCommand(args ...string) (string, error) {
	allArgs := append([]string{"--new-consumer", "--bootstrap-server", col.BootstrapServers}, args...)
	cmd := exec.Command(col.ConsumerGroupCommandPath, allArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	return string(output), nil
}

func (col *Collector) groupList() ([]string, error) {
	output, err := col.execConsumerGroupCommand("--list")
	if err != nil {
		return nil, err
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

func parseClientIdAndConsumerAddress(clientIdAndConsumerAddress string) (string, string) {
	const Separator = "_/"
	splitPoint := strings.LastIndex(clientIdAndConsumerAddress, Separator)
	if splitPoint == -1 {
		return clientIdAndConsumerAddress, ""
	}
	return clientIdAndConsumerAddress[:splitPoint], clientIdAndConsumerAddress[splitPoint+len(Separator):]
}

func parseLong(value string) int64 {
	longVal, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return -1
	}
	return longVal
}

type PartitionInfo struct {
	Topic           string
	PartitionId     string
	CurrentOffset   int64
	Lag             int64
	ClientId        string
	ConsumerAddress string
}

func parsePartitionInfo(line string) (*PartitionInfo, error) {
	fields := ConsumerGroupCommandDescribeOutputSeparatorRegexp.Split(line, -1)
	if len(fields) != 7 {
		return nil, fmt.Errorf("malformed line")
	}

	clientId, consumerAddress := parseClientIdAndConsumerAddress(fields[6])
	partitionInfo := &PartitionInfo{
		Topic:           fields[1],
		PartitionId:     fields[2],
		CurrentOffset:   parseLong(fields[3]),
		Lag:             parseLong(fields[5]),
		ClientId:        clientId,
		ConsumerAddress: consumerAddress,
	}

	return partitionInfo, nil
}

func parsePartitionOutput(output string) ([]*PartitionInfo, error) {
	lines := strings.Split(output, "\n")[1:] /* discard header line */
	partitionInfos := make([]*PartitionInfo, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		partitionInfo, err := parsePartitionInfo(line)
		if err != nil {
			log.Warnf("failed to parse a line of group description: '%s'", line)
			continue
		}
		partitionInfos = append(partitionInfos, partitionInfo)
	}

	return partitionInfos, nil
}

func (col *Collector) describeGroup(group string) ([]*PartitionInfo, error) {
	output, err := col.execConsumerGroupCommand("--describe", "--group", group)
	if err != nil {
		return nil, err
	}
	return parsePartitionOutput(output)
}

func (col *Collector) maybeUpdate() {
	// Prevent multiple queries run simultaneously
	if !atomic.CompareAndSwapUintptr(&col.fetchBarrier, 0, 1) {
		return
	}
	defer atomic.CompareAndSwapUintptr(&col.fetchBarrier, 1, 0)

	nextValue := make(map[string][]*PartitionInfo)

	groups, err := col.groupList()
	if err != nil {
		log.Errorf("failed to get group list: %s", err)
		return
	}

	for _, group := range groups {
		partitionInfos, err := col.describeGroup(group)
		if err != nil {
			log.Errorf("failed to get group description of %s: %s", group, err)
			continue
		}
		nextValue[group] = partitionInfos
	}

	col.updateLock.Lock()
	defer col.updateLock.Unlock()
	col.lastValue = nextValue
}

func (col *Collector) Collect(reg *harness.MetricRegistry) {
	col.updateLock.Lock()
	defer col.updateLock.Unlock()

	for group, partitionInfos := range col.lastValue {
		for _, partitionInfo := range partitionInfos {
			labels := map[string]string{
				"group_id":         group,
				"consumer_address": partitionInfo.ConsumerAddress,
				"client_id":        partitionInfo.ClientId,
				"topic":            partitionInfo.Topic,
				"partition":        partitionInfo.PartitionId,
			}

			reg.Get(MetricCurrentOffset).(*prometheus.GaugeVec).With(labels).Set(float64(partitionInfo.CurrentOffset))
			reg.Get(MetricOffsetLag).(*prometheus.GaugeVec).With(labels).Set(float64(partitionInfo.Lag))
		}
	}

	// Prepare next update(if necessary)
	go col.maybeUpdate()
}
