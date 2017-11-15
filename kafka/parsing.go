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

const missingColumnValue = "-"

var errLagMissing = errors.New("lag is missing")

func parseGroups(output CommandOutput) ([]string, error) {
	if strings.Contains(output.Stderr, "java.lang.RuntimeException") {
		return nil, fmt.Errorf("Got runtime error when executing script. Output: %s", output)
	}

	lines := strings.Split(output.Stdout, "\n")
	groups := make([]string, 0, len(lines))
	for _, line := range lines {
		if line != "" {
			groups = append(groups, line)
		}
	}

	return groups, nil
}

type regexpParser struct {
	// header is the expected format of the first line. Used as fingerprint to distinguish if this parser is the right one.
	header *regexp.Regexp
	// line is the regexp used for the remaining lines of the output (as long as Header matched correctly).
	line *regexp.Regexp
	//
	indexByName map[string]int
}

func newRegexpParser(header, line *regexp.Regexp) (*regexpParser, error) {
	indexByName := make(map[string]int)
	for i, name := range line.SubexpNames() {
		indexByName[name] = i
	}

	for _, field := range []string{"topic", "partitionId", "currentOffset", "lag", "clientId", "consumerAddress"} {
		if _, exists := indexByName[field]; !exists {
			return nil, fmt.Errorf("line regexp missing '%s' capturing group", field)
		}
	}

	return &regexpParser{header.Copy(), line.Copy(), indexByName}, nil
}

func (p *regexpParser) Parse(output CommandOutput) ([]exporter.PartitionInfo, error) {
	lines := removeEmptyLines(strings.Split(output.Stdout, "\n"))
	if len(lines) == 0 {
		return nil, errors.New("empty output. stderr: " + output.Stderr)
	}
	headerLine := lines[0]
	dataLines := lines[1:]

	if !p.header.MatchString(headerLine) {
		return nil, errors.New("incorrect header")
	}

	partitions := make([]exporter.PartitionInfo, 0, len(dataLines))
	var err error
	var partition *exporter.PartitionInfo
	for _, line := range dataLines {
		partition, err = p.parseLine(line)
		if err == errLagMissing {
			err = nil
			continue
		}
		if err != nil {
			break
		}
		partitions = append(partitions, *partition)
	}
	return partitions, err
}

func removeEmptyLines(s []string) []string {
	output := make([]string, 0, len(s))
	for _, e := range s {
		if strings.Trim(e, " ") != "" {
			output = append(output, e)
		}
	}
	return output
}

func (p *regexpParser) String() string {
	return fmt.Sprintf("regexpParser{header: `%s`, line: `%s`}", p.header, p.line)
}

func (p *regexpParser) parseLine(line string) (*exporter.PartitionInfo, error) {
	matches := p.line.FindStringSubmatch(line)

	var err error

	var lag int64
	if lagIndex := p.indexByName["lag"]; lagIndex >= len(matches) {
		lag = -1
		err = fmt.Errorf("unable to find current offset field. line: %s", line)
		log.Warn(err.Error())
	} else if matches[lagIndex] == missingColumnValue {
		// This happens when there are more consumers than partitions.
		return nil, errLagMissing
	} else {
		lag, err = parseLong(matches[lagIndex])
	}
	if err != nil {
		log.Warn("unable to parse int for lag. line: %s", line)
	}

	var currentOffset int64
	if currentOffsetIndex := p.indexByName["currentOffset"]; currentOffsetIndex >= len(matches) {
		currentOffset = -1
		err = fmt.Errorf("unable to find current offset field. Line: %s", line)
		log.Warn(err.Error())
	} else {
		currentOffset, err = parseLong(matches[currentOffsetIndex])
	}
	if err != nil {
		log.Warn("unable to parse int for current offset. Line: %s", line)
	}

	partitionInfo := &exporter.PartitionInfo{
		Topic:           matches[p.indexByName["topic"]],
		PartitionID:     matches[p.indexByName["partitionId"]],
		CurrentOffset:   currentOffset,
		Lag:             lag,
		ClientID:        matches[p.indexByName["clientId"]],
		ConsumerAddress: matches[p.indexByName["consumerAddress"]],
	}

	return partitionInfo, err
}

func parseLong(value string) (int64, error) {
	longVal, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return -1, err
	}
	return longVal, nil
}

// Parsers for "describe group" output.
var (
	// Parser for Kafka 0.10.2.1. Since we are unsure if the column widths are dynamic, we are using `\s+` for delimiters.
	kafka0_10_2_1DescribeGroupParser = mustBuildNewRegexpParser(
		regexp.MustCompile(`TOPIC\s+PARTITION\s+CURRENT-OFFSET\s+LOG-END-OFFSET\s+LAG\s+CONSUMER-ID\s+HOST\s+CLIENT-ID`),
		regexp.MustCompile(`(?P<topic>[a-zA-Z0-9\\._\\-]+)\s+(?P<partitionId>\d+|-)\s+(?P<currentOffset>\d+|-)\s+(\d+|-)\s+(?P<lag>\d+|-)\s+(?P<consumerId>[^/\s]+)\s*/?(?P<consumerAddress>\S+)\s+(?P<clientId>\S+)`),
	)

	// Parser for Kafka 0.10.1.X.
	kafka0_10_1DescribeGroupParser = mustBuildNewRegexpParser(
		regexp.MustCompile(`GROUP\s+TOPIC\s+PARTITION\s+CURRENT-OFFSET\s+LOG-END-OFFSET\s+LAG\s+OWNER`),
		regexp.MustCompile(`.+\s+(?P<topic>[a-zA-Z0-9\\._\\-]+)\s+(?P<partitionId>\d+)\s+(?P<currentOffset>\d+)\s+\d+\s+(?P<lag>\d+)\s+(?P<clientId>\S+)_/(?P<consumerAddress>.+)`),
	)

	// Parser for Kafka 0.10.0.1. Since we are unsure if the column widths are dynamic, we are using `\s+` for delimiters.
	kafka0_10_0_1DescribeGroupParser = mustBuildNewRegexpParser(
		regexp.MustCompile(`GROUP\s+TOPIC\s+PARTITION\s+CURRENT-OFFSET\s+LOG-END-OFFSET\s+LAG\s+OWNER`),
		regexp.MustCompile(`.+\s+(?P<topic>[a-zA-Z0-9\\._\\-]+)\s+(?P<partitionId>\d+)\s+(?P<currentOffset>\d+)\s+\d+\s+(?P<lag>\d+)\s+(?P<clientId>\S+)_/(?P<consumerAddress>.+)`),
	)
	kafka0_9_0_1DescribeGroupParser = mustBuildNewRegexpParser(
		regexp.MustCompile("GROUP, TOPIC, PARTITION, CURRENT OFFSET, LOG END OFFSET, LAG, OWNER"),
		regexp.MustCompile(`[^,]+, (?P<topic>[a-zA-Z0-9\\._\\-]+), (?P<partitionId>\d+), (?P<currentOffset>\d+), \d+, (?P<lag>\d+), (?P<clientId>.+)_/(?P<consumerAddress>.+)`),
	)
)

func mustBuildNewRegexpParser(header, line *regexp.Regexp) *regexpParser {
	parser, err := newRegexpParser(header, line)
	if err != nil {
		log.Fatal(err)
	}
	return parser
}

// DefaultDescribeGroupParser returns a DelegatingParser consisting of all formats known.
func DefaultDescribeGroupParser() *DelegatingParser {
	return &DelegatingParser{
		[]DescribeGroupParser{
			kafka0_9_0_1DescribeGroupParser,
			kafka0_10_0_1DescribeGroupParser,
			kafka0_10_1DescribeGroupParser,
			kafka0_10_2_1DescribeGroupParser,
		},
	}
}

// DelegatingParser is a parser that tries multiple parser returning the first
// succesful parsed result.
type DelegatingParser struct {
	Parsers []DescribeGroupParser
}

// Parse parses the output. It tries each Parser in order, returning an error
// if all fails.
func (p *DelegatingParser) Parse(output CommandOutput) ([]exporter.PartitionInfo, error) {
	var err error
	var partitions []exporter.PartitionInfo

	for _, parser := range p.Parsers {
		partitions, err = parser.Parse(output)
		if err == nil {
			return partitions, err
		}
	}

	return nil, errors.New("no parser could parse the output")
}

func (p *DelegatingParser) String() string {
	return "DefaultParser"
}
