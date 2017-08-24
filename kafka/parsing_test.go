package kafka

import (
	. "testing"

	exporter "github.com/kawamuray/prometheus-kafka-consumer-group-exporter"
)

func TestParsingPartitionTableForKafkaVersion0_10_2_1(t *T) {
	output := CommandOutput{
		Stdout: `TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG        CONSUMER-ID                                       HOST                           CLIENT-ID
xxx           4          21555284        22970821        1415537    -                                                 -                              -
yyy           1          496377          525680          29303      -                                                 -                              -
yyy           7          478173          507408          29235      -                                                 -                              -`,
	}

	expected := []exporter.PartitionInfo{
		{
			Topic:           "xxx",
			PartitionID:     "4",
			CurrentOffset:   21555284,
			Lag:             1415537,
			ClientID:        "-",
			ConsumerAddress: "-",
		},
		{
			Topic:           "yyy",
			PartitionID:     "1",
			CurrentOffset:   496377,
			Lag:             29303,
			ClientID:        "-",
			ConsumerAddress: "-",
		},
		{
			Topic:           "yyy",
			PartitionID:     "7",
			CurrentOffset:   478173,
			Lag:             29235,
			ClientID:        "-",
			ConsumerAddress: "-",
		},
	}

	comparePartitionTable(t, kafka0_10_2_1DescribeGroupParser, output, expected)
	comparePartitionTable(t, DefaultDescribeGroupParser(), output, expected)
}

func TestParsingPartitionTableForKafkaVersion0_10_0_1(t *T) {
	output := CommandOutput{
		Stdout: `GROUP                          TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             OWNER
foobar-consumer topic-A                      2          12345200        12345200        0               foobar-consumer-1-StreamThread-1-consumer_/192.168.1.1
foobar-consumer topic-A                      1          45678335        45678337        2               foobar-consumer-1-StreamThread-1-consumer_/192.168.1.2
foobar-consumer topic-A                      0          91011178        91011179        1               foobar-consumer-1-StreamThread-1-consumer_/192.168.1.3`,
	}

	expected := []exporter.PartitionInfo{
		{
			Topic:           "topic-A",
			PartitionID:     "2",
			CurrentOffset:   12345200,
			Lag:             0,
			ClientID:        "foobar-consumer-1-StreamThread-1-consumer",
			ConsumerAddress: "192.168.1.1",
		},
		{
			Topic:           "topic-A",
			PartitionID:     "1",
			CurrentOffset:   45678335,
			Lag:             2,
			ClientID:        "foobar-consumer-1-StreamThread-1-consumer",
			ConsumerAddress: "192.168.1.2",
		},
		{
			Topic:           "topic-A",
			PartitionID:     "0",
			CurrentOffset:   91011178,
			Lag:             1,
			ClientID:        "foobar-consumer-1-StreamThread-1-consumer",
			ConsumerAddress: "192.168.1.3",
		},
	}

	comparePartitionTable(t, kafka0_10_0_1DescribeGroupParser, output, expected)
	comparePartitionTable(t, DefaultDescribeGroupParser(), output, expected)
}

func TestParsingPartitionTableForKafkaVersion0_9_0_1(t *T) {
	output := CommandOutput{
		Stdout: `GROUP, TOPIC, PARTITION, CURRENT OFFSET, LOG END OFFSET, LAG, OWNER
foobar-consumer, topic-A, 2, 12344967, 12344973, 6, foobar-consumer-1-StreamThread-1-consumer_/192.168.1.1
foobar-consumer, topic-A, 1, 45678117, 45678117, 0, foobar-consumer-1-StreamThread-1-consumer_/192.168.1.2
foobar-consumer, topic-A, 0, 91011145, 91011145, 0, foobar-consumer-1-StreamThread-1-consumer_/192.168.1.3`,
	}

	expected := []exporter.PartitionInfo{
		{
			Topic:           "topic-A",
			PartitionID:     "2",
			CurrentOffset:   12344967,
			Lag:             6,
			ClientID:        "foobar-consumer-1-StreamThread-1-consumer",
			ConsumerAddress: "192.168.1.1",
		},
		{
			Topic:           "topic-A",
			PartitionID:     "1",
			CurrentOffset:   45678117,
			Lag:             0,
			ClientID:        "foobar-consumer-1-StreamThread-1-consumer",
			ConsumerAddress: "192.168.1.2",
		},
		{
			Topic:           "topic-A",
			PartitionID:     "0",
			CurrentOffset:   91011145,
			Lag:             0,
			ClientID:        "foobar-consumer-1-StreamThread-1-consumer",
			ConsumerAddress: "192.168.1.3",
		},
	}

	comparePartitionTable(t, kafka0_9_0_1DescribeGroupParser, output, expected)
	comparePartitionTable(t, DefaultDescribeGroupParser(), output, expected)
}

func comparePartitionTable(t *T, parser DescribeGroupParser, output CommandOutput, expected []exporter.PartitionInfo) {
	values, err := parser.Parse(output)
	if err != nil {
		t.Fatal("Failed parsing. Parser:", parser, "Error:", err)
	}

	if len(values) != len(expected) {
		t.Fatal("Not same lengths. Was:", len(values), "Was:", len(expected))
	}
	for i, value := range values {
		comparePartitionInfo(t, parser, value, expected[i])
	}
}

func comparePartitionInfo(t *T, parser DescribeGroupParser, value, expected exporter.PartitionInfo) {
	if value, expected := value.Topic, expected.Topic; expected != value {
		t.Error("Wrong topic. Parser:", parser, "Expected:", expected, "Was:", value)
	}
	if value, expected := value.PartitionID, expected.PartitionID; expected != value {
		t.Error("Wrong PartitionID. Parser:", parser, "Expected:", expected, "Was:", value)
	}
	if value, expected := value.CurrentOffset, expected.CurrentOffset; expected != value {
		t.Error("Wrong CurrentOffset. Parser:", parser, "Expected:", expected, "Was:", value)
	}
	if value, expected := value.Lag, expected.Lag; expected != value {
		t.Error("Wrong Lag. Parser:", parser, "Expected:", expected, "Was:", value)
	}
	if value, expected := value.ClientID, expected.ClientID; expected != value {
		t.Error("Wrong ClientID. Parser:", parser, "Expected:", expected, "Was:", value)
	}
	if value, expected := value.ConsumerAddress, expected.ConsumerAddress; expected != value {
		t.Error("Wrong ConsumerAddress. Parser:", parser, "Expected:", expected, "Was:", value)
	}
}

// Test Kafka 10.0.0 script unable to connect to describe group.
func TestKafka10_0_0ScriptDescribeGroupErrorOutput(t *T) {
	cannotConnectOutput := `Error while executing consumer group command Request GROUP_COORDINATOR failed on brokers List(localhost:9042 (id: -1 rack: null))
java.lang.RuntimeException: Request GROUP_COORDINATOR failed on brokers List(localhost:9042 (id: -1 rack: null))
	at kafka.admin.AdminClient.sendAnyNode(AdminClient.scala:67)
	at kafka.admin.AdminClient.findCoordinator(AdminClient.scala:72)
	at kafka.admin.AdminClient.describeGroup(AdminClient.scala:125)
	at kafka.admin.AdminClient.describeConsumerGroup(AdminClient.scala:147)
	at kafka.admin.ConsumerGroupCommand$KafkaConsumerGroupService.describeGroup(ConsumerGroupCommand.scala:315)
	at kafka.admin.ConsumerGroupCommand$ConsumerGroupService$class.describe(ConsumerGroupCommand.scala:86)
	at kafka.admin.ConsumerGroupCommand$KafkaConsumerGroupService.describe(ConsumerGroupCommand.scala:303)
	at kafka.admin.ConsumerGroupCommand$.main(ConsumerGroupCommand.scala:65)
	at kafka.admin.ConsumerGroupCommand.main(ConsumerGroupCommand.scala)

`
	if _, err := DefaultDescribeGroupParser().Parse(CommandOutput{Stdout: cannotConnectOutput}); err == nil {
		t.Error("Expected to get an error due to internal error in Kafka script.")
	}
}

// Test Kafka 10.0.0 script unable to connect to list groups.
func TestKafka10_0_0ScriptListGroupsErrorOutput(t *T) {
	cannotConnectOutput := `Error while executing consumer group command Request METADATA failed on brokers List(localhost:9042 (id: -1 rack: null))
java.lang.RuntimeException: Request METADATA failed on brokers List(localhost:9042 (id: -1 rack: null))
	at kafka.admin.AdminClient.sendAnyNode(AdminClient.scala:67)
	at kafka.admin.AdminClient.findAllBrokers(AdminClient.scala:87)
	at kafka.admin.AdminClient.listAllGroups(AdminClient.scala:96)
	at kafka.admin.AdminClient.listAllGroupsFlattened(AdminClient.scala:117)
	at kafka.admin.AdminClient.listAllConsumerGroupsFlattened(AdminClient.scala:121)
	at kafka.admin.ConsumerGroupCommand$KafkaConsumerGroupService.list(ConsumerGroupCommand.scala:311)
	at kafka.admin.ConsumerGroupCommand$.main(ConsumerGroupCommand.scala:63)
	at kafka.admin.ConsumerGroupCommand.main(ConsumerGroupCommand.scala)

`
	if _, err := DefaultDescribeGroupParser().Parse(CommandOutput{Stdout: cannotConnectOutput}); err == nil {
		t.Error("Expected to get an error due to internal error in Kafka script.")
	}
}

func TestInterfaceImplementation(t *T) {
	var _ exporter.ConsumerGroupInfoClient = (*ConsumerGroupsCommandClient)(nil)
}
