package kafka

import (
	. "testing"

	exporter "github.com/kawamuray/prometheus-kafka-consumer-group-exporter"
)

func TestParsingPartitionTableForKafkaVersion0_10_2_1(t *T) {
	output := CommandOutput{
		Stderr: "Note: This will only show information about consumers that use the Java consumer API (non-ZooKeeper-based consumers).\n",
		Stdout: `
TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG        CONSUMER-ID                                       HOST                           CLIENT-ID
topic1           0          3545            3547            2          consumer-1-583b2298-f285-4696-ba0e-576109284592   /10.21.95.43                   consumer-1
topic2-detail 0          0               0               0          consumer-1-583b2298-f285-4696-ba0e-576109284592   /10.21.95.43                   consumer-1
topic3            0          45              45              0          consumer-1-583b2298-f285-4696-ba0e-576109284592   /10.21.95.43                   consumer-1`,
	}

	expected := []exporter.PartitionInfo{
		{
			Topic:           "topic1",
			PartitionID:     "0",
			CurrentOffset:   3545,
			Lag:             2,
			ClientID:        "consumer-1",
			ConsumerAddress: "10.21.95.43",
		},
		{
			Topic:           "topic2-detail",
			PartitionID:     "0",
			CurrentOffset:   0,
			Lag:             0,
			ClientID:        "consumer-1",
			ConsumerAddress: "10.21.95.43",
		},
		{
			Topic:           "topic3",
			PartitionID:     "0",
			CurrentOffset:   45,
			Lag:             0,
			ClientID:        "consumer-1",
			ConsumerAddress: "10.21.95.43",
		},
	}

	t.Run("kafka0_10_2_1DescribeGroupParser", func(t *T) {
		comparePartitionTable(t, kafka0_10_2_1DescribeGroupParser, output, expected)
	})
	t.Run("DefaultDescribeGroupParser", func(t *T) {
		comparePartitionTable(t, DefaultDescribeGroupParser(), output, expected)
	})
}

func TestParsingPartitionTableWithMissingValuesConsumerIdForKafkaVersion0_10_2_1(t *T) {
	output := CommandOutput{
        Stderr: "NOTE: When there is no active consumer CONSUMER_ID, HOST and CLIENT-ID is -\n",
		Stdout: `
TOPIC           PARTITION   CURRENT-OFFSET  LOG-END-OFFSET  LAG     CONSUMER-ID     HOST        CLIENT-ID
TEST            0           109             134             25      -               -           -`,
	}

	expected := []exporter.PartitionInfo{
		{
			Topic:           "TEST",
			PartitionID:     "0",
			CurrentOffset:   109,
			Lag:             25,
			ClientID:        "-",
			ConsumerAddress: "-",
		},
	}

	t.Run("kafka0_10_2_1DescribeGroupParser", func(t *T) {
		comparePartitionTable(t, kafka0_10_2_1DescribeGroupParser, output, expected)
	})
	t.Run("DefaultDescribeGroupParser", func(t *T) {
		comparePartitionTable(t, DefaultDescribeGroupParser(), output, expected)
	})
}

func TestParsingPartitionTableWithMissingValuesForKafkaVersion0_10_2_1(t *T) {
	output := CommandOutput{
		Stderr: "Note: This will only show information about consumers that use the Java consumer API (non-ZooKeeper-based consumers).\n",
		Stdout: `
TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG        CONSUMER-ID                                       HOST                           CLIENT-ID
UPDATE_TRANSACTIONS            0          12              12              0          consumer-1-6e9f2372-79fc-4f60-8c0e-3fdb995bad29   /10.1.2.3                  consumer-1
-                              -          -               -               -          consumer-1-868b1bd1-824c-4d43-a4a6-5af0f331452a   /10.2.3.4                  consumer-1
-                              -          -               -               -          consumer-1-d6a6c9b7-fd62-46b7-996f-2cfe0a956262   /10.5.6.7                  consumer-1`,
	}

	expected := []exporter.PartitionInfo{
		{
			Topic:           "UPDATE_TRANSACTIONS",
			PartitionID:     "0",
			CurrentOffset:   12,
			Lag:             0,
			ClientID:        "consumer-1",
			ConsumerAddress: "10.1.2.3",
		},
	}

	t.Run("kafka0_10_2_1DescribeGroupParser", func(t *T) {
		comparePartitionTable(t, kafka0_10_2_1DescribeGroupParser, output, expected)
	})
	t.Run("DefaultDescribeGroupParser", func(t *T) {
		comparePartitionTable(t, DefaultDescribeGroupParser(), output, expected)
	})
}

func TestParsingPartitionTableWithLongConsumerIDForKafkaVersion0_10_2_1(t *T) {
	output := CommandOutput{
		Stderr: "Note: This will only show information about consumers that use the Java consumer API (non-ZooKeeper-based consumers).\n",
		Stdout: `
TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG        CONSUMER-ID                                       HOST                           CLIENT-ID
topic1        0          2               2               0          looong-name-consumer-e12431ea-8ba0-420c-9be7-70c30840f59a/11.111.111.111                looong-name-consumer
topic2        0          2               3               4          looong-name-consumer-e12431ea-8ba0-420c-9be7-70c30840f59a/11.111.111.111                looong-name-consumer`,
	}

	expected := []exporter.PartitionInfo{
		{
			Topic:           "topic1",
			PartitionID:     "0",
			CurrentOffset:   2,
			Lag:             0,
			ClientID:        "looong-name-consumer",
			ConsumerAddress: "11.111.111.111",
		},
		{
			Topic:           "topic2",
			PartitionID:     "0",
			CurrentOffset:   2,
			Lag:             4,
			ClientID:        "looong-name-consumer",
			ConsumerAddress: "11.111.111.111",
		},
	}

	t.Run("kafka0_10_2_1DescribeGroupParser", func(t *T) {
		comparePartitionTable(t, kafka0_10_2_1DescribeGroupParser, output, expected)
	})
	t.Run("DefaultDescribeGroupParser", func(t *T) {
		comparePartitionTable(t, DefaultDescribeGroupParser(), output, expected)
	})
}

func TestParsingPartitionTableForKafkaVersion0_10_1_X(t *T) {
	output := CommandOutput{
		Stdout: `GROUP                          TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             OWNER
group1             requests                       0         1176417636761   1176419294709   1657948         consumer1_/1.1.1.5
group1             requests                       1         1144248140115   1144249685348   1545233         consumer1_/1.1.1.18
group1             requests                       2         1101749049926   1101751118906   2068980         consumer1_/1.1.1.23`,
	}

	expected := []exporter.PartitionInfo{
		{
			Topic:           "requests",
			PartitionID:     "0",
			CurrentOffset:   1176417636761,
			Lag:             1657948,
			ClientID:        "consumer1",
			ConsumerAddress: "1.1.1.5",
		},
		{
			Topic:           "requests",
			PartitionID:     "1",
			CurrentOffset:   1144248140115,
			Lag:             1545233,
			ClientID:        "consumer1",
			ConsumerAddress: "1.1.1.18",
		},
		{
			Topic:           "requests",
			PartitionID:     "2",
			CurrentOffset:   1101749049926,
			Lag:             2068980,
			ClientID:        "consumer1",
			ConsumerAddress: "1.1.1.23",
		},
	}

	t.Run("kafka0_10_1_XDescribeGroupParser", func(t *T) {
		comparePartitionTable(t, kafka0_10_1DescribeGroupParser, output, expected)
	})
	t.Run("DefaultDescribeGroupParser", func(t *T) {
		comparePartitionTable(t, DefaultDescribeGroupParser(), output, expected)
	})
}

func TestParsingNoActiveMemberErrorForKafkaVersion0_10_2_1(t *T) {
	output := CommandOutput{
		Stderr: "Consumer group '$groupId' has no active members.\n",
	}
	t.Run("kafka0_10_2_1DescribeGroupParser", func(t *T) {
		assertErrorParsing(t, kafka0_10_2_1DescribeGroupParser, output)
	})
	t.Run("DefaultDescribeGroupParser", func(t *T) {
		assertErrorParsing(t, DefaultDescribeGroupParser(), output)
	})
}

func TestParsingWhenRebalancingErrorForKafkaVersion0_10_2_1(t *T) {
	output := CommandOutput{
		Stderr: "Warning: Consumer group '$groupId' is rebalancing.\n",
	}
	t.Run("kafka0_10_2_1DescribeGroupParser", func(t *T) {
		assertErrorParsing(t, kafka0_10_2_1DescribeGroupParser, output)
	})
	t.Run("DefaultDescribeGroupParser", func(t *T) {
		assertErrorParsing(t, DefaultDescribeGroupParser(), output)
	})
}

func assertErrorParsing(t *T, parser DescribeGroupParser, output CommandOutput) {
	_, err := parser.Parse(output)
	if err == nil {
		t.Error("Expected an error.")
	}
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

	t.Run("kafka0_10_0_1DescribeGroupParser", func(t *T) {
		comparePartitionTable(t, kafka0_10_0_1DescribeGroupParser, output, expected)
	})
	t.Run("DefaultDescribeGroupParser", func(t *T) {
		comparePartitionTable(t, DefaultDescribeGroupParser(), output, expected)
	})
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

	t.Run("kafka0_9_0_1DescribeGroupParser", func(t *T) {
		comparePartitionTable(t, kafka0_9_0_1DescribeGroupParser, output, expected)
	})
	t.Run("DefaultDescribeGroupParser", func(t *T) {
		comparePartitionTable(t, DefaultDescribeGroupParser(), output, expected)
	})
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
