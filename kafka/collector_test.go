package kafka

import (
	"context"
	"os"
	. "testing"
	"time"

	exporter "github.com/kawamuray/prometheus-kafka-consumer-group-exporter"
)

func TestParsePartitionTableForKafkaVersion0_10_0_1(t *T) {
	partitions, err := parsePartitionOutput(`GROUP                          TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             OWNER
foobar-consumer topic-A                      2          12345200        12345200        0               foobar-consumer-1-StreamThread-1-consumer_/192.168.1.1
foobar-consumer topic-A                      1          45678335        45678337        2               foobar-consumer-1-StreamThread-1-consumer_/192.168.1.2
foobar-consumer topic-A                      0          91011178        91011179        1               foobar-consumer-1-StreamThread-1-consumer_/192.168.1.3`)

	if err != nil {
		t.Fatal(err)
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

	comparePartitionTable(t, partitions, expected)
}

func TestParsePartitionTableForKafkaVersion0_9_0_1(t *T) {
	partitions, err := parsePartitionOutput(`GROUP, TOPIC, PARTITION, CURRENT OFFSET, LOG END OFFSET, LAG, OWNER
foobar-consumer, topic-A, 2, 12344967, 12344973, 6, foobar-consumer-1-StreamThread-1-consumer_/192.168.1.1
foobar-consumer, topic-A, 1, 45678117, 45678117, 0, foobar-consumer-1-StreamThread-1-consumer_/192.168.1.2
foobar-consumer, topic-A, 0, 91011145, 91011145, 0, foobar-consumer-1-StreamThread-1-consumer_/192.168.1.3`)

	if err != nil {
		t.Fatal(err)
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

	comparePartitionTable(t, partitions, expected)
}
func comparePartitionTable(t *T, values, expected []exporter.PartitionInfo) {
	if len(values) != len(expected) {
		t.Fatal("Not same lengths. Was:", len(values), "Was:", len(expected))
	}
	for i, value := range values {
		comparePartitionInfo(t, value, expected[i])
	}
}

func comparePartitionInfo(t *T, value, expected exporter.PartitionInfo) {
	if value, expected := value.Topic, expected.Topic; expected != value {
		t.Error("Wrong topic. Expected:", expected, "Was:", value)
	}
	if value, expected := value.PartitionID, expected.PartitionID; expected != value {
		t.Error("Wrong PartitionID. Expected:", expected, "Was:", value)
	}
	if value, expected := value.CurrentOffset, expected.CurrentOffset; expected != value {
		t.Error("Wrong CurrentOffset. Expected:", expected, "Was:", value)
	}
	if value, expected := value.Lag, expected.Lag; expected != value {
		t.Error("Wrong Lag. Expected:", expected, "Was:", value)
	}
	if value, expected := value.ClientID, expected.ClientID; expected != value {
		t.Error("Wrong ClientID. Expected:", expected, "Was:", value)
	}
	if value, expected := value.ConsumerAddress, expected.ConsumerAddress; expected != value {
		t.Error("Wrong ConsumerAddress. Expected:", expected, "Was:", value)
	}
}

func TestKafkaPartitionExecution(t *T) {
	// Check prerequisites

	scriptPath := os.Getenv("KAFKA_CONSUMER_GROUP_SCRIPT")
	if scriptPath == "" {
		t.Skip("Please define KAFKA_CONSUMER_GROUP_SCRIPT environment flag.")
	}

	bootstrapServer := os.Getenv("KAFKA_BOOTSTRAP_SERVER")
	if bootstrapServer == "" {
		t.Skip("Please define KAFKA_BOOTSTRAP_SERVER environment flag.")
	}

	// Build consumer.

	consumer := ConsumerGroupsCommandClient{
		bootstrapServer,
		scriptPath,
	}

	// Test the consumer

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	groups, err := consumer.Groups(ctx)
	cancel()
	if err != nil {
		t.Fatal("Could not list groups:", err)
	}

	for _, groupname := range groups {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		partitions, err := consumer.DescribeGroup(ctx, groupname)
		cancel()
		if err != nil {
			t.Fatal("Could get group description for group", groupname, "Error:", err)
		}

		// Basic partition sanity checks.

		for _, partition := range partitions {
			checkPartitionLooksSane(t, groupname, partition)
		}
	}

}

func checkPartitionLooksSane(t *T, groupname string, partition exporter.PartitionInfo) {
	checkFieldIsNotEmpty(t, groupname, partition.Topic, "Topic")
	checkFieldIsNotEmpty(t, groupname, partition.PartitionID, "PartitionID")
	checkFieldIsNotEmpty(t, groupname, partition.ClientID, "ClientID")
	checkFieldIsNotEmpty(t, groupname, partition.ConsumerAddress, "ConsumerAddress")
}

func checkFieldIsNotEmpty(t *T, groupname, value, fieldname string) {
	if value != "" {
		t.Error(fieldname, "missing for consumer group:", groupname)
	}
}

func TestKafkaDownFails(t *T) {
	// Check prerequisites

	brokenServer := "localhost:9042"

	scriptPath := os.Getenv("KAFKA_CONSUMER_GROUP_SCRIPT")
	if scriptPath == "" {
		t.Skip("Please define KAFKA_CONSUMER_GROUP_SCRIPT environment flag.")
	}

	bootstrapServer := os.Getenv("KAFKA_BOOTSTRAP_SERVER")
	if bootstrapServer == brokenServer {
		t.Fatal("Broken server is defined in KAFKA_BOOTSTRAP_SERVER. It's not expected to be accessible.")
	}

	// Build consumer.

	consumer := ConsumerGroupsCommandClient{
		brokenServer,
		scriptPath,
	}

	// Test the consumer

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	_, err := consumer.Groups(ctx)
	cancel()
	if err == nil {
		t.Error("Expected an error when not being to connect to to Kafka.")
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
	if _, err := parsePartitionOutput(cannotConnectOutput); err == nil {
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
	if _, err := parseGroups(cannotConnectOutput); err == nil {
		t.Error("Expected to get an error due to internal error in Kafka script.")
	}
}

func TestInterfaceImplementation(t *T) {
	var _ exporter.ConsumerGroupInfoClient = (*ConsumerGroupsCommandClient)(nil)
}
