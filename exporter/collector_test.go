package exporter

import (
	. "testing"
)

func TestParsePartitionTableForKafkaVersion0_10_0_1(t *T) {
	partitions, err := parsePartitionOutput(`GROUP                          TOPIC                          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             OWNER
foobar-consumer topic-A                      2          12345200        12345200        0               foobar-consumer-1-StreamThread-1-consumer_/192.168.1.1
foobar-consumer topic-A                      1          45678335        45678337        2               foobar-consumer-1-StreamThread-1-consumer_/192.168.1.2
foobar-consumer topic-A                      0          91011178        91011179        1               foobar-consumer-1-StreamThread-1-consumer_/192.168.1.3`)

	if err != nil {
		t.Fatal(err)
	}

	expected := []*PartitionInfo{
		{
			Topic:           "topic-A",
			PartitionId:     "2",
			CurrentOffset:   12345200,
			Lag:             0,
			ClientId:        "foobar-consumer-1-StreamThread-1-consumer",
			ConsumerAddress: "192.168.1.1",
		},
		{
			Topic:           "topic-A",
			PartitionId:     "1",
			CurrentOffset:   45678335,
			Lag:             2,
			ClientId:        "foobar-consumer-1-StreamThread-1-consumer",
			ConsumerAddress: "192.168.1.2",
		},
		{
			Topic:           "topic-A",
			PartitionId:     "0",
			CurrentOffset:   91011178,
			Lag:             1,
			ClientId:        "foobar-consumer-1-StreamThread-1-consumer",
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

	expected := []*PartitionInfo{
		{
			Topic:           "topic-A",
			PartitionId:     "2",
			CurrentOffset:   12344967,
			Lag:             6,
			ClientId:        "foobar-consumer-1-StreamThread-1-consumer",
			ConsumerAddress: "192.168.1.1",
		},
		{
			Topic:           "topic-A",
			PartitionId:     "1",
			CurrentOffset:   45678117,
			Lag:             0,
			ClientId:        "foobar-consumer-1-StreamThread-1-consumer",
			ConsumerAddress: "192.168.1.2",
		},
		{
			Topic:           "topic-A",
			PartitionId:     "0",
			CurrentOffset:   91011145,
			Lag:             0,
			ClientId:        "foobar-consumer-1-StreamThread-1-consumer",
			ConsumerAddress: "192.168.1.3",
		},
	}

	comparePartitionTable(t, partitions, expected)
}
func comparePartitionTable(t *T, values, expected []*PartitionInfo) {
	if len(values) != len(expected) {
		t.Fatal("Not same lengths. Was:", len(values), "Was:", len(expected))
	}
	for i, value := range values {
		comparePartitionInfo(t, value, expected[i])
	}
}

func comparePartitionInfo(t *T, value, expected *PartitionInfo) {
	if value, expected := value.Topic, expected.Topic; expected != value {
		t.Error("Wrong topic. Expected:", expected, "Was:", value)
	}
	if value, expected := value.PartitionId, expected.PartitionId; expected != value {
		t.Error("Wrong PartitionId. Expected:", expected, "Was:", value)
	}
	if value, expected := value.CurrentOffset, expected.CurrentOffset; expected != value {
		t.Error("Wrong CurrentOffset. Expected:", expected, "Was:", value)
	}
	if value, expected := value.Lag, expected.Lag; expected != value {
		t.Error("Wrong Lag. Expected:", expected, "Was:", value)
	}
	if value, expected := value.ClientId, expected.ClientId; expected != value {
		t.Error("Wrong ClientId. Expected:", expected, "Was:", value)
	}
	if value, expected := value.ConsumerAddress, expected.ConsumerAddress; expected != value {
		t.Error("Wrong ConsumerAddress. Expected:", expected, "Was:", value)
	}
}
