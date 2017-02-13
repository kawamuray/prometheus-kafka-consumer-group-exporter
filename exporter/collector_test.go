package exporter

import (
	. "testing"
)

func TestParsePartitionInfoForKafkaVersion0_9_0_1(t *T) {
	info, err := parsePartitionInfo("foobar-consumer, topic-A, 2, 12344967, 12344973, 6, foobar-consumer-1-StreamThread-1-consumer_/192.168.1.1")
	if err != nil {
		t.Fatal(err)
	}

	if value, expected := info.Topic, "topic-A"; expected != value {
		t.Error("Wrong topic. Expected:", expected, "Was:", value)
	}
	if value, expected := info.PartitionId, "2"; expected != value {
		t.Error("Wrong PartitionId. Expected:", expected, "Was:", value)
	}
	if value, expected := info.CurrentOffset, int64(12344967); expected != value {
		t.Error("Wrong CurrentOffset. Expected:", expected, "Was:", value)
	}
	if value, expected := info.Lag, int64(6); expected != value {
		t.Error("Wrong Lag. Expected:", expected, "Was:", value)
	}
	if value, expected := info.ClientId, "foobar-consumer-1-StreamThread-1-consumer"; expected != value {
		t.Error("Wrong ClientId. Expected:", expected, "Was:", value)
	}
	if value, expected := info.ConsumerAddress, "192.168.1.1"; expected != value {
		t.Error("Wrong ConsumerAddress. Expected:", expected, "Was:", value)
	}
}

func TestParsePartitionInfoForKafkaVersion0_10_0_1(t *T) {
	info, err := parsePartitionInfo("foobar-consumer topic-A                      1          45678335        45678337        2               foobar-consumer-1-StreamThread-1-consumer_/192.168.1.2")
	if err != nil {
		t.Fatal(err)
	}

	if value, expected := info.Topic, "topic-A"; expected != value {
		t.Error("Wrong topic. Expected:", expected, "Was:", value)
	}
	if value, expected := info.PartitionId, "1"; expected != value {
		t.Error("Wrong PartitionId. Expected:", expected, "Was:", value)
	}
	if value, expected := info.CurrentOffset, int64(45678335); expected != value {
		t.Error("Wrong CurrentOffset. Expected:", expected, "Was:", value)
	}
	if value, expected := info.Lag, int64(2); expected != value {
		t.Error("Wrong Lag. Expected:", expected, "Was:", value)
	}
	if value, expected := info.ClientId, "foobar-consumer-1-StreamThread-1-consumer"; expected != value {
		t.Error("Wrong ClientId. Expected:", expected, "Was:", value)
	}
	if value, expected := info.ConsumerAddress, "192.168.1.2"; expected != value {
		t.Error("Wrong ConsumerAddress. Expected:", expected, "Was:", value)
	}
}
