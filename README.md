prometheus-kafka-consumer-group-exporter
========================================
[![Build Status](https://secure.travis-ci.org/kawamuray/prometheus-kafka-consumer-group-exporter.png?branch=master)](http://travis-ci.org/kawamuray/prometheus-kafka-consumer-group-exporter) [![Go Report Card](https://goreportcard.com/badge/github.com/kawamuray/prometheus-kafka-consumer-group-exporter)](https://goreportcard.com/report/github.com/kawamuray/prometheus-kafka-consumer-group-exporter) [![GoDoc](https://godoc.org/github.com/kawamuray/prometheus-kafka-consumer-group-exporter?status.svg)](https://godoc.org/github.com/kawamuray/prometheus-kafka-consumer-group-exporter)

A [prometheus](https://prometheus.io/) exporter for Kafka's consumer group
information. For other metrics from Kafka, have a look at the [JMX
exporter](https://github.com/prometheus/jmx_exporter).

Capabilities
============
 - Exports Kafka's consumer group information which can be obtained by
   executing `kafka-consumer-groups.sh`
 - Supports only new consumer (`--new-consumer` switch enabled by default)
   which uses Kafka broker as the offset checkpoint store

Export metrics
==============
 - `kafka_broker_consumer_group_current_offset`: Consuming offset of each
   consumer group/client/topic/partition based on committed offset
 - `kafka_broker_consumer_group_offset_lag`: Offset lag between the last log
   end offset and consuming point of each consumer group/client/topic/partition

Supported Kafka versions
========================
This exporter relies on `kafka-consumer-groups.sh` script that is shipped as
part of Apache Kafka distribution.  Here is the list of Apache Kafka versions
which has been tested to use from this exporter:

 - `0.10.0.1`
 - `0.9.0.1`

Install and run
===============
```sh
$ go get github.com/kawamuray/prometheus-kafka-consumer-group-exporter/...
$ $GOPATH/bin/prometheus-kafka-consumer-group-exporter -help
```

How to start developing
=======================
```sh
$ go get github.com/kawamuray/prometheus-kafka-consumer-group-exporter
$ cd $GOPATH/src/github.com/kawamuray/prometheus-kafka-consumer-group-exporter
$ git checkout -b my-new-feature
$ go test -v ./...    # Run tests to make sure you have a good development environment and all works.
```

Before pull requests you may also want to make sure that `go vet`, `gofmt` and `golint` passes:
```sh
$ go get -u github.com/golang/lint/golint
$ golint ./...
$ ./check_gofmt.sh
$ go vet ./...
```
(otherwise that's done through our CI on pull request submission)

Example Usage
=============
```sh
# Download latest Kafka distribution (if necessary)
$ tar zxvf kafka_LATEST_VERSION.tgz
$ ./kafka_consumer_group_exporter --consumer-group-command-path=./kafka_LATEST_VERSION/bin/kafka-consumer-groups.sh BOOTSTRAP_SERVERS
```
