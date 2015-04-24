/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"time"

	prometheus "github.com/prometheus/client_golang/prometheus"
	metrics "github.com/rcrowley/go-metrics"

	kafka "github.com/stealthly/go_kafka_client"
)

type consumerConfigs []string

func (i *consumerConfigs) String() string {
	return fmt.Sprintf("%s", *i)
}

func (i *consumerConfigs) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var (
	msg_read = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mirrormaker",
		Subsystem: "indexer",
		Name:      "msg_read",
		Help:      "The number of messages read.",
	})
	queue_size = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mirrormaker",
		Subsystem: "queue",
		Name:      "queue_size",
		Help:      "The number of messages in queue.",
	})
	msg_sent = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mirrormaker",
		Subsystem: "indexer",
		Name:      "msg_sent",
		Help:      "The number of messages sent.",
	})
	num_streams = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mirrormaker",
		Subsystem: "indexer",
		Name:      "num_streams",
		Help:      "The number of streams.",
	})
	num_producers = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mirrormaker",
		Subsystem: "indexer",
		Name:      "num_producers",
		Help:      "The number of producers.",
	})
)

var whitelist = flag.String("whitelist", "", "regex pattern for whitelist. Providing both whitelist and blacklist is an error.")
var blacklist = flag.String("blacklist", "", "regex pattern for blacklist. Providing both whitelist and blacklist is an error.")
var consumerConfig consumerConfigs
var producerConfig = flag.String("producer.config", "", "Path to producer configuration file.")
var numProducers = flag.Int("num.producers", 1, "Number of producers.")
var numStreams = flag.Int("num.streams", 1, "Number of consumption streams.")
var preservePartitions = flag.Bool("preserve.partitions", false, "preserve partition number. E.g. if message was read from partition 5 it'll be written to partition 5.")
var preserveOrder = flag.Bool("preserve.order", false, "E.g. message sequence 1, 2, 3, 4, 5 will remain 1, 2, 3, 4, 5 in destination topic.")
var prefix = flag.String("prefix", "", "Destination topic prefix.")
var queueSize = flag.Int("queue.size", 10000, "Number of messages that are buffered between the consumer and producer.")
var maxProcs = flag.Int("max.procs", runtime.NumCPU(), "Maximum number of CPUs that can be executing simultaneously.")
var graphiteConnect = flag.String("graphite.connect", "", "IP and Port of Graphite Instance.")
var graphiteFlush = flag.String("graphite.flush", "10s", "Graphite Flush interval. Default: 10s")
var graphitePrefix = flag.String("graphite.prefix", "mirrormaker", "Graphite Prefix key. Default: mirrormaker")
var prometheusAddr = flag.String("prometheus.address", "", "The address to listen on for HTTP requests.")

func parseAndValidateArgs() *kafka.MirrorMakerConfig {
	flag.Var(&consumerConfig, "consumer.config", "Path to consumer configuration file.")
	flag.Parse()
	runtime.GOMAXPROCS(*maxProcs)

	if (*whitelist != "" && *blacklist != "") || (*whitelist == "" && *blacklist == "") {
		fmt.Println("Exactly one of whitelist or blacklist is required.")
		os.Exit(1)
	}
	if *producerConfig == "" {
		fmt.Println("Producer config is required.")
		os.Exit(1)
	}
	if len(consumerConfig) == 0 {
		fmt.Println("At least one consumer config is required.")
		os.Exit(1)
	}
	if *queueSize < 0 {
		fmt.Println("Queue size should be equal or greater than 0")
		os.Exit(1)
	}

	config := kafka.NewMirrorMakerConfig()
	config.Blacklist = *blacklist
	config.Whitelist = *whitelist
	config.ChannelSize = *queueSize
	config.ConsumerConfigs = []string(consumerConfig)
	config.NumProducers = *numProducers
	config.NumStreams = *numStreams
	config.PreservePartitions = *preservePartitions
	config.PreserveOrder = *preserveOrder
	config.ProducerConfig = *producerConfig
	config.TopicPrefix = *prefix

	return config
}

func main() {
	config := parseAndValidateArgs()

	if *graphiteConnect != "" {
		flushTime, _ := time.ParseDuration(*graphiteFlush)
		startMetrics(*graphiteConnect, flushTime, *graphitePrefix)
	}

	if *prometheusAddr != "" {
		go func() {
			http.Handle("/metrics", prometheus.Handler())
			log.Println(http.ListenAndServe(*prometheusAddr, nil))
		}()

		num_streams.Set(float64(config.NumStreams))
		num_producers.Set(float64(config.NumProducers))

		successHook := func(msg *kafka.ProducerMessage) {
			kafka.Infof("producer", "Send msg to: %s", msg.Topic)
			msg_sent.Inc()
			queue_size.Dec()
		}

		config.ProducerSuccessCallbacks = append(config.ProducerSuccessCallbacks, successHook)
	}

	mirrorMaker := kafka.NewMirrorMaker(config)

	go mirrorMaker.Start()

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)
	<-ctrlc
	mirrorMaker.Stop()
}

func startMetrics(connect string, flush time.Duration, prefix string) {
	kafka.Infof("metrics", "Send metrics to: %s", connect)
	addr, err := net.ResolveTCPAddr("tcp", connect)
	if err != nil {
		panic(err)
	}
	go metrics.GraphiteWithConfig(metrics.GraphiteConfig{
		Addr:          addr,
		Registry:      metrics.DefaultRegistry,
		FlushInterval: flush,
		DurationUnit:  time.Second,
		Prefix:        prefix,
		Percentiles:   []float64{0.5, 0.75, 0.95, 0.99, 0.999},
	})
}

func GetStrategy(consumerId string) func(*kafka.Worker, *kafka.Message, kafka.TaskId) kafka.WorkerResult {
	consumeRate := metrics.NewRegisteredMeter(fmt.Sprintf("%s-ConsumeRate", consumerId), metrics.DefaultRegistry)
	return func(_ *kafka.Worker, msg *kafka.Message, id kafka.TaskId) kafka.WorkerResult {
		kafka.Infof("main", "Got a message: %s", string(msg.Value))
		consumeRate.Mark(1)

		return kafka.NewSuccessfulResult(id)
	}
}

func init() {
	prometheus.MustRegister(msg_read)
	prometheus.MustRegister(msg_sent)
	prometheus.MustRegister(queue_size)
	prometheus.MustRegister(num_streams)
	prometheus.MustRegister(num_producers)
}
