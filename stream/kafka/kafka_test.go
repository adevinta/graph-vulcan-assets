package kafka

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/go-cmp/cmp"

	"github.com/adevinta/graph-vulcan-assets/stream"
	"github.com/adevinta/graph-vulcan-assets/stream/streamtest"
)

const (
	bootstrapServers = "127.0.0.1:29092"
	groupPrefix      = "stream_kafka_kafka_test_group_"
	topicPrefix      = "stream_kafka_kafka_test_topic_"
	messagesFile     = "testdata/messages.json"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func setupKafka(topic, filename string) (msgs []stream.Message, err error) {
	cfg := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,

		// Set message timeout to 5s, so the kafka client returns an
		// error if the broker is not up.
		"message.timeout.ms": 5000,
	}

	prod, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating producer: %v", err)
	}
	defer prod.Close()

	msgs = streamtest.Parse(filename)
	for _, msg := range msgs {
		if err := produceMessage(prod, topic, msg); err != nil {
			return nil, fmt.Errorf("error producing message: %v", err)
		}
	}

	for prod.Flush(10000) > 0 {
		// Waiting to flush outstanding messages.
	}

	return msgs, nil
}

func produceMessage(prod *kafka.Producer, topic string, msg stream.Message) error {
	events := make(chan kafka.Event)
	defer close(events)

	kmsg := &kafka.Message{
		Key:            msg.Key,
		Value:          msg.Value,
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	}

	for _, e := range msg.Metadata {
		hdr := kafka.Header{
			Key:   string(e.Key),
			Value: e.Value,
		}
		kmsg.Headers = append(kmsg.Headers, hdr)
	}

	if err := prod.Produce(kmsg, events); err != nil {
		return fmt.Errorf("failed to produce message: %v", err)
	}

	e := <-events
	kmsg, ok := e.(*kafka.Message)
	if !ok {
		return errors.New("event type is not *kafka.Message")
	}
	if kmsg.TopicPartition.Error != nil {
		return fmt.Errorf("could not deliver message: %w", kmsg.TopicPartition.Error)
	}

	return nil
}

func TestAloProcessorProcess(t *testing.T) {
	topic := topicPrefix + strconv.FormatInt(rand.Int63(), 16)

	want, err := setupKafka(topic, messagesFile)
	if err != nil {
		t.Fatalf("error setting up kafka: %v", err)
	}

	cfg := map[string]any{
		"bootstrap.servers":       bootstrapServers,
		"group.id":                groupPrefix + strconv.FormatInt(rand.Int63(), 16),
		"auto.commit.interval.ms": 100,
		"auto.offset.reset":       "earliest",
	}

	proc, err := NewAloProcessor(cfg)
	if err != nil {
		t.Fatalf("error creating kafka processor: %v", err)
	}
	defer proc.Close()

	var (
		ctr int
		got []stream.Message
	)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	err = proc.Process(ctx, topic, func(msg stream.Message) error {
		got = append(got, msg)

		ctr++
		if ctr >= len(want) {
			cancel()
		}

		return nil
	})
	if err != nil {
		t.Fatalf("error processing messages: %v", err)
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("messages mismatch (-want +got):\n%v", diff)
	}
}

func TestAloProcessorProcessAtLeastOnce(t *testing.T) {
	// Number of messages to process before error.
	const n = 2

	topic := topicPrefix + strconv.FormatInt(rand.Int63(), 16)

	want, err := setupKafka(topic, messagesFile)
	if err != nil {
		t.Fatalf("error setting up kafka: %v", err)
	}

	if n > len(want) {
		t.Fatal("n > testdata length")
	}

	cfg := map[string]any{
		"bootstrap.servers":       bootstrapServers,
		"group.id":                groupPrefix + strconv.FormatInt(rand.Int63(), 16),
		"auto.commit.interval.ms": 100,
		"auto.offset.reset":       "earliest",
	}

	proc, err := NewAloProcessor(cfg)
	if err != nil {
		t.Fatalf("error creating kafka processor: %v", err)
	}
	defer proc.Close()

	var (
		ctr int
		got []stream.Message
	)

	// Fail after processing n messages.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	err = proc.Process(ctx, topic, func(msg stream.Message) error {
		if ctr >= n {
			return errors.New("error")
		}

		got = append(got, msg)
		ctr++

		return nil
	})
	if err == nil {
		t.Fatalf("Process should have returned error: %v", err)
	}

	// Wait for 1s to ensure that the offsets are commited.
	time.Sleep(1 * time.Second)

	// Resume stream processing.
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	err = proc.Process(ctx, topic, func(msg stream.Message) error {
		got = append(got, msg)

		ctr++
		if ctr >= len(want) {
			cancel()
		}

		return nil
	})
	if err != nil {
		t.Fatalf("error processing messages: %v", err)
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("messages mismatch (-want +got):\n%v", diff)
	}
}
