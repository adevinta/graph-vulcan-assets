// Package kafka allows to process messages from a kafka topic ensuring
// at-least-once semantics.
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/adevinta/graph-vulcan-assets/stream"
)

// An AloProcessor allows to process messages from a kafka topic ensuring
// at-least-once semantics.
type AloProcessor struct {
	c *kafka.Consumer
}

// NewAloProcessor returns an [AloProcessor] with the provided kafka
// configuration properties.
func NewAloProcessor(config map[string]any) (AloProcessor, error) {
	kconfig := make(kafka.ConfigMap)
	for k, v := range config {
		if err := kconfig.SetKey(k, v); err != nil {
			return AloProcessor{}, fmt.Errorf("could not set config key: %w", err)
		}
	}

	// Ensure at-least-once semantics.
	//
	// confluent-kafka-go uses librdkafka under the hood. librdkafka, by
	// default (enable.auto.commit=true, enable.auto.offset.store=true),
	// commits offsets in a background thread independently of the
	// application polling the consumer for new messages.
	//
	// enable.auto.offset.store controls if librdkafka should automatically
	// store the offset of the last message provided to the application.
	// The offset store is an in-memory store of the next offset to
	// (auto-)commit for each partition.
	//
	// In order to achieve at-least-once semantics, we set
	// enable.auto.offset.store=false, which allows us to store offsets
	// explicitly using the methods kafka.Consumer.StoreOffsets and
	// kafka.Consumer.StoreMessage.
	//
	// More information: https://github.com/confluentinc/confluent-kafka-go/issues/481
	kconfig["enable.auto.commit"] = true
	kconfig["enable.auto.offset.store"] = false

	c, err := kafka.NewConsumer(&kconfig)
	if err != nil {
		return AloProcessor{}, fmt.Errorf("failed to create a consumer: %w", err)
	}

	return AloProcessor{c}, nil
}

// Process processes the messages received in the topic called entity by
// calling h. This method blocks the calling goroutine until the specified
// context is cancelled or an error occurs. It replaces the current kafka
// subscription, so it should not be called concurrently.
func (proc AloProcessor) Process(ctx context.Context, entity string, h stream.MsgHandler) error {
	if err := proc.c.Subscribe(entity, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topic %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		kmsg, err := proc.c.ReadMessage(100 * time.Millisecond)
		if err != nil {
			kerr, ok := err.(kafka.Error)
			if ok && kerr.Code() == kafka.ErrTimedOut {
				continue
			}
			return fmt.Errorf("error reading message: %w", kerr)
		}

		msg := stream.Message{
			Key:   kmsg.Key,
			Value: kmsg.Value,
		}

		for _, hdr := range kmsg.Headers {
			entry := stream.MetadataEntry{
				Key:   []byte(hdr.Key),
				Value: hdr.Value,
			}
			msg.Metadata = append(msg.Metadata, entry)
		}

		if err := h(msg); err != nil {
			return fmt.Errorf("error processing message: %w", err)
		}

		if _, err := proc.c.StoreMessage(kmsg); err != nil {
			return fmt.Errorf("error storing offset: %w", err)
		}
	}
}

// Close closes the underlaying kafka consumer.
func (proc AloProcessor) Close() error {
	return proc.c.Close()
}
