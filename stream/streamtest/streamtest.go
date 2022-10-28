// Package streamtest provides utilities for stream testing.
package streamtest

import (
	"context"
	"encoding/json"
	"os"

	"github.com/adevinta/graph-vulcan-assets/stream"
)

// Parse parses a json file with messages and returns them. It panics if the
// file cannot be parsed.
func Parse(filename string) []stream.Message {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var testdata []struct {
		Key      *string `json:"key,omitempty"`
		Value    *string `json:"value,omitempty"`
		Metadata []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"metadata,omitempty"`
	}

	if err := json.NewDecoder(f).Decode(&testdata); err != nil {
		panic(err)
	}

	var msgs []stream.Message
	for _, td := range testdata {
		var msg stream.Message
		if td.Key != nil {
			msg.Key = []byte(*td.Key)
		}
		if td.Value != nil {
			msg.Value = []byte(*td.Value)
		}
		for _, e := range td.Metadata {
			if e.Key == "" {
				panic("empty metadata key")
			}
			if e.Value == "" {
				panic("empty metadata value")
			}
			entry := stream.MetadataEntry{
				Key:   []byte(e.Key),
				Value: []byte(e.Value),
			}
			msg.Metadata = append(msg.Metadata, entry)
		}
		msgs = append(msgs, msg)
	}

	return msgs
}

// MockProcessor mocks a stream processor with a predefined set of messages. It
// implements the interface [stream.Processor].
type MockProcessor struct {
	msgs []stream.Message
}

// NewMockProcessor returns a [MockProcessor]. It initializes its internal list
// of messages with msgs.
func NewMockProcessor(msgs []stream.Message) *MockProcessor {
	return &MockProcessor{msgs}
}

// Process processes the messages passed to [NewMockProcessor].
func (mp *MockProcessor) Process(ctx context.Context, entity string, h stream.MsgHandler) error {
	for _, msg := range mp.msgs {
		if err := h(msg); err != nil {
			return err
		}
	}
	return nil
}
