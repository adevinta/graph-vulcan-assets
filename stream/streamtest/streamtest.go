// Package streamtest provides utilities for stream testing.
package streamtest

import (
	"bufio"
	"bytes"
	"context"
	"os"

	"github.com/adevinta/graph-vulcan-assets/stream"
)

// Message represents a message coming from a stream.
type Message struct {
	Key   []byte
	Value []byte
}

// Parse parses a file with messages and returns them. The expected format is:
//
//	# Comment.
//	key0:value0
//	key1:value1
//	...
//	keyN:valueN
//
// It panics if the file cannot be parsed.
func Parse(filename string) []Message {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	s := bufio.NewScanner(f)

	var msgs []Message
	for s.Scan() {
		l := s.Bytes()
		if len(l) == 0 {
			// Empty line.
			continue
		}
		if l[0] == '#' {
			// Comment.
			continue
		}
		parts := bytes.SplitN(l, []byte{':'}, 2)
		if len(parts) != 2 {
			panic("malformed line")
		}
		msgs = append(msgs, Message{Key: parts[0], Value: parts[1]})
	}

	if err := s.Err(); err != nil {
		panic(err)
	}

	return msgs
}

// MockProcessor mocks a stream processor with a predefined set of messages. It
// implements the interface [stream.Processor].
type MockProcessor struct {
	msgs []Message
}

// NewMockProcessor returns a [MockProcessor]. It initializes its internal list
// of messages with msgs.
func NewMockProcessor(msgs []Message) *MockProcessor {
	return &MockProcessor{msgs}
}

// Process processes the messages passed to [NewMockProcessor].
func (mp *MockProcessor) Process(ctx context.Context, entity string, h stream.MsgHandler) error {
	for _, msg := range mp.msgs {
		if err := h(msg.Key, msg.Value); err != nil {
			return err
		}
	}
	return nil
}
