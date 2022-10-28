// Package stream allows to interact with different stream-processing
// platforms.
package stream

import "context"

// Message represents a message coming from a stream.
type Message struct {
	Key      []byte
	Value    []byte
	Metadata []MetadataEntry
}

// MetadataEntry represents a metadata entry.
type MetadataEntry struct {
	Key   []byte
	Value []byte
}

// A Processor represents a stream message processor.
type Processor interface {
	Process(ctx context.Context, entity string, h MsgHandler) error
}

// A MsgHandler processes a message.
type MsgHandler func(msg Message) error
