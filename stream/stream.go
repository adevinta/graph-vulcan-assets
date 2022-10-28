// Package stream allows to interact with different stream-processing
// platforms.
package stream

import "context"

// A Processor represents a stream message processor.
type Processor interface {
	Process(ctx context.Context, entity string, h MsgHandler) error
}

// A MsgHandler processes a message.
type MsgHandler func(key []byte, value []byte) error
