package redis_mq

import (
	"context"
)

type Consumer[T any] interface {
	// Consume consumes a message.
	Consume(ctx context.Context) (*Message[T], error)

	// BlockConsume consumes a message, blocking until a message is available.
	BlockConsume(ctx context.Context) (*Message[T], error)

	// Ack acknowledges a message.
	Ack(ctx context.Context, message *Message[T]) error

	// Close closes the consumer.
	Close() error

	// ReRun re-run a message.
	ReRun(ctx context.Context) error
}

type RemoveDuplicates func(topic, id string) bool

func DefaultRemoveDuplicates() RemoveDuplicates {
	last := ""
	return func(topic, id string) bool {
		if last != "" && last > id {
			return true
		}
		last = id
		return false
	}
}
