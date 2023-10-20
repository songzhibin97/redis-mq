package redis_mq

import (
	"context"
	"strconv"
	"time"

	"github.com/songzhibin97/gkit/generator"
)

type Producer[T any] interface {
	// Produce produces a message.
	Produce(ctx context.Context, message *Message[T]) error

	// BatchProduce produces a batch of messages.
	BatchProduce(ctx context.Context, messages []*Message[T]) error

	// Close closes the producer.
	Close() error
}

type GeneralID func() string

func DefaultGeneralID() GeneralID {
	gen := generator.NewSnowflake(time.Date(2023, 10, 1, 0, 0, 0, 0, time.Local), 1)
	return func() string {
		genID, _ := gen.NextID()
		return strconv.FormatUint(genID, 10)
	}
}
