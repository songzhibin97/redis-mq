package list

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	mq "github.com/songzhibin97/redis-mq"

	"github.com/songzhibin97/gkit/timeout"

	redis "github.com/go-redis/redis/v8"
)

var _ mq.Producer[any] = (*RedisListMQ[any])(nil)
var _ mq.Consumer[any] = (*RedisListMQ[any])(nil)

type RedisListMQ[T any] struct {
	Topic string `json:"topic"` // Topic is the topic of the message.

	ConsumptionQueueName string `json:"consumption_queue_name"` // ConsumptionQueueName is the name of the consumption queue.
	PendingACKQueueName  string `json:"pending_ack_queue_name"` // PendingACKQueueName is the name of the pending ack queue.

	client redis.UniversalClient

	ctx    context.Context
	cancel context.CancelFunc

	// producer
	generalID mq.GeneralID

	// consumer
	removeDuplicates mq.RemoveDuplicates
}

func (r *RedisListMQ[T]) InitProducer(generalID mq.GeneralID) {
	r.generalID = generalID
}

func (r *RedisListMQ[T]) InitConsumer(removeDuplicates mq.RemoveDuplicates) {
	r.removeDuplicates = removeDuplicates
}

func (r *RedisListMQ[T]) Consume(ctx context.Context) (*mq.Message[T], error) {
	message := &mq.Message[T]{}
	for {
		result, err := r.client.RPopLPush(timeout.Compare(ctx, r.ctx), r.ConsumptionQueueName, r.PendingACKQueueName).Result()
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(result), message); err != nil {
			return nil, err
		}

		if r.removeDuplicates != nil && r.removeDuplicates(message.Topic, message.ID) {
			err = r.Ack(r.ctx, message)
			if err != nil {
				return nil, err
			}
			continue
		}
		return message, nil
	}
}

func (r *RedisListMQ[T]) BlockConsume(ctx context.Context) (*mq.Message[T], error) {
	message := &mq.Message[T]{}

	for {
		result, err := r.client.BRPopLPush(timeout.Compare(ctx, r.ctx), r.ConsumptionQueueName, r.PendingACKQueueName, 0).Result()
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal([]byte(result), message); err != nil {
			return nil, err
		}

		if r.removeDuplicates != nil && r.removeDuplicates(message.Topic, message.ID) {
			err = r.Ack(r.ctx, message)
			if err != nil {
				return nil, err
			}
			continue
		}
		return message, nil
	}
}

func (r *RedisListMQ[T]) Ack(ctx context.Context, message *mq.Message[T]) error {
	return r.client.LRem(timeout.Compare(ctx, r.ctx), r.PendingACKQueueName, -1, message).Err()
}

func (r *RedisListMQ[T]) PretreatmentProduceMessage(message *mq.Message[T]) {
	if message == nil {
		return
	}
	message.CreateTimeStamp = time.Now().UnixNano()
	message.Topic = r.Topic
	if r.generalID != nil {
		message.ID = r.generalID()
	}
}

func (r *RedisListMQ[T]) Produce(ctx context.Context, message *mq.Message[T]) error {
	r.PretreatmentProduceMessage(message)
	return r.client.LPush(timeout.Compare(ctx, r.ctx), r.ConsumptionQueueName, message).Err()
}

func (r *RedisListMQ[T]) BatchProduce(ctx context.Context, messages []*mq.Message[T]) error {
	ms := make([]interface{}, len(messages))
	for i, message := range messages {
		r.PretreatmentProduceMessage(message)
		ms[i] = message
	}

	return r.client.LPush(timeout.Compare(ctx, r.ctx), r.ConsumptionQueueName, ms...).Err()
}

func (r *RedisListMQ[T]) Close() error {
	r.cancel()
	return r.client.Close()
}

func (r *RedisListMQ[T]) ReRun(ctx context.Context) error {
	for {
		err := r.client.RPopLPush(timeout.Compare(ctx, r.ctx), r.PendingACKQueueName, r.ConsumptionQueueName).Err()
		if err != nil && errors.Is(err, redis.Nil) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func NewRedisListMQ[T any](ctx context.Context, topic string, options redis.UniversalOptions) (*RedisListMQ[T], error) {
	client := redis.NewUniversalClient(&options)
	if client == nil {
		return nil, errors.New("redis.NewUniversalClient error")
	}
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	return &RedisListMQ[T]{
		Topic:                topic,
		ConsumptionQueueName: topic + "_consumption_queue",
		PendingACKQueueName:  topic + "_pending_ack_queue",
		ctx:                  ctx,
		cancel:               cancel,
		client:               client,
		generalID:            mq.DefaultGeneralID(),
		removeDuplicates:     mq.DefaultRemoveDuplicates(),
	}, nil
}
