package stream

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/songzhibin97/gkit/timeout"
	"github.com/songzhibin97/go-baseutils/base/bternaryexpr"

	"github.com/go-redis/redis/v8"

	mq "github.com/songzhibin97/redis-mq"
)

var _ mq.Producer[any] = (*RedisStreamMQ[any])(nil)
var _ mq.Consumer[any] = (*RedisStreamMQ[any])(nil)

type RedisStreamMQ[T any] struct {
	Topic string `json:"topic"` // Topic is the topic of the message.

	client redis.UniversalClient

	ctx    context.Context
	cancel context.CancelFunc

	// producer
	generalID mq.GeneralID

	// consumer
	removeDuplicates mq.RemoveDuplicates
	blockDuration    time.Duration
	groupName        string
	consumerName     string

	reStar string // reStar is the start of the message.
}

func (r *RedisStreamMQ[T]) InitProducer(generalID mq.GeneralID) {
	r.generalID = generalID
}

func (r *RedisStreamMQ[T]) InitConsumer(groupName string, consumerName string, start string, blockDuration time.Duration, removeDuplicates mq.RemoveDuplicates) error {
	r.removeDuplicates = removeDuplicates
	r.blockDuration = blockDuration
	r.groupName = groupName
	r.consumerName = consumerName
	if r.reStar != "" {
		start = r.reStar
	}
	_, err := r.client.XGroupCreate(r.ctx, r.Topic, r.groupName, start).Result()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

func (r *RedisStreamMQ[T]) Consume(ctx context.Context) (*mq.Message[T], error) {
	if r.groupName == "none" {
		return r.consume(ctx)
	}
	return r.consumeGroup(ctx)
}

func (r *RedisStreamMQ[T]) consume(ctx context.Context) (*mq.Message[T], error) {
	for {
		result, err := r.client.XRead(timeout.Compare(ctx, r.ctx), &redis.XReadArgs{
			Streams: []string{r.Topic},
			Count:   1,
			Block:   0,
		}).Result()
		if err != nil {
			return nil, err
		}
		if len(result) == 0 || len(result[0].Messages) == 0 {
			return nil, errors.New("no message")
		}
		message := &mq.Message[T]{}
		if err := json.Unmarshal([]byte(result[0].Messages[0].Values["body"].(string)), message); err != nil {
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

func (r *RedisStreamMQ[T]) consumeGroup(ctx context.Context) (*mq.Message[T], error) {

	for {
		result, err := r.client.XReadGroup(timeout.Compare(ctx, r.ctx), &redis.XReadGroupArgs{
			Group:    r.groupName,
			Consumer: r.consumerName,
			Streams:  []string{r.Topic, bternaryexpr.TernaryExpr(r.reStar == "", ">", r.reStar)},
			Count:    1,
			Block:    0,
			NoAck:    false,
		}).Result()
		if err != nil {
			return nil, err
		}
		if len(result) == 0 || len(result[0].Messages) == 0 && r.reStar != "" {
			r.reStar = ""
			continue
		}
		if len(result) == 0 || len(result[0].Messages) == 0 {
			return nil, errors.New("no message")
		}
		message := &mq.Message[T]{}
		if err := json.Unmarshal([]byte(result[0].Messages[0].Values["body"].(string)), message); err != nil {
			return nil, err
		}
		if r.reStar != "" {
			r.reStar = result[0].Messages[0].ID
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

func (r *RedisStreamMQ[T]) BlockConsume(ctx context.Context) (*mq.Message[T], error) {
	if r.groupName == "none" {
		return r.blockConsume(ctx)
	}
	return r.blockConsumeGroup(ctx)
}

func (r *RedisStreamMQ[T]) blockConsume(ctx context.Context) (*mq.Message[T], error) {
	for {
		result, err := r.client.XRead(timeout.Compare(ctx, r.ctx), &redis.XReadArgs{
			Streams: []string{r.Topic},
			Count:   1,
			Block:   r.blockDuration,
		}).Result()
		if err != nil {
			return nil, err
		}
		if len(result) == 0 || len(result[0].Messages) == 0 {
			return nil, errors.New("no message")
		}
		message := &mq.Message[T]{}
		if err := json.Unmarshal([]byte(result[0].Messages[0].Values["body"].(string)), message); err != nil {
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

func (r *RedisStreamMQ[T]) blockConsumeGroup(ctx context.Context) (*mq.Message[T], error) {
	for {
		result, err := r.client.XReadGroup(timeout.Compare(ctx, r.ctx), &redis.XReadGroupArgs{
			Group:    r.groupName,
			Consumer: r.consumerName,
			Streams:  []string{r.Topic, bternaryexpr.TernaryExpr(r.reStar == "", ">", r.reStar)},
			Count:    1,
			Block:    r.blockDuration,
			NoAck:    false,
		}).Result()
		if err != nil {
			return nil, err
		}
		if len(result) == 0 || len(result[0].Messages) == 0 && r.reStar != "" {
			r.reStar = ""
			continue
		}
		if len(result) == 0 || len(result[0].Messages) == 0 {
			return nil, errors.New("no message")
		}
		message := &mq.Message[T]{}
		if err := json.Unmarshal([]byte(result[0].Messages[0].Values["body"].(string)), message); err != nil {
			return nil, err
		}
		if r.reStar != "" {
			r.reStar = result[0].Messages[0].ID
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

func (r *RedisStreamMQ[T]) Ack(ctx context.Context, message *mq.Message[T]) error {
	if r.groupName == "none" {
		return r.ack(ctx, message)
	}
	return r.ackGroup(ctx, message)
}

func (r *RedisStreamMQ[T]) ack(ctx context.Context, message *mq.Message[T]) error {
	return r.client.XDel(timeout.Compare(ctx, r.ctx), r.Topic, message.ID).Err()
}

func (r *RedisStreamMQ[T]) ackGroup(ctx context.Context, message *mq.Message[T]) error {
	return r.client.XAck(timeout.Compare(ctx, r.ctx), r.Topic, r.groupName, message.ID).Err()
}

func (r *RedisStreamMQ[T]) ReRun(ctx context.Context) error {
	if r.groupName == "none" {
		return nil
	}
	result, err := r.client.XPendingExt(timeout.Compare(ctx, r.ctx), &redis.XPendingExtArgs{
		Stream:   r.Topic,
		Group:    r.groupName,
		Idle:     0,
		Start:    "-",
		End:      "+",
		Count:    1,
		Consumer: r.consumerName,
	}).Result()
	if err != nil {
		return err
	}
	if len(result) == 0 {
		return nil
	}
	r.reStar = result[0].ID
	return nil
}

func (r *RedisStreamMQ[T]) PretreatmentProduceMessage(message *mq.Message[T]) {
	if message == nil {
		return
	}
	message.CreateTimeStamp = time.Now().UnixNano()
	message.Topic = r.Topic
	if r.generalID != nil {
		message.ID = r.generalID()
	}
}

func (r *RedisStreamMQ[T]) Produce(ctx context.Context, message *mq.Message[T]) error {
	r.PretreatmentProduceMessage(message)

	_, err := r.client.XAdd(timeout.Compare(ctx, r.ctx), &redis.XAddArgs{
		Stream: r.Topic,
		ID:     message.ID,
		Values: []interface{}{"body", message},
	}).Result()
	return err
}

func (r *RedisStreamMQ[T]) BatchProduce(ctx context.Context, messages []*mq.Message[T]) error {
	for _, message := range messages {
		err := r.Produce(ctx, message)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RedisStreamMQ[T]) Close() error {
	r.cancel()
	return r.client.Close()
}

func NewRedisStreamMQ[T any](ctx context.Context, topic string, options redis.UniversalOptions) (*RedisStreamMQ[T], error) {
	client := redis.NewUniversalClient(&options)
	if client == nil {
		return nil, errors.New("redis.NewUniversalClient error")
	}
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	return &RedisStreamMQ[T]{
		Topic:            topic,
		client:           client,
		ctx:              ctx,
		cancel:           cancel,
		generalID:        mq.DefaultGeneralID(),
		removeDuplicates: mq.DefaultRemoveDuplicates(),
		blockDuration:    10 * time.Second,
		groupName:        "none",
		consumerName:     "none",
	}, nil
}
