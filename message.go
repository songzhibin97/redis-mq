package redis_mq

import "encoding/json"

type Message[T any] struct {
	ID              string `json:"id"`
	Topic           string `json:"topic"`
	CreateTimeStamp int64  `json:"create_time_stamp"`

	Body T `json:"body"`
}

func (m *Message[T]) GetID() string {
	return m.ID
}

func (m *Message[T]) GetCreateTimeStamp() int64 {
	return m.CreateTimeStamp
}

func (m *Message[T]) GetBody() T {
	return m.Body
}

func (m *Message[T]) MarshalBinary() (data []byte, err error) {
	return json.Marshal(m)
}

func (m *Message[T]) UnmarshalBinary(bytes []byte) interface{} {
	message := &Message[T]{}
	_ = json.Unmarshal(bytes, message)
	return message
}

func NewMessage[T any](body T) *Message[T] {
	return &Message[T]{
		Body: body,
	}
}
