package pubsub

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	jsonUnmarshaller := func(data []byte) (T, error) {
		var val T
		if err := json.Unmarshal(data, &val); err != nil {
			return val, err
		}
		return val, nil
	}
	return subscribe(conn, exchange, queueName, key, queueType, handler, jsonUnmarshaller)
}
