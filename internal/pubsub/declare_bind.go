package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Creating channel failed: %w", err)
	}

	queue, err := ch.QueueDeclare(
		queueName,
		queueType == SimpleQueueDurable,
		queueType != SimpleQueueDurable,
		queueType != SimpleQueueDurable,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Failed to declare channel: %w", err)
	}
	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Failed to QueueBind: %w", err)
	}
	return ch, queue, nil
}
