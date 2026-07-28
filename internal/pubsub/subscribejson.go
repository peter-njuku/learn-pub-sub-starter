package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T)) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryChan, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for msg := range deliveryChan {
			var target T
			err = json.Unmarshal(msg.Body, &target)
			if err != nil {
				log.Printf("Error unmarshaling messages: %v", err)
				continue
			}
			handler(target)
			err = msg.Ack(false)
			if err != nil {
				log.Printf("Error acknowlodging message: %v", err)
			}
		}
	}()

	return nil
}
