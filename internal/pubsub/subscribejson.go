package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
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
				msg.Nack(false, false)
				continue
			}
			ackType := handler(target)

			switch ackType {
			case Ack:
				msg.Ack(false)
				log.Println("Ack: Message processed successfully")
			case NackRequeue:
				msg.Nack(false, true)
				log.Println("NackRequeue: Message will be requeued")
			case NackDiscard:
				msg.Nack(false, false)
				log.Println("NackDiscard: Message discarded")
			}
		}
	}()

	return nil
}
