package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func encodeGob(val any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(val); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	bytes, err := encodeGob(val)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return ch.PublishWithContext(
		ctx,
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        bytes,
		},
	)
}

func subscribe[T any](conn *amqp.Connection, exchange, queueName, key string, simpleQueueType SimpleQueueType, handler func(T) AckType, unmarshaller func([]byte) (T, error)) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	msgs, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume queue: %w", err)
	}

	go func() {
		defer channel.Close()
		for msg := range msgs {
			val, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("error unmarshalling the message: %v\n", err)
				msg.Nack(false, false)
				continue
			}

			ackType := handler(val)
			switch ackType {
			case Ack:
				msg.Ack(false)
			case NackRequeue:
				msg.Nack(false, true)
			case NackDiscard:
				msg.Nack(false, false)
			}
		}
	}()

	return nil
}

func SubscribeGob[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	gobUnmarshaller := func(data []byte) (T, error) {
		var val T
		buf := bytes.NewBuffer(data)
		decoder := gob.NewDecoder(buf)
		if err := decoder.Decode(&val); err != nil {
			return val, err
		}
		return val, nil
	}
	return subscribe(conn, exchange, queueName, key, queueType, handler, gobUnmarshaller)
}
