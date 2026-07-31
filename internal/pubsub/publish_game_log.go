package pubsub

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGameLog(ch *amqp.Channel, username string, log routing.GameLog) error {
	routingKey := fmt.Sprintf("%s.%s", routing.GameLogSlug, username)
	return PublishGob(
		ch,
		routing.ExchangePerilTopic,
		routingKey,
		log,
	)
}
