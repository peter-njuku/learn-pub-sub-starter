package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(war gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(war)

		var msg string

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			fmt.Println("\nNackequeue: Not involved")
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			fmt.Println("\nNackDiscard: War outcome has no units")
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			msg = fmt.Sprintf("\nAck: %s won against %s", winner, loser)

		case gamelogic.WarOutcomeYouWon:
			msg = fmt.Sprintf("\nAck: %s won against %s", winner, loser)

		case gamelogic.WarOutcomeDraw:
			msg = fmt.Sprintf("\nAck: The war between %s and %s ended in a draw", winner, loser)

		default:
			fmt.Println("\nNackDiscard: Unknown Message")
			return pubsub.NackDiscard
		}

		gameLog := routing.GameLog{
			CurrentTime: time.Now(),
			Message:     msg,
			Username:    war.Attacker.Username,
		}

		err := pubsub.PublishGameLog(
			ch,
			war.Attacker.Username,
			gameLog,
		)
		if err != nil {
			fmt.Printf("Error publishing game log: %v\n", err)
			return pubsub.NackRequeue

		}
		return pubsub.Ack
	}
}
