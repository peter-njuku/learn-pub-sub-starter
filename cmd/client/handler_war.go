package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
)

func handlerWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(war gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(war)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			fmt.Println("\nNackRequeue: Not involved")
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			fmt.Println("\nNackDiscard: War outcome has no units")
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			fmt.Printf("\nAck: %s won against %s", winner, loser)
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			fmt.Printf("\nAck: %s won against %s", winner, loser)
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			fmt.Printf("\nAck: The war between %s and %s ended in a draw", winner, loser)
			return pubsub.Ack
		default:
			fmt.Println("\nNackDiscard: Unknown Message")
			return pubsub.Ack
		}
	}
}
