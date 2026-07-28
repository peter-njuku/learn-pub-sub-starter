package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
)

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(am gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(am)
		switch outcome {
		case gamelogic.MoveOutcomeSamePlayer:
			fmt.Println("Detected our own move")
		case gamelogic.MoveOutcomeMakeWar:
			fmt.Printf("War declared by %s at %s!\n", am.Player.Username, am.ToLocation)
		}
	}
}
