package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connectionStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionStr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	fmt.Println("Connection successful")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}

	gameState := gamelogic.NewGameState(username)
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	// _, queue, err := pubsub.DeclareAndBind(conn,
	// 	routing.ExchangePerilDirect,
	// 	queueName,
	// 	routing.PauseKey,
	// 	pubsub.SimpleQueueTransient,
	// )
	// if err != nil {
	// 	log.Fatalf("Could not subscribe to pause: %v", err)
	// }
	//fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("Pausing failed: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.SimpleQueueTransient,
		handlerMove(gameState),
	)
	if err != nil {
		log.Fatalf("Moving failed: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "spawn":
			fmt.Println("Publishing spawn command")
			err = gameState.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("Spawn command published successfully")

		case "move":
			fmt.Println("Publishing move command")
			move, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, move.Player.Username),
				move,
			)
			if err != nil {
				fmt.Printf("Error publishing move: %v\n", err)
				continue
			}
			fmt.Println("Move command published successfully")

		case "status":
			gameState.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("Unknown command")
		}
	}
}
