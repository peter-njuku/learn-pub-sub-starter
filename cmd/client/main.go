package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

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
		log.Fatalf("Pausing failed: %v\n", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.SimpleQueueTransient,
		handlerMove(gameState, ch),
	)
	if err != nil {
		log.Fatalf("Moving failed: %v\n", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gameState.GetPlayerSnap().Username),
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.SimpleQueueDurable,
		handlerWar(gameState, ch),
	)
	if err != nil {
		log.Fatalf("Could not subscribe to war recognition: %v\n", err)
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

		case "spam":
			if len(words) != 2 {
				fmt.Println("Usage: spam <n>")
				continue
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Printf("Could not convert to integer. Try Again: %v\n", err)
				continue
			}
			for i := 0; i < n; i++ {
				log := routing.GameLog{
					CurrentTime: time.Now(),
					Message:     gamelogic.GetMaliciousLog(),
					Username:    username,
				}
				err := pubsub.PublishGob(
					ch,
					routing.ExchangePerilTopic,
					fmt.Sprintf("%s.%s", routing.GameLogSlug, username),
					log,
				)
				if err != nil {
					fmt.Printf("Error publishing malicious log: %v\n", err)
					break
				}

			}
			fmt.Printf("Published %d malicious logs!\n", n)

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("Unknown command")
		}
	}
}
