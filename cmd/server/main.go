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
	fmt.Println("Starting Peril server...")
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

	//_, queue, err := pubsub.DeclareAndBind(
	//	conn,
	//	routing.ExchangePerilTopic,
	//	routing.GameLogSlug,
	//	routing.GameLogSlug+".",
	//	pubsub.SimpleQueueDurable,
	//)
	//if err != nil {
	//		log.Fatalf("Could not subscribe to pause: %v", err)
	//	}
	//fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gamelogic.PrintServerHelp()

	err = pubsub.SubscribeGob(conn, routing.ExchangePerilTopic, routing.GameLogSlug, fmt.Sprintf("%s.*", routing.GameLogSlug), pubsub.SimpleQueueDurable,
		handlerGameLog())
	if err != nil {
		fmt.Printf("Subscribe Gob wong: %v\n", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "pause":
			fmt.Println("Publishing a pause message.")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("Pause message sent successfully")
		case "resume":
			fmt.Println("Publishing a resume message.")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("Resume message sent successfully")
		case "quit":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("I do not understand the command.")
		}
	}
}
