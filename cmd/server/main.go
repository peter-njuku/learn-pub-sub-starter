package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connectionStr := "amqp://guest:guest@localhost:5672/"
	amqp, err := amqp.Dial(connectionStr)
	if err != nil {
		log.Print(err)
	}
	defer amqp.Close()
	fmt.Println("Connection successfull")
	_, err = amqp.Channel()
	if err != nil {
		log.Print(err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh
	fmt.Println("Signal received. Shutting down...")
}
