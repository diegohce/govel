package main

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)


func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}



func main() {

	conn, err := amqp.Dial("amqp://guest:guest@10.0.3.214:5672/")
	failOnError(err, "Error connecting to RabbitMQ")
	defer conn.Close()


	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()


	body := "hello world!"
	err = ch.Publish(
		"E_test",     // exchange
		"00Z8", //q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing {
		  ContentType: "text/plain",
		  Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")

}



