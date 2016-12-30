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


	msgs, err := ch.Consume(
	  "Q_test", //q.Name, // queue
	  "",     // consumer
	  true,   // auto-ack
	  false,  // exclusive
	  false,  // no-local
	  false,  // no-wait
	  nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s with RK %s", d.Body, d.RoutingKey)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever


}



