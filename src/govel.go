package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)


type Rabbit struct {
	username string
	password string
	host     string
	port     string
	vhost    string
	conn     *amqp.Connection
	ch       *amqp.Channel
	err_chan chan *amqp.Error
}

type RabbitA struct {
	rabbit *Rabbit
	queue_name string
	stop_flag chan bool
}

type RabbitB struct {
	rabbit *Rabbit
	exchange_name string
}



func NewRabbit(username, password, host, port, vhost string) (*Rabbit, error) {

	rabbit := &Rabbit{ username: username,
                      password: password,
                      host    : host,
                      port    : port,
                      vhost   : vhost}

	err := rabbit.Setup()
	if err != nil {
		return nil, err
	}

	rabbit.err_chan = make(chan *amqp.Error)

	rabbit.err_chan = rabbit.conn.NotifyClose(rabbit.err_chan)

	go func(){
		for e := range rabbit.err_chan {
			log.Println(e)
			rabbit.ch.Close()
			rabbit.conn.Close()
			rabbit.conn = nil
			rabbit.ch = nil
		}
	}()


	return rabbit, nil
}


func (r *Rabbit) Setup() error {

	var err error

	if r.conn == nil {
		url := fmt.Sprintf("amqp://%s:%s@%s:%s%s", r.username, r.password, r.host, r.port, r.vhost)
		r.conn, err = amqp.Dial(url)
		if err != nil {
			return  err
		}
	}

	if r.ch == nil {
		r.ch, err = r.conn.Channel()
		if err != nil {
			r.conn.Close()
			return err
		}
	}

	return nil
}

func NewRabbitA(rabbit *Rabbit, queue_name string) *RabbitA{

	return &RabbitA{rabbit    : rabbit,
                    queue_name: queue_name }
}

func NewRabbitB(rabbit *Rabbit, exchange_name string) *RabbitB{

	return &RabbitB{rabbit       : rabbit,
                    exchange_name: exchange_name }
}


func (rb *RabbitB) RePublish(d amqp.Delivery) error {

	err := rb.rabbit.Setup()
	if err != nil {
		return err
	}

	err = rb.rabbit.ch.Publish(
		rb.exchange_name,// exchange
		d.RoutingKey, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing {
          Headers    : d.Headers,
		  ContentType: d.ContentType,
		  Body       : d.Body,
		})

	return err
}

func (ra *RabbitA) Consume(rabbitB *RabbitB)  error {

	err := ra.rabbit.Setup()
	if err != nil {
		return err
	}

	msgs, err := ra.rabbit.ch.Consume(
	  ra.queue_name, // queue
	  "govel",     // consumer
	  true,   // auto-ack
	  false,  // exclusive
	  false,  // no-local
	  false,  // no-wait
	  nil,    // args
	)
	if err != nil {
		return err
	}

	ra.stop_flag = make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received message: %s with RK %s", d.Body, d.RoutingKey)
			log.Println("Sending message to side B")
			err := rabbitB.RePublish(d)
			if err != nil {
				log.Println(err, "Failed to register a consumer")
				d.Nack(false, //multiple
                       true)  //requeue
			}
		}
		log.Println("Exiting consume loop")
		for {
			log.Println("Trying to reconnect")
			err := ra.Consume(rabbitB)
			if err == nil {
				log.Println("Reconnected")
				break
			}
			log.Println(err)
			log.Println("Will try again in 5 secs.")
			time.Sleep(5 * time.Second)
		}
	}()

	return nil

}

func main() {

	log.Println("Connecting to rabbit_from")
	rabbit_from, err := NewRabbit("guest", "guest", "10.0.3.10", "5672", "/")
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Connecting to rabbit_to")
	rabbit_to, err := NewRabbit("guest", "guest", "10.0.3.11", "5672", "/")
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Creating side A")
	rabbit_a := NewRabbitA(rabbit_from, "Q_test")

	log.Println("Creating side B")
	rabbit_b := NewRabbitB(rabbit_to, "E_test")


	log.Println("Starting consume process")
	err = rabbit_a.Consume( rabbit_b )
	if err != nil {
		log.Fatalln(err)
	}

	<-rabbit_a.stop_flag
	//rabbit_a.stop_flag <- true

}


