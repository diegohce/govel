package main

import (
	"os"
	"log"
	"time"
	"flag"

	"github.com/streadway/amqp"
)


type Rabbit struct {
	url      string
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



func NewRabbit(url string) (*Rabbit, error) {

	rabbit := &Rabbit{ url: url }

/*	err := rabbit.Setup()
	if err != nil {
		return nil, err
	}*/

	return rabbit, nil
}


func (r *Rabbit) Setup() error {

	var err error

	if r.conn == nil {
		log.Println("Connecting to", r.url)
		r.conn, err = amqp.Dial(r.url)
		if err != nil {
			return  err
		}
	}

	if r.ch == nil {
		r.ch, err = r.conn.Channel()
		if err != nil {
			r.conn.Close()
			r.conn = nil
			return err
		}
	}

	r.err_chan = make(chan *amqp.Error)
	r.err_chan = r.conn.NotifyClose(r.err_chan)

	go func(){
		for e := range r.err_chan {
			log.Println(e)
			if r.ch != nil {
				r.ch.Close()
			}
			if r.conn != nil {
				r.conn.Close()
			}
			r.conn = nil
			r.ch = nil
		}
	}()

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
	  false, //true,   // auto-ack
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
				log.Println(err, "Publishing failed" )
				//d.Nack(false, //multiple
                //       true)  //requeue
			} else {
				d.Ack(false) //multiple
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

	var from_url string
	var from_queue string
	var to_url string
	var to_exchange string

	fs := flag.NewFlagSet("govel", flag.ExitOnError)

	fs.StringVar(&from_url, "from-url", "", "Source rabbit connection url (amqp://guest:guest@rabbitmq-server-A:5672/)")
	fs.StringVar(&from_queue, "from-queue", "", "Queue to read messages from")
	fs.StringVar(&to_url, "to-url", "", "Target rabbit connection url (amqp://guest:guest@rabbitmq-server-B:5672/)")
	fs.StringVar(&to_exchange, "to-exchange", "", "Exchange to write messages to")


	if len(os.Args[1:]) == 0 {
		 os.Args = append(os.Args, "--help")
	}

	err := fs.Parse(os.Args[1:])
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Connecting to rabbit_from")
	rabbit_from, err := NewRabbit(from_url)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Connecting to rabbit_to")
	rabbit_to, err := NewRabbit(to_url)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Creating side A")
	rabbit_a := NewRabbitA(rabbit_from, from_queue)

	log.Println("Creating side B")
	rabbit_b := NewRabbitB(rabbit_to, to_exchange)


	log.Println("Starting consume process")
	err = rabbit_a.Consume( rabbit_b )
	if err != nil {
		log.Fatalln(err)
	}

	<-rabbit_a.stop_flag
	//rabbit_a.stop_flag <- true

}


