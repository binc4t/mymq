package main

import (
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@117.50.190.205:5672/")
	failOnError(err, "failed to connect to RammitMQ")
	ch, err := conn.Channel()
	failOnError(err, "failed to create a channel")
	q, err := ch.QueueDeclare(
		"task_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed declare a queue")

	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to register a consumer")
	err = ch.Qos(1, 0, false)
	failOnError(err, "failed to set qos")

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s\n", d.Body)
			deal(d.Body)
			err := d.Ack(false)
			failOnError(err, "failed to ack message")
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-make(chan struct{}, 0)
}

func deal(msg []byte) {
	time.Sleep(5 * time.Second)
	log.Printf("done with message: %s !!!\n", msg)
}
