package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
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

	err = ch.ExchangeDeclarePassive(
		"logs_topic",
		amqp.ExchangeTopic,
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	failOnError(err, "failed declare a queue")

	if len(os.Args) < 2 {
		log.Fatal("please type in [warn] [info] [error] ...")
		return
	}

	for _, severity := range os.Args[1:] {
		err = ch.QueueBind(
			q.Name,
			severity,
			"logs_topic",
			false,
			nil,
		)
		failOnError(err, "failed bind a queue")
		fmt.Printf("bind %s success\n", severity)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to register a consumer")

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s\n", d.Body)
			failOnError(err, "failed to ack message")
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-make(chan struct{}, 0)
}

//func deal(msg []byte) {
//	time.Sleep(5 * time.Second)
//	log.Printf("done with message: %s !!!\n", msg)
//}
