package main

import (
	"context"
	"log"
	"math"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SIMPLE FANOUT CONSUMER
type RConsumer struct {
	ch *amqp.Channel
	q  amqp.Queue
}

func (r *RConsumer) Listen() error {
	msgs, err := r.ch.Consume(
		r.q.Name, // queue
		"",       // consumer
		true,     // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
	if err != nil {
		return err
	}

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf(" [x] %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever

	return nil
}

func CreateRConsumer(conn *amqp.Connection, exhangeName, queueName string) (*RConsumer, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exhangeName, // name
		"fanout",    // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		true,      // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return nil, err
	}

	err = ch.QueueBind(
		q.Name,      // queue name
		"",          // routing key
		exhangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &RConsumer{
		ch: ch,
		q:  q,
	}, nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

// SIMPLE FANOUT SENDER
type RSender struct {
	ch     *amqp.Channel
	exName string
}

func (rs *RSender) Push(message string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := message
	err := rs.ch.PublishWithContext(ctx,
		rs.exName, // exchange
		"",        // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
	return nil
}

func CreateRSender(conn *amqp.Connection, exchangeName string) *RSender {

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")
	return &RSender{
		ch:     ch,
		exName: exchangeName,
	}
}

// UTILS
func ConnectToRabbitMQ(connString string) (*amqp.Connection, error) {
	var counts int64
	var backOff = 1 * time.Second
	var connection *amqp.Connection

	for {
		c, err := amqp.Dial(connString) // "amqp://guest:guest@localhost:5672/"
		if err != nil {
			log.Println("RabbitMQ not yet ready...")
			counts++
		} else {
			log.Println("Connected to RabbitMQ")
			connection = c
			break
		}

		if counts > 5 {
			log.Println(err)
			return nil, err
		}

		backOff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
		log.Println("Waiting...")
		time.Sleep(backOff)
		continue
	}

	return connection, nil
}
