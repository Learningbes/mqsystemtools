package mqsystemtools

import (
	"log"
	"math"
	"time"

	"github.com/streadway/amqp"
)

func RabbitMQConnect(rabbitmqUrl string) (*amqp.Connection, error) {
	var counts int64
	var backOff = 1 * time.Second
	var connection *amqp.Connection

	for {
		c, err := amqp.Dial(rabbitmqUrl)
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
		log.Println("waiting...")
		time.Sleep(backOff)
		continue
	}

	return connection, nil
}

func RabbitMQQueueDeclare(ch *amqp.Channel, queue string) (*amqp.Queue, error) {
	q, err := ch.QueueDeclare(queue, false, false, false, false, nil)

	log.Println(q) // log queue status
	if err != nil {
		return nil, err
	}

	return &q, nil
}
